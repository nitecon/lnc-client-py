[← Back to Project Root](../README.md)

# lnc-client-py — Engineering Standards

## Table of Contents

- [1. The Golden Rules](#1-the-golden-rules)
- [2. Project Structure](#2-project-structure)
- [3. Async Patterns](#3-async-patterns)
- [4. Protocol Correctness](#4-protocol-correctness)
- [5. Error Handling](#5-error-handling)
- [6. Connection Management](#6-connection-management)
- [7. Performance](#7-performance)
- [8. Code Style & Idioms](#8-code-style--idioms)
- [9. Testing](#9-testing)
- [10. CI/CD & Review Gates](#10-cicd--review-gates)
- [Quick Reference Card](#quick-reference-card)

**Revision**: 2026.02 | **Status**: Mandatory | **Scope**: All Contributors

> *"This client is the async bridge between Python workloads and the Lance streaming platform. Protocol fidelity and connection resilience are non-negotiable."*

---

## 1. The Golden Rules

These rules are **non-negotiable**. Violations block merge.

| Rule | Rationale | Detection |
|------|-----------|-----------|
| **No blocking calls in async code** | Blocks the event loop, starves all consumers/producers | `ruff` + manual review |
| **No bare `except:`** | Swallows cancellation, hides bugs | `ruff` B001/B002 |
| **No credentials in source** | Security — all config via parameters or environment | `grep -rE "password\|secret\|token" src/` |
| **All public APIs must have type hints** | Client library contract — callers depend on type safety | `mypy` / manual review |
| **Protocol bytes must match LWP spec** | A single wrong byte corrupts the stream for all consumers | Integration tests against Lance |
| **No `time.sleep()` in async code** | Use `asyncio.sleep()` — blocking sleep freezes the loop | `ruff` + `grep` |

---

## 2. Project Structure

```
lnc-client-py/
├── src/lnc_client/
│   ├── __init__.py          # Public API exports
│   ├── client.py            # LanceClient — management API (create/delete topics)
│   ├── producer.py          # Producer — publish messages to topics
│   ├── consumer.py          # StandaloneConsumer — subscribe and consume messages
│   ├── connection.py        # Low-level TCP connection management
│   ├── protocol.py          # LWP frame encoding/decoding
│   ├── tlv.py               # TLV (Type-Length-Value) serialization
│   ├── config.py            # Configuration dataclasses
│   └── errors.py            # Exception hierarchy
├── tests/                   # pytest + pytest-asyncio tests
├── pyproject.toml           # Build config, dependencies, ruff/pytest settings
└── docs/
    └── CodingGuidelines.md  # This file
```

### 2.1 Module Dependency Rules

| Module | May Import From | Must NOT Import From |
|--------|----------------|---------------------|
| `protocol.py` | `tlv.py`, `errors.py`, stdlib | `client.py`, `producer.py`, `consumer.py` |
| `connection.py` | `protocol.py`, `config.py`, `errors.py` | `client.py`, `producer.py`, `consumer.py` |
| `producer.py` | `connection.py`, `protocol.py`, `config.py`, `errors.py` | `consumer.py`, `client.py` |
| `consumer.py` | `connection.py`, `protocol.py`, `config.py`, `errors.py` | `producer.py`, `client.py` |
| `client.py` | All internal modules | — |

**Principle**: `protocol.py` and `tlv.py` are pure encoding/decoding with no I/O. `connection.py` handles the socket. Higher-level classes compose these layers.

---

## 3. Async Patterns

### 3.1 All I/O Is Async

Every function that performs network I/O must be `async`:

```python
# ✅ CORRECT: Async I/O
async def send_frame(self, frame: bytes) -> None:
    self._writer.write(frame)
    await self._writer.drain()

# ❌ FORBIDDEN: Blocking I/O in async context
def send_frame(self, frame: bytes) -> None:
    self._socket.sendall(frame)  # BLOCKS EVENT LOOP
```

### 3.2 Cancellation Safety

All async operations must be cancellation-safe:

```python
# ✅ CORRECT: Cleanup on cancellation
async def consume(self) -> AsyncIterator[Message]:
    try:
        while True:
            msg = await self._read_message()
            yield msg
    except asyncio.CancelledError:
        await self._send_disconnect()
        raise  # ALWAYS re-raise CancelledError

# ❌ FORBIDDEN: Swallowing cancellation
except Exception:  # Catches CancelledError!
    pass
```

### 3.3 Timeouts

All network operations must have timeouts:

```python
# ✅ CORRECT: Bounded wait
response = await asyncio.wait_for(self._read_response(), timeout=30.0)

# ❌ WRONG: Unbounded wait (hangs forever on network issues)
response = await self._read_response()
```

---

## 4. Protocol Correctness

### 4.1 LWP Frame Encoding

The Lance Wire Protocol (LWP) uses a fixed-size header. All frame encoding/decoding must:

- Use `struct.pack` / `struct.unpack` with explicit byte order (`>` big-endian or `<` little-endian as per spec)
- Validate magic bytes on every received frame
- Compute and verify CRC32C checksums
- Never assume payload alignment

```python
# ✅ CORRECT: Explicit format, validated
header = struct.pack(HEADER_FORMAT, MAGIC, version, flags, reserved, crc)
assert len(header) == HEADER_SIZE

# ❌ WRONG: String concatenation for binary protocol
header = magic + version.to_bytes(1, "big") + ...  # FRAGILE
```

### 4.2 TLV Encoding

TLV fields must be encoded/decoded via `tlv.py` helpers. Never hand-roll TLV serialization inline.

### 4.3 Byte Buffers

- Use `bytes` for immutable payloads, `bytearray` for mutable construction
- Use `memoryview` for zero-copy slicing when parsing frames
- Avoid unnecessary copies: slice the buffer, don't extract to new `bytes` objects

---

## 5. Error Handling

### 5.1 Exception Hierarchy

All client exceptions must inherit from a base `LanceClientError`:

```python
class LanceClientError(Exception): ...
class ConnectionError(LanceClientError): ...
class ProtocolError(LanceClientError): ...
class TimeoutError(LanceClientError): ...
class TopicNotFoundError(LanceClientError): ...
```

### 5.2 Error Patterns

```python
# ✅ CORRECT: Typed exception with context
raise ProtocolError(f"Invalid magic bytes: expected {MAGIC!r}, got {actual!r}")

# ✅ CORRECT: Wrapping low-level errors
except OSError as e:
    raise ConnectionError(f"Failed to connect to {host}:{port}") from e

# ❌ FORBIDDEN: Bare raise with no context
raise Exception("error")

# ❌ FORBIDDEN: Silent swallow
except Exception:
    pass
```

---

## 6. Connection Management

### 6.1 Reconnection

- Implement exponential backoff with jitter for reconnection attempts
- Log every reconnection attempt at `WARNING` level
- Emit metrics (if available) for connection state changes
- Set a maximum retry count or timeout, then raise `ConnectionError`

### 6.2 Graceful Shutdown

- Send a disconnect frame before closing the socket
- Cancel all pending reads/writes
- Close the `asyncio.StreamWriter` properly (`writer.close(); await writer.wait_closed()`)

### 6.3 Resource Cleanup

All connection-holding classes must support async context managers:

```python
# ✅ CORRECT: Context manager support
async with Producer(host, port, topic) as producer:
    await producer.send(data)
# Connection is guaranteed closed here
```

---

## 7. Performance

### 7.1 Batching

When sending multiple messages, batch them into a single write where the protocol allows:

```python
# ✅ PREFERRED: Batch write
frames = [encode_frame(msg) for msg in messages]
self._writer.writelines(frames)
await self._writer.drain()

# ❌ AVOID: One syscall per message
for msg in messages:
    self._writer.write(encode_frame(msg))
    await self._writer.drain()  # Flush per message = slow
```

### 7.2 Buffer Management

- Pre-allocate read buffers where possible
- Use `memoryview` for zero-copy slicing during frame parsing
- Avoid creating intermediate `bytes` objects in hot loops

### 7.3 Compression

- LZ4 compression is supported via the `lz4` dependency
- Compression should be configurable, not mandatory
- Never compress already-compressed payloads

---

## 8. Code Style & Idioms

### 8.1 Tooling

| Tool | Config | Purpose |
|------|--------|---------|
| **Ruff** | `pyproject.toml` `[tool.ruff]` | Linting + formatting |
| **pytest** | `pyproject.toml` `[tool.pytest]` | Testing |

### 8.2 Ruff Configuration

```toml
[tool.ruff]
target-version = "py310"
line-length = 100

[tool.ruff.lint]
select = ["E", "F", "W", "I", "UP", "B", "SIM"]
ignore = ["E501"]

[tool.ruff.format]
quote-style = "double"
```

### 8.3 Naming Conventions

| Item | Convention | Example |
|------|-----------|---------|
| Modules | `snake_case` | `protocol.py`, `connection.py` |
| Classes | `PascalCase` | `Producer`, `StandaloneConsumer`, `LanceClient` |
| Functions | `snake_case` | `send_frame`, `read_message` |
| Constants | `SCREAMING_SNAKE` | `HEADER_SIZE`, `MAGIC` |
| Private members | `_leading_underscore` | `self._writer`, `self._buffer` |

### 8.4 Docstrings

All public classes and functions must have Google-style docstrings:

```python
async def send(self, payload: bytes, *, compress: bool = False) -> None:
    """Send a message to the connected topic.

    Args:
        payload: Raw message bytes to publish.
        compress: If True, apply LZ4 compression before sending.

    Raises:
        ConnectionError: If not connected to a Lance server.
        ProtocolError: If the frame encoding fails.
    """
```

### 8.5 Type Hints

All public API signatures must include type hints:

```python
# ✅ CORRECT
async def connect(self, host: str, port: int, *, timeout: float = 30.0) -> None: ...

# ❌ WRONG: No type hints on public API
async def connect(self, host, port, timeout=30.0): ...
```

---

## 9. Testing

### 9.1 Test Strategy

| Layer | Test Type | Notes |
|-------|----------|-------|
| `protocol.py` / `tlv.py` | Unit tests | Round-trip encode/decode, edge cases, malformed input |
| `connection.py` | Unit tests with mock socket | Connection lifecycle, reconnection, timeout |
| `producer.py` / `consumer.py` | Integration tests | Require running Lance (mark `@pytest.mark.integration`) |
| `client.py` | Integration tests | Topic create/delete/list against Lance |

### 9.2 Test Rules

- Tests use `pytest-asyncio` with `asyncio_mode = "auto"`
- Unit tests must not require network access
- Integration tests must be marked and skippable: `@pytest.mark.integration`
- All protocol encoding must have a corresponding decode test (round-trip)
- Edge cases: empty payloads, maximum-size frames, malformed headers

### 9.3 Validation Before Commit

```bash
ruff check .
ruff format --check .
pytest
```

---

## 10. CI/CD & Review Gates

### 10.1 Pre-Merge Checks

| Check | Command | Purpose |
|-------|---------|---------|
| **Lint** | `ruff check .` | Catch bugs and enforce style |
| **Format** | `ruff format --check .` | Consistent formatting |
| **Test** | `pytest` | All unit tests pass |

### 10.2 Human Review Checklist

Before approving any PR:

- [ ] **All public APIs have type hints and docstrings**
- [ ] **Async functions don't call blocking I/O**
- [ ] **CancelledError is re-raised, not swallowed**
- [ ] **Network operations have timeouts**
- [ ] **Protocol changes match the LWP spec**
- [ ] **New exceptions inherit from `LanceClientError`**
- [ ] **Connection cleanup uses async context managers**
- [ ] **Tests cover round-trip encoding for any protocol changes**

---

## Quick Reference Card

### Forbidden Patterns

```python
# ❌ time.sleep() in async code
# ❌ bare except: or except Exception: (swallows CancelledError)
# ❌ socket.recv() / socket.send() (blocking I/O)
# ❌ await without timeout on network ops
# ❌ manual TLV encoding outside tlv.py
# ❌ credentials in source code
# ❌ public API without type hints
```

### Required Patterns

```python
# ✅ asyncio.sleep() for delays
# ✅ except asyncio.CancelledError: ... raise
# ✅ asyncio.StreamReader/StreamWriter for I/O
# ✅ asyncio.wait_for(coro, timeout=N)
# ✅ async with for connection lifecycle
# ✅ struct.pack/unpack for binary protocol
# ✅ Google-style docstrings on public API
```

---

[↑ Back to Top](#lnc-client-py--engineering-standards) | [← Back to Project Root](../README.md)
