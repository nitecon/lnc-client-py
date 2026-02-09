#!/usr/bin/env bash
# lnc-client-py Integrity Test Runner
#
# Runs integration tests against a live Lance server.
#
# Usage:
#   ./run-integrity-tests.sh --target host:port
#   ./run-integrity-tests.sh --target host:port --benchmark
#   ./run-integrity-tests.sh --target host:port --filter "TestProducer"
#   ./run-integrity-tests.sh --target host:port -v

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Options
TARGET_ENDPOINT=""
TEST_FILTER=""
VERBOSE=""
RUN_BENCHMARK=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --target)
            TARGET_ENDPOINT="$2"
            shift 2
            ;;
        --target=*)
            TARGET_ENDPOINT="${1#*=}"
            shift
            ;;
        --filter|-k)
            TEST_FILTER="$2"
            shift 2
            ;;
        --benchmark)
            RUN_BENCHMARK=true
            shift
            ;;
        --verbose|-v)
            VERBOSE="-v"
            shift
            ;;
        --help|-h)
            echo "Usage: $0 --target HOST:PORT [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --target HOST:PORT   Lance server endpoint (required)"
            echo "  --filter PATTERN     Run only tests matching pattern (pytest -k)"
            echo "  --benchmark          Enable throughput benchmark test"
            echo "  --verbose, -v        Verbose pytest output"
            echo "  --help, -h           Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

if [ -z "$TARGET_ENDPOINT" ]; then
    echo "ERROR: --target is required"
    echo "Usage: $0 --target HOST:PORT"
    exit 1
fi

# Parse host:port
HOST="${TARGET_ENDPOINT%:*}"
PORT="${TARGET_ENDPOINT#*:}"

echo ""
echo "========================================"
echo "  lnc-client-py Integrity Tests"
echo "========================================"
echo ""
echo "  Target: $TARGET_ENDPOINT"
echo ""

# Verify connectivity
echo "=== Connectivity Check ==="
if nc -z -w 3 "$HOST" "$PORT" 2>/dev/null; then
    echo "  [OK] $TARGET_ENDPOINT is reachable"
else
    echo "  [FAIL] Cannot reach $TARGET_ENDPOINT"
    exit 1
fi
echo ""

# Activate venv if it exists
if [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
    source "$SCRIPT_DIR/.venv/bin/activate"
fi

# Set environment
export LANCE_TEST_ADDR="$TARGET_ENDPOINT"
if [ "$RUN_BENCHMARK" = true ]; then
    export LANCE_RUN_BENCHMARK=1
fi

# Build pytest args
PYTEST_ARGS=(
    "tests/test_integrity.py"
    "-s"          # Show print output
    "--timeout=60"
)

if [ -n "$VERBOSE" ]; then
    PYTEST_ARGS+=("$VERBOSE")
fi

if [ -n "$TEST_FILTER" ]; then
    PYTEST_ARGS+=("-k" "$TEST_FILTER")
fi

echo "=== Running Tests ==="
echo "  Command: pytest ${PYTEST_ARGS[*]}"
echo ""

cd "$SCRIPT_DIR"
pytest "${PYTEST_ARGS[@]}"
EXIT_CODE=$?

echo ""
echo "========================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "  [OK] All integrity tests passed!"
else
    echo "  [FAIL] Some tests failed (exit code: $EXIT_CODE)"
fi
echo "========================================"

exit $EXIT_CODE
