#!/bin/bash
# Setup virtual environment for lnc-client-py
set -e

cd "$(dirname "$0")"

# Install python3-venv if not available
if ! python3 -m venv --help &>/dev/null; then
    echo "Installing python3-venv..."
    sudo apt install -y python3-venv python3-pip
fi

# Create venv
echo "Creating virtual environment..."
python3 -m venv .venv

# Activate and install
echo "Installing dependencies..."
source .venv/bin/activate
pip install -e ".[dev]"

echo ""
echo "=== Setup complete ==="
echo "Activate with: source .venv/bin/activate"
echo "Run tests with: pytest -v"
