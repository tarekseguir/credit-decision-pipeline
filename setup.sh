#!/usr/bin/env bash
# One-shot venv setup for macOS / Linux.
# Usage:  ./setup.sh

set -euo pipefail

PYTHON=${PYTHON:-python3}

if ! command -v "$PYTHON" > /dev/null; then
    echo "❌ Python not found. Install Python 3.11+ first."
    exit 1
fi

PY_VERSION=$("$PYTHON" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
echo "Using $PYTHON ($PY_VERSION)"

if [ ! -d ".venv" ]; then
    echo "→ Creating .venv"
    "$PYTHON" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

echo "→ Upgrading pip"
pip install --upgrade pip --quiet

echo "→ Installing dependencies"
pip install -r requirements.txt --quiet

echo ""
echo "✅ Setup complete."
echo ""
echo "Activate the environment with:"
echo "    source .venv/bin/activate"
echo ""
echo "Then run:"
echo "    make generate && make run && make dashboard"
