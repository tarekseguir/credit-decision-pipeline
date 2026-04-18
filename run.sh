#!/usr/bin/env bash
# One-command runner for macOS / Linux.
#
# Runs the pipeline against the committed 50-customer sample in data/sample/.
# To generate fresh data at a different size:
#   python -m src.data_generation.generate --customers N --output data/sample
#
# Usage:
#   ./run.sh              → run the pipeline
#   ./run.sh dashboard    → run + launch the Streamlit dashboard
#   ./run.sh test         → run the test suite
#   ./run.sh all          → run + test + dashboard

set -euo pipefail

# --- Activate venv if present ------------------------------------------------
if [ -d ".venv" ]; then
    # shellcheck disable=SC1091
    source .venv/bin/activate
else
    echo "⚠️  .venv not found. Run ./setup.sh first, or install deps manually:"
    echo "    pip install -r requirements.txt"
    echo ""
fi

COMMAND="${1:-default}"

run_pipeline() {
    echo "→ Running the pipeline DAG against data/sample/"
    python run.py
    echo ""
}

run_tests() {
    echo "→ Running tests"
    pytest -v
    echo ""
}

run_dashboard() {
    echo "→ Launching Streamlit dashboard at http://localhost:8501"
    streamlit run dashboard/app.py
}

case "$COMMAND" in
    default)
        run_pipeline
        echo "✅ Done. Run './run.sh dashboard' to open the UI."
        ;;
    dashboard)
        run_pipeline
        run_dashboard
        ;;
    test)
        run_tests
        ;;
    all)
        run_pipeline
        run_tests
        run_dashboard
        ;;
    *)
        echo "Unknown command: $COMMAND"
        echo "Usage: ./run.sh [dashboard|test|all]"
        exit 1
        ;;
esac
