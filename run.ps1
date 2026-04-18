# One-command runner for Windows PowerShell.
#
# Runs the pipeline against the committed 50-customer sample in data/sample/.
# To generate fresh data at a different size:
#   python -m src.data_generation.generate --customers N --output data/sample
#
# Usage:
#   .\run.ps1              → run the pipeline
#   .\run.ps1 dashboard    → run + launch the Streamlit dashboard
#   .\run.ps1 test         → run the test suite
#   .\run.ps1 all          → run + test + dashboard

param(
    [string]$Command = "default"
)

$ErrorActionPreference = "Stop"

if (Test-Path ".venv\Scripts\Activate.ps1") {
    & ".venv\Scripts\Activate.ps1"
} else {
    Write-Host "⚠️  .venv not found. Run .\setup.ps1 first, or install deps manually:" -ForegroundColor Yellow
    Write-Host "    pip install -r requirements.txt"
    Write-Host ""
}

function Invoke-Pipeline {
    Write-Host "→ Running the pipeline DAG against data/sample/" -ForegroundColor Cyan
    python run.py
    Write-Host ""
}

function Invoke-Tests {
    Write-Host "→ Running tests" -ForegroundColor Cyan
    pytest -v
    Write-Host ""
}

function Invoke-Dashboard {
    Write-Host "→ Launching Streamlit dashboard at http://localhost:8501" -ForegroundColor Cyan
    streamlit run dashboard\app.py
}

switch ($Command) {
    "default" {
        Invoke-Pipeline
        Write-Host "✅ Done. Run '.\run.ps1 dashboard' to open the UI." -ForegroundColor Green
    }
    "dashboard" {
        Invoke-Pipeline
        Invoke-Dashboard
    }
    "test" {
        Invoke-Tests
    }
    "all" {
        Invoke-Pipeline
        Invoke-Tests
        Invoke-Dashboard
    }
    default {
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Write-Host "Usage: .\run.ps1 [dashboard|test|all]"
        exit 1
    }
}
