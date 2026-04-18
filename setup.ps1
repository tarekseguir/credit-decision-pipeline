# One-shot venv setup for Windows PowerShell.
# Usage:  .\setup.ps1

$ErrorActionPreference = "Stop"

$python = "python"
if (-not (Get-Command $python -ErrorAction SilentlyContinue)) {
    Write-Host "❌ Python not found. Install Python 3.11+ first." -ForegroundColor Red
    exit 1
}

$pyVersion = & $python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
Write-Host "Using python ($pyVersion)"

if (-not (Test-Path ".venv")) {
    Write-Host "→ Creating .venv"
    & $python -m venv .venv
}

& ".venv\Scripts\Activate.ps1"

Write-Host "→ Upgrading pip"
pip install --upgrade pip --quiet

Write-Host "→ Installing dependencies"
pip install -r requirements.txt --quiet

Write-Host ""
Write-Host "✅ Setup complete." -ForegroundColor Green
Write-Host ""
Write-Host "Activate the environment with:"
Write-Host "    .venv\Scripts\Activate.ps1"
Write-Host ""
Write-Host "Then run:"
Write-Host "    python -m src.data_generation.generate"
Write-Host "    python run.py"
Write-Host "    streamlit run dashboard\app.py"
