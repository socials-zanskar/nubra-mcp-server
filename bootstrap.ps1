$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$VenvPath = Join-Path $RepoRoot ".venv"
$PythonExe = Join-Path $VenvPath "Scripts\python.exe"

if (-not (Test-Path $VenvPath)) {
    python -m venv $VenvPath
}

& $PythonExe -m pip install --upgrade pip
& $PythonExe -m pip install -r (Join-Path $RepoRoot "requirements.txt")

if (-not (Test-Path (Join-Path $RepoRoot ".env"))) {
    Copy-Item (Join-Path $RepoRoot ".env.example") (Join-Path $RepoRoot ".env")
    Write-Host "Created .env from .env.example. Fill in your Nubra phone number and MPIN before authenticating."
}

Write-Host "Bootstrap complete."
