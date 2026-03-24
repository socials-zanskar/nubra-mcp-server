$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$VenvPath = Join-Path $RepoRoot ".venv"
$PythonExe = Join-Path $VenvPath "Scripts\python.exe"
$RequirementsPath = Join-Path $RepoRoot "requirements.txt"

function Test-HealthyVenv {
    param(
        [string]$PythonPath
    )

    if (-not (Test-Path $PythonPath)) {
        return $false
    }

    & $PythonPath -c "import pip" *> $null
    return $LASTEXITCODE -eq 0
}

if ((-not (Test-Path $VenvPath)) -or (-not (Test-HealthyVenv -PythonPath $PythonExe))) {
    if (Test-Path $VenvPath) {
        Remove-Item -Recurse -Force $VenvPath
    }
    python -m venv $VenvPath
}

& $PythonExe -m pip install --upgrade pip
& $PythonExe -m pip install -r $RequirementsPath

if (-not (Test-Path (Join-Path $RepoRoot ".env"))) {
    Copy-Item (Join-Path $RepoRoot ".env.example") (Join-Path $RepoRoot ".env")
    Write-Host "Created .env from .env.example. Fill in your Nubra phone number and MPIN before authenticating."
}

Write-Host "Bootstrap complete."
