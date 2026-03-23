$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
$PythonExe = if (Test-Path $VenvPython) { $VenvPython } else { (Get-Command python).Source }

Set-Location $RepoRoot
& $PythonExe (Join-Path $RepoRoot "server.py") --transport streamable-http
