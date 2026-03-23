$ErrorActionPreference = "Stop"

$baseUrl = if ($env:NUBRA_MCP_BASE_URL) { $env:NUBRA_MCP_BASE_URL } else { "http://127.0.0.1:8000" }

Write-Host "Checking $baseUrl/health"
$health = Invoke-RestMethod -Method Get -Uri "$baseUrl/health"
$health | ConvertTo-Json -Depth 6

Write-Host ""
Write-Host "Checking $baseUrl/"
$root = Invoke-RestMethod -Method Get -Uri "$baseUrl/"
$root | ConvertTo-Json -Depth 6
