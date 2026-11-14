# Usage: scripts\bench.ps1 [matrix.toml] [out-name]
# Thin wrapper: builds release binaries, then lets matier-bench spawn/stop the server.
param(
    [string]$Matrix = "bench/matrix.toml",
    [string]$Name = (Get-Date -Format "yyyyMMdd-HHmmss")
)
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
cargo build --release -p matier-bench -p matier-bench-server
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
New-Item -ItemType Directory -Force bench-results | Out-Null
& .\target\release\matier-bench.exe matrix --spawn-server --config $Matrix --out "bench-results/$Name.json"
exit $LASTEXITCODE
