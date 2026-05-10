# Create a new GitHub repo, add origin, and push this project (browser login).
#
# Usage:
#   cd path\to\ppp-enrichment
#   pwsh -ExecutionPolicy Bypass -File scripts/publish_to_github.ps1
#
# Options:
#   -RepoName my-repo   (default: ppp-enrichment)
#   -Public              (default: private repo)

param(
    [string] $RepoName = "ppp-enrichment",
    [switch] $Public
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$GhExe = Join-Path ${env:ProgramFiles} "GitHub CLI/gh.exe"
if (-not (Test-Path $GhExe)) {
    throw "GitHub CLI not found at $GhExe. Install: winget install GitHub.cli"
}

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RepoRoot

Write-Host "Repo root: $RepoRoot" -ForegroundColor DarkGray

& $GhExe auth status 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Host "`nNot logged into GitHub. Opening browser to authenticate...`n" -ForegroundColor Cyan
    & $GhExe auth login -h github.com -p https -w
}

git remote get-url origin 2>$null | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "Remote 'origin' exists. Pushing branch main..." -ForegroundColor Yellow
    git push -u origin main
    exit 0
}

$desc = "PPP enrichment pipeline + chunk inputs"
Write-Host "Creating '$RepoName' on GitHub and pushing..." -ForegroundColor Cyan
if ($Public) {
    & $GhExe repo create $RepoName --public --source=. --remote=origin --push --description $desc --confirm
}
else {
    & $GhExe repo create $RepoName --private --source=. --remote=origin --push --description $desc --confirm
}

$url = (& $GhExe repo view --json url -q .url).Trim()
Write-Host "`nPublished: $url`nEnable Actions Read/Write under Settings -> Actions -> General.`n" -ForegroundColor Green
