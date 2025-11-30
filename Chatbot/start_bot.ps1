<#
.SYNOPSIS
    Simple launcher for the TennisBot components.

    Opens three new PowerShell windows and runs these exact commands (as requested):
      1) Activate the project's venv and run:   rasa run actions --port 5055
      2) Activate the project's venv and run:   rasa run --enable-api --cors "*" --port 5005
      3) Start ngrok:                           ngrok http 5005

    Usage: .\start_bot.ps1

    Notes:
      - Expects your venv to live at .\venv
      - Requires `rasa` to be available inside the venv (installed)
      - If `ngrok` is not in PATH you must run it manually
#>

$root = Split-Path -Parent $MyInvocation.MyCommand.Definition
$venvActivate = Join-Path $root "venv\Scripts\Activate.ps1"

# No token handling here: token is expected to be stored in credentials.yml (literal)
$envPrefix = ""

function Start-NewShell {
    param(
        [string]$Title,
        [string]$Command
    )
    Write-Host "Starting $Title..."
    # Create a PowerShell process that sets location, activates venv and runs the command
    # prefix with TELEGRAM_TOKEN export when available
    $psCommand = "$envPrefix Set-Location -Path '$root'; . '$venvActivate'; $Command"
    Start-Process -FilePath "powershell" -ArgumentList "-NoExit", "-Command", $psCommand
}

if (-not (Test-Path $venvActivate)) {
    Write-Error "Virtualenv activate script not found at $venvActivate. Create the venv or update the script.";
    exit 1
}

# 1) Actions
$actionsCmd = "rasa run actions --port 5055"
Start-NewShell "Actions (5055)" $actionsCmd
Start-Sleep -Seconds 3

# 2) Rasa
$rasaCmd = 'rasa run --enable-api --cors "*" --port 5005'
Start-NewShell "Rasa (5005)" $rasaCmd
Start-Sleep -Seconds 2

# 3) ngrok
try {
    Get-Command ngrok -ErrorAction Stop | Out-Null
    Start-NewShell "ngrok (http 5005)" "ngrok http 5005"
} catch {
    Write-Warning "ngrok not found in PATH. Start it manually: ngrok http 5005"
}

Write-Host "Launcher executed. Check the new PowerShell windows for logs." 
