# test_adapter_operations.Tests.ps1

param(
    [string]$PythonExe = "python3"
)

# Resolve current test file directory robustly
$ThisFilePath = $PSCommandPath
if ([string]::IsNullOrWhiteSpace($ThisFilePath)) {
    $ThisFilePath = $MyInvocation.MyCommand.Path
}
if ([string]::IsNullOrWhiteSpace($ThisFilePath)) {
    throw "Cannot resolve current test file path. PSCommandPath='$PSCommandPath'"
}

$ThisDir     = Split-Path -Parent $ThisFilePath
# Adapter script is one level up from tests: .../adapters/python/adapter.py
$AdapterDir  = Split-Path -Parent $ThisDir
$AdapterPath = if ($AdapterDir) { Join-Path -Path $AdapterDir -ChildPath "adapter.py" } else { "" }

# Helper to run adapter
function global:Invoke-Adapter {
    param(
        [string]$Operation,
        [string]$ResourceType,
        [string]$InputJson = "{}"
    )

    # Resolve adapter directory from PSScriptRoot (tests folder) at call time
    $testsDir   = $PSScriptRoot
    if ([string]::IsNullOrWhiteSpace($testsDir)) {
        throw "PSScriptRoot is empty; cannot resolve adapter path."
    }
    $adapterDir  = Split-Path -Parent $testsDir
    $adapterPath = Join-Path -Path $adapterDir -ChildPath "adapter.py"

    if (-not (Test-Path -LiteralPath $adapterPath)) {
        throw "Adapter script not found at '$adapterPath'."
    }

    $scriptPath = (Resolve-Path -LiteralPath $adapterPath).Path
    Write-Host "CMD: $PythonExe `"$scriptPath`" adapter --operation $Operation --input $InputJson --ResourceType $ResourceType" -ForegroundColor Yellow

    Push-Location -LiteralPath $adapterDir
    try {
        $stdout = & $PythonExe $scriptPath adapter --operation $Operation --input $InputJson --ResourceType $ResourceType 2> stderr.txt
        $stderr = Get-Content -LiteralPath stderr.txt -Raw -ErrorAction SilentlyContinue
    }
    finally {
        Pop-Location
    }

    [pscustomobject]@{
        ExitCode = $LASTEXITCODE
        StdOut   = ($stdout | Out-String).Trim()
        StdErr   = ($stderr | Out-String).Trim()
    }
}

Describe "Python Adapter - GET Operation" {
    It "should return wrapper JSON with actualState" {
        $rt = "PythonTest/Get"
        $json = '{"name":"pkg","_exist":true}'

        $result = Invoke-Adapter -Operation "get" -ResourceType $rt -InputJson $json

        $result.ExitCode | Should -Be 0 -Because $result.StdErr
        $result.StdOut   | Should -Match '^\{.*\}$' -Because $result.StdErr

        $payload = $result.StdOut | ConvertFrom-Json

        $payload.metadata."Microsoft.DSC".operation | Should -Be "Get"
        $payload.type  | Should -Be "Microsoft.DSC.Adapters/Python"
        $payload.result[0].type | Should -Be $rt
        $payload.result[0].result.actualState.name | Should -Be "pkg"
        $payload.result[0].result.actualState._exist | Should -Be $true
    }
}

