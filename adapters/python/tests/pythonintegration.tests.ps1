# pythonintegration.tests.ps1 - DSC resource integration tests for Python adapter

param(
    [string]$DscExe = "dsc"
)

# Helper to run dsc resource get
function global:Invoke-DscResourceGet {
    param(
        [string]$ResourceType,
        [string]$InputJson = "{}"
    )

    Write-Host "CMD: $DscExe resource get --resource `"$ResourceType`" --input '$InputJson'" -ForegroundColor Yellow

    $stdout = & $DscExe resource get --resource $ResourceType --input $InputJson 2> stderr_dsc.txt
    $stderr = Get-Content -Path stderr_dsc.txt -Raw -ErrorAction SilentlyContinue

    [pscustomobject]@{
        ExitCode = $LASTEXITCODE
        StdOut   = ($stdout | Out-String).Trim()
        StdErr   = ($stderr | Out-String).Trim()
    }
}

# Helper to run dsc resource set
function global:Invoke-DscResourceSet {
    param(
        [string]$ResourceType,
        [string]$InputJson = "{}"
    )

    Write-Host "CMD: $DscExe resource set --resource `"$ResourceType`" --input '$InputJson'" -ForegroundColor Yellow

    $stdout = & $DscExe resource set --resource $ResourceType --input $InputJson 2> stderr_dsc.txt
    $stderr = Get-Content -Path stderr_dsc.txt -Raw -ErrorAction SilentlyContinue

    [pscustomobject]@{
        ExitCode = $LASTEXITCODE
        StdOut   = ($stdout | Out-String).Trim()
        StdErr   = ($stderr | Out-String).Trim()
    }
}

# Helper to run dsc resource test
function global:Invoke-DscResourceTest {
    param(
        [string]$ResourceType,
        [string]$InputJson = "{}"
    )

    Write-Host "CMD: $DscExe resource test --resource `"$ResourceType`" --input '$InputJson'" -ForegroundColor Yellow

    $stdout = & $DscExe resource test --resource $ResourceType --input $InputJson 2> stderr_dsc.txt
    $stderr = Get-Content -Path stderr_dsc.txt -Raw -ErrorAction SilentlyContinue

    [pscustomobject]@{
        ExitCode = $LASTEXITCODE
        StdOut   = ($stdout | Out-String).Trim()
        StdErr   = ($stderr | Out-String).Trim()
    }
}

# Helper to run dsc resource export
function global:Invoke-DscResourceExport {
    param(
        [string]$ResourceType,
        [string]$InputJson = "{}"
    )

    Write-Host "CMD: $DscExe resource export --resource `"$ResourceType`" --input '$InputJson'" -ForegroundColor Yellow

    $stdout = & $DscExe resource export --resource $ResourceType --input $InputJson 2> stderr_dsc.txt
    $stderr = Get-Content -Path stderr_dsc.txt -Raw -ErrorAction SilentlyContinue

    [pscustomobject]@{
        ExitCode = $LASTEXITCODE
        StdOut   = ($stdout | Out-String).Trim()
        StdErr   = ($stderr | Out-String).Trim()
    }
}

Describe "Python Adapter - GET Operation via DSC" {
    It "should return wrapper JSON with actualState" {
        $rt = "PythonTest/Get"
        $json = '{"name":"pkg","_exist":true}'

        $result = Invoke-DscResourceGet -ResourceType $rt -InputJson $json

        $result.ExitCode | Should -Be 0 -Because $result.StdErr
        $result.StdOut   | Should -Match '^\{.*\}$' -Because "Expected JSON output"

        $payload = $result.StdOut | ConvertFrom-Json

        # DSC engine wraps the adapter's response in an "actualState" envelope
        # Structure: { "actualState": { "metadata": {...}, "result": [...] } }
        $payload.actualState | Should -Not -BeNullOrEmpty -Because "DSC engine wraps output in actualState"
        
        $wrapped = $payload.actualState
        $wrapped.metadata."Microsoft.DSC".operation | Should -Be "Get"
        $wrapped.type | Should -Be "Microsoft.DSC.Adapters/Python"
        $wrapped.result | Should -Not -BeNullOrEmpty
        
        $resourceResult = $wrapped.result[0]
        $resourceResult.type | Should -Be $rt
        $resourceResult.result.actualState.name | Should -Be "pkg"
        $resourceResult.result.actualState._exist | Should -Be $true
    }
}

Describe "Python Adapter - SET Operation via DSC" {
    It "should apply desired state and report changes" {
        $rt = "PythonTest/Set"
        $json = '{"name":"curl","_exist":false}'

        $result = Invoke-DscResourceSet -ResourceType $rt -InputJson $json

        $result.ExitCode | Should -Be 0 -Because $result.StdErr
        $result.StdOut   | Should -Match '^\{.*\}$' -Because "Expected JSON output"

        $payload = $result.StdOut | ConvertFrom-Json

        # DSC set typically returns beforeState, afterState, changedProperties
        $payload.beforeState | Should -Not -BeNullOrEmpty
        $payload.afterState | Should -Not -BeNullOrEmpty

        # afterState should reflect desired _exist=false
        $payload.afterState.name | Should -Be "curl"
        $payload.afterState._exist | Should -Be $false

        # changedProperties should list _exist
        $payload.changedProperties | Should -Contain "_exist"
    }
}

Describe "Python Adapter - TEST Operation via DSC" {
    It "should compare actual vs desired and report diffs" {
        $rt = "PythonTest/Test"
        # Desired: _exist=true; TestOnlyResource.test() will simulate actual=false → drift
        $json = '{"name":"pkg","desired_exist":true,"_exist":false}'

        $result = Invoke-DscResourceTest -ResourceType $rt -InputJson $json

        $result.ExitCode | Should -Be 0 -Because $result.StdErr
        $result.StdOut   | Should -Match '^\{.*\}$' -Because "Expected JSON output"

        $payload = $result.StdOut | ConvertFrom-Json

        # DSC test returns actualState, desiredState, inDesiredState, differingProperties
        $payload.actualState | Should -Not -BeNullOrEmpty
        $payload.desiredState | Should -Not -BeNullOrEmpty
        $payload.inDesiredState | Should -Be $false
        $payload.differingProperties | Should -Contain "_exist"
    }
}

Describe "Python Adapter - EXPORT Operation via DSC" {
    It "should return exported package list" {
        $rt = "PythonTest/Export"
        $json = '{}'

        $result = Invoke-DscResourceExport -ResourceType $rt -InputJson $json

        $result.ExitCode | Should -Be 0 -Because $result.StdErr
        $result.StdOut   | Should -Match '^\{.*\}$' -Because "Expected JSON output"

        $payload = $result.StdOut | ConvertFrom-Json

        # DSC export returns a configuration document with resources array
        # Structure: { "$schema": "...", "resources": [ { "type": "...", "properties": { "packages": [...] } } ] }
        $payload.resources | Should -Not -BeNullOrEmpty -Because "DSC export wraps output in resources array"
        $payload.resources.Count | Should -BeGreaterThan 0

        # Extract the first resource's properties (where packages live)
        $exported = $payload.resources[0].properties

        # Verify structure
        $exported.packages | Should -Not -BeNullOrEmpty
        $exported.packages.Count | Should -BeGreaterThan 0
        
        # Verify first package
        $exported.packages[0].name | Should -Be "alpha"
        $exported.packages[0].version | Should -Be "1.0.0"
        $exported.packages[0]._exist | Should -Be $true
        
        # Verify second package
        $exported.packages[1].name | Should -Be "beta"
        $exported.packages[1].version | Should -Be "2.0.0"
        $exported.packages[1]._exist | Should -Be $true
    }
}