param (
    [Parameter(Mandatory = $true)]
    [string]$Path
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$resolvedPath = (Resolve-Path -LiteralPath $Path).Path
$textFilePattern = '\.(?:json|config|xml|ya?ml|txt|ps1|psm1|cmd|bat|sh|env|ini|properties)$'
$credentialPatterns = @(
    [regex]::new('mongodb(?:\+srv)?://[^/\s:@]+:[^@\s/]+@', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase),
    [regex]::new('(?:Password|Pwd|AccountKey|SharedAccessKey)\s*=\s*[^;\s"]{4,}', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase),
    [regex]::new('"(?:password|clientSecret|client_secret|apiKey|accessKey)"\s*:\s*"(?!\s*(?:<|\$\(|\$\{))[^"]{4,}"', [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
)
$findings = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)

function Get-StringValues {
    param (
        [AllowNull()]
        [object]$Value
    )

    if ($null -eq $Value) {
        return
    }

    if ($Value -is [string]) {
        $Value
        return
    }

    if ($Value -is [System.Collections.IDictionary]) {
        foreach ($item in $Value.Values) {
            Get-StringValues -Value $item
        }
        return
    }

    if ($Value -is [System.Management.Automation.PSCustomObject]) {
        foreach ($property in $Value.PSObject.Properties) {
            Get-StringValues -Value $property.Value
        }
        return
    }

    if ($Value -is [System.Collections.IEnumerable]) {
        foreach ($item in $Value) {
            Get-StringValues -Value $item
        }
    }
}

function Test-ArtifactEntry {
    param (
        [Parameter(Mandatory = $true)]
        [string]$EntryName,

        [Parameter(Mandatory = $true)]
        [scriptblock]$ReadContent
    )

    $fileName = [System.IO.Path]::GetFileName($EntryName)
    if ($fileName -match '^appsettings\..+\.json$') {
        [void]$findings.Add("$EntryName (environment-specific settings file)")
    }

    if ($EntryName -notmatch $textFilePattern) {
        return
    }

    [string]$content = & $ReadContent
    $valuesToScan = @($content)
    if ($EntryName -match '\.json$' -and -not [string]::IsNullOrWhiteSpace($content)) {
        try {
            $json = $content | ConvertFrom-Json
        } catch {
            throw "Unable to parse JSON artifact entry: $EntryName"
        }
        $valuesToScan += @(Get-StringValues -Value $json)
    }

    foreach ($value in $valuesToScan) {
        foreach ($pattern in $credentialPatterns) {
            if ($pattern.IsMatch($value)) {
                [void]$findings.Add("$EntryName (possible embedded credential)")
                return
            }
        }
    }
}

if (Test-Path -LiteralPath $resolvedPath -PathType Container) {
    Get-ChildItem -LiteralPath $resolvedPath -Recurse -File | ForEach-Object {
        $file = $_
        $relativePath = $file.FullName.Substring($resolvedPath.Length).TrimStart([char[]]@('\', '/'))
        Test-ArtifactEntry -EntryName $relativePath -ReadContent {
            Get-Content -LiteralPath $file.FullName -Raw
        }
    }
} elseif ([System.IO.Path]::GetExtension($resolvedPath) -ieq ".zip") {
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $archive = [System.IO.Compression.ZipFile]::OpenRead($resolvedPath)
    try {
        foreach ($entry in $archive.Entries) {
            if ([string]::IsNullOrEmpty($entry.Name)) {
                continue
            }

            Test-ArtifactEntry -EntryName $entry.FullName -ReadContent {
                $stream = $entry.Open()
                $reader = [System.IO.StreamReader]::new($stream)
                try {
                    $reader.ReadToEnd()
                } finally {
                    $reader.Dispose()
                    $stream.Dispose()
                }
            }
        }
    } finally {
        $archive.Dispose()
    }
} else {
    throw "Artifact path must be a directory or a .zip file: $resolvedPath"
}

if ($findings.Count -gt 0) {
    $findingList = ($findings | Sort-Object | ForEach-Object { " - $_" }) -join [Environment]::NewLine
    throw "Unsafe publish artifact. Remove the following files or credentials before packaging or deployment:$([Environment]::NewLine)$findingList"
}

Write-Host "Artifact safety validation passed: $resolvedPath"
