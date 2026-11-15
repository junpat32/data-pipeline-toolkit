# First-release verification

Run on Windows PowerShell with Python 3.12.14 on 13 November 2026. The virtual environment started empty. Dependency installation used network access, and the subsequent demo and tests ran with the sandbox's network restrictions.

## Install and test

```powershell
python -m venv .venv
. ./.venv/Scripts/Activate.ps1
python -m pip --disable-pip-version-check install --no-cache-dir -e '.[test]'
python -m pip check
$testDir = Join-Path (Get-Location) ('work-test-' + [guid]::NewGuid().ToString('N').Substring(0,8))
$null = New-Item -ItemType Directory -Path $testDir
$env:PYTEST_DEBUG_TEMPROOT = $testDir
python -m pytest
```

The install succeeded and `pip check` found no broken requirements. **15 tests passed.** The local temp-root override addressed restricted access to pytest's default Windows temp directory. The suite uses synthetic inputs and covers CSV and typed Parquet equivalence, preserved file hashes, record numbers, multiple findings, malformed and incompatible sources, locking, config changes, reruns, transactional rollback after a written batch, failed report publication, inventory reuse, and dashboard filters and zero denominators.

A wheel built with `python -m pip wheel --no-deps --no-build-isolation --wheel-dir work-build .`. Inspection confirmed it includes the `dpt` entry point and all four retail SQL resources.

## Retail demo and dashboard

```powershell
dpt ingest --config configs/retail.json --source data/demo/retail-01.csv --delivery demo-01 --work-dir work
dpt ingest --config configs/retail.json --source data/demo/retail-02.csv --delivery demo-02 --work-dir work
$retailReport = dpt report --dataset retail --work-dir work --output reports
python -m streamlit run dashboard/app.py --server.headless true --server.address 127.0.0.1 --server.port 8501 -- --report-dir "$retailReport"
```

| Delivery | Input | Accepted | Rejected | Warned | Findings |
| --- | ---: | ---: | ---: | ---: | ---: |
| `demo-01` | 5 | 4 | 1 | 1 | 2 |
| `demo-02` | 4 | 4 | 0 | 0 | 0 |

The dashboard opened in Microsoft Edge with both deliveries and a package date range of 3 January to 5 February 2025. It showed £110.00 gross, £26.00 negative-line value, and £84.00 net, with 75% UK customer ID coverage (3 of 4). Product `0020` ranked first at £16.00. Selecting France in the browser gave £44.00 gross, £16.00 negative-line value, and £28.00 net, with UK coverage unavailable (0 of 0). Delivery quality totals stayed at 9 input, 8 accepted, 1 rejected, 1 warned, and 2 findings. The automated dashboard test verified the single-day zero-gross case and an empty country selection. [Dashboard screenshot](retail-dashboard.png)

## Repeat, conflict, and recovery

```powershell
dpt ingest --config configs/retail.json --source data/demo/retail-01.csv --delivery demo-01 --work-dir work
dpt ingest --config configs/retail.json --source data/demo/retail-01.csv --delivery copy-demo-01 --work-dir work
dpt ingest --config configs/retail.json --source data/demo/retail-02.csv --delivery demo-01 --work-dir work
```

Both repeats returned exit code 0 and reported the original delivery. The changed input returned exit code 1 with a committed-ID conflict. The daily export SHA-256 stayed the same after publishing another package. The automated test also compared the daily exports byte for byte.

The [README retry example](../README.md#diagnose-and-retry-a-failed-delivery) returned exit code 1 with `Malformed CSV at physical line 2: expected 8 fields, found 2.` The first report recorded 4 accepted rows for `demo-01` and 0 committed rows for the failed attempt. The corrected import with the same `demo-02` ID added 4 accepted rows and published a two-delivery package.

## Inventory and exact UCI slice

```powershell
dpt ingest --config configs/inventory.json --source data/demo/inventory-01.csv --delivery inventory-demo-01 --work-dir work
dpt report --dataset inventory --work-dir work --output reports
$source = '../Data Quality Monitor/test-data/retail-pilot-source/online_retail_first_10000.csv'
Get-FileHash -Algorithm SHA256 -LiteralPath $source
dpt ingest --config configs/retail.json --source "$source" --delivery uci-first-10000 --work-dir work-uci
dpt report --dataset retail --work-dir work-uci --output reports-uci
```

Inventory committed 2 of 3 records and published 1 `NEGATIVE_STOCK` finding. The UCI input hash matched the brief. The 1–5 December 2010 convenience slice yielded 10,000 accepted records, 2,280 UK lines with missing customer IDs, 130 negative quantities, and 46 zero prices. There were 9,403 UK lines, of which 7,123 had customer IDs.

GitHub Actions is configured for Windows and Ubuntu. The workflow awaits its first run on GitHub, and Ubuntu verification remains pending.
