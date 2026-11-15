import csv
import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from filelock import FileLock
from streamlit.testing.v1 import AppTest

import dpt.reporting as reporting
from dpt.cli import main
from dpt.config import PipelineError, load_config
from dpt.ingest import ingest
from dpt.reporting import publish
from dpt.storage import database


ROOT = Path(__file__).resolve().parents[1]
DEMO = ROOT / "data" / "demo"
RETAIL = ROOT / "configs" / "retail.json"
INVENTORY = ROOT / "configs" / "inventory.json"


def csv_rows(path):
    with path.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


def query(work, sql, parameters=None):
    with database(work, read_only=True) as connection:
        return connection.execute(sql, parameters).fetchall()


def run_ingest(config, source, delivery, work):
    return main([
        "ingest", "--config", str(config), "--source", str(source),
        "--delivery", delivery, "--work-dir", str(work),
    ])


def demo_package(tmp_path, capsys):
    work = tmp_path / "work"
    for number in ("01", "02"):
        assert run_ingest(RETAIL, DEMO / f"retail-{number}.csv", f"demo-{number}", work) == 0
    capsys.readouterr()
    assert main(["report", "--dataset", "retail", "--work-dir", str(work),
                 "--output", str(tmp_path / "reports")]) == 0
    return work, Path(capsys.readouterr().out.strip())


def test_cli_reports_and_reruns(tmp_path, capsys):
    work, package = demo_package(tmp_path, capsys)
    assert (package / "_SUCCESS").is_file()
    manifest = json.loads((package / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["date_scope"] == {"start": "2025-01-03", "end": "2025-02-05"}
    assert [item["delivery_id"] for item in manifest["deliveries"]] == ["demo-01", "demo-02"]
    daily = csv_rows(package / "retail_daily.csv")
    assert [sum(Decimal(row[field]) for row in daily) for field in (
        "gross_sales_value", "negative_line_value", "net_line_value",
    )] == [110, 26, 84]
    assert [sum(int(row[field]) for row in daily) for field in (
        "accepted_lines", "sales_lines", "negative_quantity_lines", "cancellation_lines",
        "zero_price_lines", "uk_lines", "uk_lines_with_id",
    )] == [8, 6, 2, 2, 1, 4, 3]
    findings = csv_rows(package / "findings.csv")
    assert [(row["record_number"], row["check_code"], row["severity"]) for row in findings] == [
        ("3", "UK_CUSTOMER_ID_MISSING", "warning"), ("5", "NEGATIVE_UNIT_PRICE", "error"),
    ]
    assert findings[0]["source_file"] == "retail-01.csv"
    assert query(work, """
        SELECT record_number, invoice_no, stock_code, customer_id
        FROM rows_retail WHERE delivery_id = 'demo-01' ORDER BY record_number
    """)[:2] == [(1, "000101", "0010", "00042"), (2, "000101", "0020", "00042")]

    first = DEMO / "retail-01.csv"
    assert run_ingest(RETAIL, first, "demo-01", work) == 0
    assert run_ingest(RETAIL, first, "duplicate-copy", work) == 0
    assert capsys.readouterr().out.count("committed as demo-01") == 2
    assert run_ingest(RETAIL, DEMO / "retail-02.csv", "demo-01", work) == 1
    assert "different input bytes" in capsys.readouterr().err
    second_package = publish("retail", work, tmp_path / "reports")
    assert second_package != package
    assert (second_package / "retail_daily.csv").read_bytes() == (package / "retail_daily.csv").read_bytes()
    assert [row["status"] for row in csv_rows(second_package / "attempts.csv")] == [
        "committed", "committed", "skipped", "skipped", "failed",
    ]


def test_equivalent_csv_and_scalar_parquet(tmp_path):
    parquet = tmp_path / "retail.parquet"
    source = DEMO / "retail-01.csv"
    rows = csv_rows(source)
    pq.write_table(pa.table({
        "InvoiceNo": [row["InvoiceNo"] for row in rows],
        "StockCode": [row["StockCode"] for row in rows],
        "Description": [row["Description"] for row in rows],
        "Quantity": [int(row["Quantity"]) for row in rows],
        "InvoiceDate": [datetime.fromisoformat(row["InvoiceDate"]) for row in rows],
        "UnitPrice": pa.array([Decimal(row["UnitPrice"]) for row in rows], type=pa.decimal128(18, 4)),
        "CustomerID": [row["CustomerID"] or None for row in rows],
        "Country": [row["Country"] for row in rows],
    }), parquet)
    config = load_config(RETAIL)
    results = []
    for kind, path in (("csv", source), ("parquet", parquet)):
        work = tmp_path / kind
        ingest(config, path, "first", work)
        fingerprint = hashlib.sha256(path.read_bytes()).hexdigest()
        assert (work / "archive" / f"{fingerprint}.source").read_bytes() == path.read_bytes()
        assert query(work, "SELECT source_sha256, source_size FROM attempts") == [
            (fingerprint, path.stat().st_size),
        ]
        results.append((
            query(work, "SELECT * FROM rows_retail ORDER BY record_number"),
            query(work, "SELECT * FROM findings ORDER BY record_number"),
        ))
    assert results[0] == results[1]


def test_rollback_after_a_written_batch_and_retry(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("dpt.readers.BATCH_SIZE", 2)
    work = tmp_path / "work"
    first = DEMO / "retail-01.csv"
    assert run_ingest(RETAIL, first, "first", work) == 0
    malformed = tmp_path / "broken.csv"
    valid = (DEMO / "retail-02.csv").read_text(encoding="utf-8")
    malformed.write_text(valid + '"bad","only two fields"\n', encoding="utf-8")
    assert run_ingest(RETAIL, malformed, "second", work) == 1
    assert "Malformed CSV" in capsys.readouterr().err
    assert query(work, "SELECT delivery_id, count(*) FROM rows_retail GROUP BY 1") == [("first", 4)]
    assert query(work, "SELECT input_rows, accepted_rows FROM attempts WHERE status = 'failed'") == [(4, 0)]
    assert query(work, "SELECT count(*) FROM findings WHERE delivery_id = 'second'") == [(0,)]
    assert run_ingest(RETAIL, DEMO / "retail-02.csv", "second", work) == 0
    assert query(work, "SELECT count(*) FROM rows_retail") == [(8,)]


def test_inventory_and_rejected_record_checks(tmp_path):
    source = tmp_path / "=inventory.csv"
    source.write_text(
        "warehouse,sku,quantity_on_hand,as_of_date\nNorth,0010,0,2025-01-31\n"
        "South,0030,-1,bad-date\nNorth, ,8,2025-01-31\n", encoding="utf-8",
    )
    work = tmp_path / "work"
    ingest(load_config(INVENTORY), source, "jan", work)
    package = publish("inventory", work, tmp_path / "reports")
    assert not (package / "retail_daily.csv").exists()
    assert query(work, "SELECT sku, as_of_date FROM rows_inventory") == [("0010", date(2025, 1, 31))]
    counts = csv_rows(package / "deliveries.csv")[0]
    assert [counts[field] for field in ("input_rows", "accepted_rows", "rejected_rows", "finding_count")] == [
        "3", "1", "2", "3",
    ]
    findings = csv_rows(package / "findings.csv")
    assert {row["source_file"] for row in findings} == {"'=inventory.csv"}
    assert [(row["record_number"], row["check_code"]) for row in findings] == [
        ("2", "INVALID_TYPE"), ("2", "NEGATIVE_STOCK"), ("3", "REQUIRED"),
    ]


@pytest.mark.parametrize("contents, message", [
    ('warehouse,sku,quantity_on_hand,as_of_date\nNorth,00"10,2,2025-01-31\n', "Malformed CSV quoting"),
    ("warehouse,warehouse,sku,quantity_on_hand,as_of_date\n", "duplicate column"),
    ("warehouse,sku,as_of_date\n", "Missing mapped source columns"),
    ("warehouse,sku,quantity_on_hand,as_of_date\nNorth,0010,-2,2025-01-31\n", "Zero accepted records"),
])
def test_bad_csv_deliveries_leave_no_committed_dataset(tmp_path, contents, message):
    source = tmp_path / "input.csv"
    source.write_text(contents, encoding="utf-8")
    work = tmp_path / "work"
    with pytest.raises(PipelineError, match=message):
        ingest(load_config(INVENTORY), source, "jan", work)
    assert query(work, "SELECT count(*) FROM datasets") == [(0,)]
    assert query(work, "SELECT status FROM attempts") == [("failed",)]
    output = tmp_path / "reports"
    with pytest.raises(PipelineError, match="No committed deliveries"):
        publish("inventory", work, output)
    assert list(output.iterdir()) == []


def test_multiline_csv_keeps_record_numbers(tmp_path):
    source = tmp_path / "inventory.csv"
    source.write_text(
        'warehouse,sku,quantity_on_hand,as_of_date\n"North\nDepot",0010,1,2025-01-31\n'
        'South,0020,-1,2025-01-31\n', encoding="utf-8", newline="\n",
    )
    work = tmp_path / "work"
    ingest(load_config(INVENTORY), source, "jan", work)
    assert query(work, "SELECT record_number, warehouse FROM rows_inventory") == [(1, "North\nDepot")]
    assert query(work, "SELECT record_number FROM findings") == [(2,)]


@pytest.mark.parametrize("schema_error", ["Nested Parquet", "Incompatible Parquet"])
def test_incompatible_parquet_schema(tmp_path, schema_error):
    path = tmp_path / "invalid.parquet"
    data = {"warehouse": ["North"], "sku": ["0010"], "quantity_on_hand": [2], "as_of_date": [date(2025, 1, 31)]}
    if schema_error == "Nested Parquet":
        data["unmapped"] = [["nested"]]
    else:
        data["sku"] = [10]
    pq.write_table(pa.table(data), path)
    with pytest.raises(PipelineError, match=schema_error):
        ingest(load_config(INVENTORY), path, "jan", tmp_path / "work")


def test_config_hash_and_rebuild_boundary(tmp_path):
    original = load_config(INVENTORY)
    raw = json.loads(INVENTORY.read_text(encoding="utf-8"))
    path = tmp_path / "inventory.json"
    path.write_text(json.dumps(raw, sort_keys=True), encoding="utf-8")
    assert load_config(path).fingerprint == original.fingerprint
    source = DEMO / "inventory-01.csv"
    work = tmp_path / "work"
    ingest(original, source, "jan", work)
    raw["version"] += 1
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(PipelineError, match="fresh --work-dir"):
        ingest(load_config(path), source, "jan", work)
    raw["rules"][0]["field"] = "unknown_field"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(PipelineError, match="unknown column"):
        load_config(path)


def test_concurrent_work_directory_is_reported(tmp_path):
    with FileLock(tmp_path / ".dpt.lock"):
        with pytest.raises(PipelineError, match="work directory is busy"):
            ingest(load_config(INVENTORY), DEMO / "inventory-01.csv", "jan", tmp_path)


def test_export_failure_preserves_previous_package(tmp_path, monkeypatch, capsys):
    work, first = demo_package(tmp_path, capsys)
    real_write = reporting.write_csv

    def fail_after_write(path, cursor):
        real_write(path, cursor)
        if path.name == "attempts.csv":
            raise OSError("Simulated export failure")

    monkeypatch.setattr(reporting, "write_csv", fail_after_write)
    with pytest.raises(PipelineError, match="Could not publish report"):
        publish("retail", work, first.parent)
    assert list(first.parent.iterdir()) == [first]
    assert (first / "_SUCCESS").is_file()
    assert len(csv_rows(first / "deliveries.csv")) == 2


def test_dashboard_scope_filters_and_zero_denominators(tmp_path, monkeypatch, capsys):
    _, package = demo_package(tmp_path, capsys)
    monkeypatch.setattr("sys.argv", ["app.py", "--report-dir", str(package)])
    app = AppTest.from_file(str(ROOT / "dashboard" / "app.py")).run()
    assert not app.exception
    assert [metric.value for metric in app.metric[:4]] == ["£110.00", "£26.00", "£84.00", "23.64%"]
    assert "demo-01, demo-02" in app.caption[0].value
    assert "3 of 4 UK lines" in app.markdown[0].value
    app.multiselect[0].set_value(["France"])
    app.run()
    assert [metric.value for metric in app.metric[:4]] == ["£44.00", "£16.00", "£28.00", "36.36%"]
    assert "0 of 0 UK lines" in app.markdown[0].value
    app.date_input[0].set_value((date(2025, 1, 5), date(2025, 1, 5)))
    app.run()
    assert not app.exception
    assert [metric.value for metric in app.metric[:4]] == ["£0.00", "£0.00", "£0.00", "Unavailable"]
    assert [metric.value for metric in app.metric[4:]] == ["9", "8", "1", "1", "2"]
    app.multiselect[0].set_value([])
    app.run()
    assert app.info[0].value == "No accepted lines match these filters."
