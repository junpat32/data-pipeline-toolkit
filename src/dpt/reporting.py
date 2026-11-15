import csv
import json
from datetime import UTC
from importlib.resources import files
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import duckdb

from .config import PipelineError, identifier
from .storage import database, now


ATTEMPT_COLUMNS = """
    a.delivery_id, a.source_name, a.source_sha256, a.source_size,
    a.config_version, a.config_sha256, a.started_at, a.finished_at, a.elapsed_seconds,
    a.status, a.input_rows, a.accepted_rows, a.rejected_rows, a.warned_rows, a.finding_count
"""
DELIVERIES = f"""
    SELECT {ATTEMPT_COLUMNS}
    FROM deliveries AS d JOIN attempts AS a USING (attempt_id)
    WHERE d.dataset = ? ORDER BY a.delivery_id
"""
ATTEMPTS = f"""
    SELECT {ATTEMPT_COLUMNS}, a.original_delivery, a.message
    FROM attempts AS a WHERE a.dataset = ? ORDER BY a.started_at, a.attempt_id
"""
FINDINGS = """
    SELECT f.delivery_id, a.source_name AS source_file, f.record_number, f.field,
           f.check_code, f.severity, a.config_version
    FROM findings AS f
    JOIN deliveries AS d USING (dataset, delivery_id)
    JOIN attempts AS a USING (attempt_id)
    WHERE f.dataset = ?
    ORDER BY f.delivery_id, f.record_number, f.field, f.check_code
"""
FINDING_COUNTS = """
    SELECT severity, check_code, field, count(*) AS findings
    FROM findings WHERE dataset = ?
    GROUP BY severity, check_code, field ORDER BY severity, check_code, field
"""


def spreadsheet_cell(value):
    if isinstance(value, str) and (
        value.startswith(("'", "\t", "\r", "\n"))
        or value.lstrip().startswith(("=", "+", "-", "@", "＝", "＋", "－", "＠"))
    ):
        return "'" + value
    return value


def write_csv(path, cursor):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(column[0] for column in cursor.description)
        while rows := cursor.fetchmany(4096):
            writer.writerows([spreadsheet_cell(value) for value in row] for row in rows)


def retail_sql(name):
    return files("dpt.sql").joinpath(f"retail_{name}.sql").read_text(encoding="utf-8")


def write_package(connection, staging, dataset, generated_at):
    cursor = connection.execute(DELIVERIES, [dataset])
    names = [column[0] for column in cursor.description]
    deliveries = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    if not deliveries:
        raise PipelineError(f"No committed deliveries for {dataset}. Ingest a delivery first.")
    for delivery in deliveries:
        for field in ("started_at", "finished_at"):
            delivery[field] = delivery[field].replace(tzinfo=UTC)

    exports = {
        "deliveries.csv": DELIVERIES,
        "attempts.csv": ATTEMPTS,
        "findings.csv": FINDINGS,
        "findings_by_rule.csv": FINDING_COUNTS,
    }
    for filename, query in exports.items():
        write_csv(staging / filename, connection.execute(query, [dataset]))

    scope = None
    if dataset == "retail":
        connection.execute(retail_sql("lines"))
        first_day, last_day = connection.execute(retail_sql("scope")).fetchone()
        scope = {"start": first_day, "end": last_day}
        for name in ("daily", "products"):
            filename = f"retail_{name}.csv"
            write_csv(staging / filename, connection.execute(retail_sql(name)))
            exports[filename] = None

    manifest = {
        "schema_version": 1,
        "dataset": dataset,
        "generated_at": generated_at.isoformat(),
        "date_scope": scope,
        "deliveries": deliveries,
        "files": sorted(exports),
    }
    (staging / "manifest.json").write_text(
        json.dumps(manifest, indent=2, default=lambda value: value.isoformat()) + "\n",
        encoding="utf-8",
    )
    (staging / "_SUCCESS").touch()


def publish(dataset: str, work_dir: Path, output: Path):
    identifier(dataset)
    output = output.resolve()
    generated_at = now()
    stamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
    destination = output / f"{dataset}-{stamp}-{uuid4().hex[:12]}"
    try:
        output.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(dir=output, prefix=".staging-") as folder:
            staging = Path(folder)
            with database(work_dir, read_only=True) as connection:
                write_package(connection, staging, dataset, generated_at)
            staging.rename(destination)
    except (duckdb.Error, OSError) as error:
        raise PipelineError(
            "Could not publish report. Check output permissions, disk space, and the dataset schema."
        ) from error
    return destination
