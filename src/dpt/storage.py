import hashlib
import os
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

import duckdb
from filelock import FileLock, Timeout

from .config import PipelineError, quote_identifier, table_name


SCHEMA = """
CREATE TABLE IF NOT EXISTS datasets (
    dataset VARCHAR PRIMARY KEY,
    config_version INTEGER NOT NULL,
    config_sha256 VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS attempts (
    attempt_id VARCHAR PRIMARY KEY,
    dataset VARCHAR NOT NULL,
    delivery_id VARCHAR NOT NULL,
    source_name VARCHAR NOT NULL,
    source_sha256 VARCHAR,
    source_size BIGINT,
    config_version INTEGER NOT NULL,
    config_sha256 VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP NOT NULL,
    elapsed_seconds DOUBLE NOT NULL,
    status VARCHAR NOT NULL,
    input_rows BIGINT NOT NULL,
    accepted_rows BIGINT NOT NULL,
    rejected_rows BIGINT NOT NULL,
    warned_rows BIGINT NOT NULL,
    finding_count BIGINT NOT NULL,
    original_delivery VARCHAR,
    message VARCHAR
);
CREATE TABLE IF NOT EXISTS deliveries (
    dataset VARCHAR NOT NULL,
    delivery_id VARCHAR NOT NULL,
    source_sha256 VARCHAR NOT NULL,
    attempt_id VARCHAR NOT NULL,
    PRIMARY KEY (dataset, delivery_id),
    UNIQUE (dataset, source_sha256)
);
CREATE TABLE IF NOT EXISTS findings (
    dataset VARCHAR NOT NULL,
    delivery_id VARCHAR NOT NULL,
    record_number BIGINT NOT NULL,
    field VARCHAR NOT NULL,
    check_code VARCHAR NOT NULL,
    severity VARCHAR NOT NULL
);
"""


def now():
    return datetime.now(UTC)


@contextmanager
def database(work_dir: Path, *, read_only=False):
    db_path = work_dir / "pipeline.duckdb"
    if read_only and not db_path.is_file():
        raise PipelineError("No pipeline database found. Ingest a delivery first.")
    work_dir.mkdir(parents=True, exist_ok=True)
    lock = FileLock(work_dir / ".dpt.lock", timeout=0)
    try:
        with lock:
            try:
                connection = duckdb.connect(
                    str(db_path), read_only=read_only,
                    config={"threads": 2},
                )
            except duckdb.IOException as error:
                raise PipelineError(
                    "Cannot open the work database. Check file access and close other DuckDB connections."
                ) from error
            with connection:
                if not read_only:
                    connection.execute(SCHEMA)
                yield connection
    except Timeout:
        raise PipelineError("This work directory is busy. Wait for the other ingest or report to finish.") from None


@contextmanager
def transaction(connection):
    connection.begin()
    try:
        yield
        connection.commit()
    except BaseException:
        connection.rollback()
        raise


def register_dataset(connection, config):
    connection.execute("INSERT INTO datasets VALUES (?, ?, ?)", [
        config.name, config.version, config.fingerprint,
    ])
    columns = [
        f"{quote_identifier(column.target)} {column.sql_type}"
        + (" NOT NULL" if column.required else "")
        for column in config.columns
    ]
    connection.execute(f"""
        CREATE TABLE {table_name(config.name)} (
            delivery_id VARCHAR NOT NULL,
            record_number BIGINT NOT NULL,
            {", ".join(columns)},
            PRIMARY KEY (delivery_id, record_number)
        )
    """)


def snapshot(work_dir, source):
    archive = work_dir / "archive"
    archive.mkdir(exist_ok=True)
    pending = None
    size = 0
    digest = hashlib.sha256()
    try:
        with source.open("rb") as stream, NamedTemporaryFile(
            dir=archive, prefix=".copy-", delete=False
        ) as copied:
            pending = Path(copied.name)
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                copied.write(block)
                digest.update(block)
                size += len(block)
        fingerprint = digest.hexdigest()
        preserved = archive / f"{fingerprint}.source"
        if preserved.exists():
            with preserved.open("rb") as existing:
                if hashlib.file_digest(existing, "sha256").hexdigest() != fingerprint:
                    raise PipelineError("Archived input failed its integrity check. Restore the archived file.")
        else:
            os.replace(pending, preserved)
        return preserved, fingerprint, size
    except OSError as error:
        raise PipelineError(
            f"Cannot preserve {source.name}: check source access and archive write permissions."
        ) from error
    finally:
        if pending is not None:
            pending.unlink(missing_ok=True)
