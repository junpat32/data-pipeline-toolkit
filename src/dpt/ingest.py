from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import duckdb

from .config import DELIVERY_ID, Dataset, PipelineError, table_name
from .readers import read_batches
from .storage import database, now, register_dataset, snapshot, transaction
from .validation import validate


@dataclass
class Counts:
    input_rows: int = 0
    accepted_rows: int = 0
    rejected_rows: int = 0
    warned_rows: int = 0
    finding_count: int = 0
    errors_by_code: Counter = field(default_factory=Counter)


@dataclass
class Attempt:
    config: Dataset
    source: Path
    delivery: str
    attempt_id: str = field(default_factory=lambda: uuid4().hex)
    started_at: datetime = field(default_factory=now)
    started_tick: float = field(default_factory=perf_counter)
    counts: Counts = field(default_factory=Counts)
    fingerprint: str | None = None
    size: int | None = None

    def record(self, connection, status, *, original=None, message=None):
        counts = self.counts
        # Failed attempts have no committed accepted or warned rows.
        accepted = counts.accepted_rows if status == "committed" else 0
        warned = counts.warned_rows if status == "committed" else 0
        connection.execute(
            "INSERT INTO attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                self.attempt_id, self.config.name, self.delivery, self.source.name,
                self.fingerprint, self.size, self.config.version, self.config.fingerprint,
                self.started_at.replace(tzinfo=None), now().replace(tzinfo=None),
                perf_counter() - self.started_tick, status,
                counts.input_rows, accepted, counts.rejected_rows, warned, counts.finding_count,
                original, message,
            ],
        )


def insert_batches(connection, attempt, preserved, suffix):
    config = attempt.config
    counts = attempt.counts
    placeholders = ", ".join("?" for _ in range(len(config.columns) + 2))
    insert_row = f"INSERT INTO {table_name(config.name)} VALUES ({placeholders})"
    for batch in read_batches(preserved, suffix, config):
        accepted, findings = [], []
        for record in batch:
            counts.input_rows += 1
            record_number = counts.input_rows
            values, checks = validate(record, config)
            errors = [check.code for check in checks if check.severity == "error"]
            counts.finding_count += len(checks)
            findings.extend(
                (config.name, attempt.delivery, record_number, check.field, check.code, check.severity)
                for check in checks
            )
            if errors:
                counts.rejected_rows += 1
                counts.errors_by_code.update(errors)
            else:
                counts.accepted_rows += 1
                counts.warned_rows += bool(checks)
                accepted.append((
                    attempt.delivery, record_number, *values.values(),
                ))
        if accepted:
            connection.executemany(insert_row, accepted)
        if findings:
            connection.executemany("INSERT INTO findings VALUES (?, ?, ?, ?, ?, ?)", findings)
    if not counts.accepted_rows:
        detail = ", ".join(f"{code}={count}" for code, count in sorted(counts.errors_by_code.items()))
        message = f"Zero accepted records (input={counts.input_rows}, rejected={counts.rejected_rows})."
        if detail:
            message += f" Failed checks: {detail}."
        raise PipelineError(message)


def ingest(config: Dataset, source: Path, delivery: str, work_dir: Path):
    if not DELIVERY_ID.fullmatch(delivery):
        raise PipelineError("Delivery ID must start with a letter or digit and use at most 80 letters, digits, dots, underscores, or hyphens.")
    attempt = Attempt(config, source, delivery)
    with database(work_dir) as connection:
        try:
            suffix = source.suffix.lower()
            if suffix not in {".csv", ".parquet"}:
                raise PipelineError("Source must have a .csv or .parquet extension.")
            preserved, attempt.fingerprint, attempt.size = snapshot(work_dir, source)
            registered = connection.execute(
                "SELECT config_sha256 FROM datasets WHERE dataset = ?", [config.name],
            ).fetchone()
            if registered and registered[0] != config.fingerprint:
                raise PipelineError(
                    f"Configuration changed for {config.name}. Replay the deliveries in a fresh --work-dir."
                )
            existing = connection.execute(
                "SELECT source_sha256 FROM deliveries WHERE dataset = ? AND delivery_id = ?",
                [config.name, delivery],
            ).fetchone()
            if existing and existing[0] != attempt.fingerprint:
                raise PipelineError(f"Delivery {delivery} is committed with different input bytes.")
            duplicate = connection.execute(
                "SELECT delivery_id FROM deliveries WHERE dataset = ? AND source_sha256 = ?",
                [config.name, attempt.fingerprint],
            ).fetchone()
            if duplicate:
                attempt.record(connection, "skipped", original=duplicate[0])
                return f"Skipped {config.name}/{delivery}. Identical input already committed as {duplicate[0]}."

            with transaction(connection):
                if not registered:
                    register_dataset(connection, config)
                insert_batches(connection, attempt, preserved, suffix)
                attempt.record(connection, "committed")
                connection.execute("INSERT INTO deliveries VALUES (?, ?, ?, ?)", [
                    config.name, delivery, attempt.fingerprint, attempt.attempt_id,
                ])
        except (PipelineError, duckdb.Error, OSError, KeyboardInterrupt) as error:
            if isinstance(error, KeyboardInterrupt):
                message = "Interrupted."
            elif isinstance(error, PipelineError):
                message = str(error)
            else:
                message = "Could not store delivery. Check database access, available disk space, and input types."
            try:
                attempt.record(connection, "failed", message=message)
            except duckdb.Error:
                message += " The database could not save the failed attempt."
            if isinstance(error, KeyboardInterrupt):
                raise
            raise PipelineError(message) from error
    counts = attempt.counts
    return (
        f"Committed {config.name}/{delivery} in {perf_counter() - attempt.started_tick:.2f}s "
        f"(input={counts.input_rows}, accepted={counts.accepted_rows}, rejected={counts.rejected_rows}, "
        f"warned={counts.warned_rows}, findings={counts.finding_count})."
    )
