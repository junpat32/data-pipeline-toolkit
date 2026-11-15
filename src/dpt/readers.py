import csv
import re
from itertools import batched
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .config import Dataset, PipelineError


BATCH_SIZE = 4096


def mapped_sources(names, config):
    if not names or any(not name.strip() or "\x00" in name for name in names):
        raise PipelineError("Input must have nonblank column names.")
    if len(names) != len(set(names)):
        raise PipelineError("Input has duplicate column names.")
    missing = [
        column.source for column in config.columns
        if column.source not in names and not column.optional_source
    ]
    if missing:
        raise PipelineError(f"Missing mapped source columns: {', '.join(missing)}")
    return [column.source for column in config.columns if column.source in names]


def csv_batches(path, config):
    # The standard reader checks quoting after a closing quote. The grammar
    # also catches a bare quote inside an unquoted field.
    separator = re.escape(config.delimiter)
    field = rf'(?:[^"{separator}\r\n]*|"(?:[^"]|"")*")'
    grammar = re.compile(rf"{field}(?:{separator}{field})*(?:\r\n|\r|\n)?")
    with path.open(encoding=config.encoding, newline="") as stream:
        lines = []

        def track_lines():
            for line in stream:
                lines.append(line)
                yield line

        reader = csv.reader(track_lines(), delimiter=config.delimiter, strict=True)

        def records():
            while True:
                first_line = reader.line_num + 1
                try:
                    row = next(reader)
                except StopIteration:
                    return
                except csv.Error as error:
                    raise PipelineError(f"Malformed CSV near physical line {first_line}: {error}") from None
                if not grammar.fullmatch("".join(lines)):
                    raise PipelineError(f"Malformed CSV quoting near physical line {first_line}.")
                lines.clear()
                yield first_line, row

        rows = records()
        _, header = next(rows, (0, []))
        mapped = mapped_sources(header, config)
        indexes = [(name, header.index(name)) for name in mapped]

        def values():
            for first_line, row in rows:
                if len(row) != len(header):
                    raise PipelineError(
                        f"Malformed CSV at physical line {first_line}: "
                        f"expected {len(header)} fields, found {len(row)}."
                    )
                yield {name: row[index] for name, index in indexes}

        yield from batched(values(), BATCH_SIZE)


def parquet_type_matches(kind, arrow_type):
    if pa.types.is_null(arrow_type):
        return True
    if kind == "text":
        return pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type)
    if kind == "integer":
        return pa.types.is_integer(arrow_type)
    if kind == "decimal":
        return pa.types.is_integer(arrow_type) or pa.types.is_decimal(arrow_type)
    if kind == "date":
        return pa.types.is_date(arrow_type)
    return pa.types.is_timestamp(arrow_type) and arrow_type.tz is None


def parquet_batches(path, config):
    with pq.ParquetFile(path) as source:
        schema = source.schema_arrow
        mapped = mapped_sources(schema.names, config)
        for field in schema:
            if field.type.num_fields or pa.types.is_nested(field.type):
                raise PipelineError(f"Nested Parquet column is unsupported: {field.name}")
        for column in config.columns:
            if column.source in mapped:
                arrow_type = schema.field(column.source).type
                if not parquet_type_matches(column.kind, arrow_type):
                    raise PipelineError(
                        f"Incompatible Parquet type for {column.source}: "
                        f"{arrow_type} cannot load as {column.kind}."
                    )
        for batch in source.iter_batches(batch_size=BATCH_SIZE, columns=mapped):
            yield batch.to_pylist()


def read_batches(path: Path, suffix: str, config: Dataset):
    reader = csv_batches if suffix == ".csv" else parquet_batches
    try:
        yield from reader(path, config)
    except (OSError, UnicodeError, pa.ArrowException, OverflowError, ValueError):
        # Decoder and Arrow diagnostics can include source bytes or values.
        if suffix == ".csv":
            raise PipelineError(
                f"Cannot read CSV. Check file access and the configured {config.encoding} encoding."
            ) from None
        raise PipelineError("Cannot read Parquet. Check file access and file integrity.") from None
