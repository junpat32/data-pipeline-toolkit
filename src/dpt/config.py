import codecs
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path


class PipelineError(Exception):
    """An actionable error suitable for the command line."""


IDENTIFIER = re.compile(r"[a-z][a-z0-9_]{0,62}")
CHECK_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,62}")
DELIVERY_ID = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,79}")
SQL_TYPES = {
    "text": "VARCHAR",
    "integer": "BIGINT",
    "date": "DATE",
    "timestamp": "TIMESTAMP",
}
BUILTIN_CODES = {"REQUIRED", "INVALID_TYPE"}


def identifier(value):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise PipelineError("Dataset and target column names must be lowercase SQL identifiers.")
    return value


def quote_identifier(value):
    return f'"{identifier(value)}"'


def table_name(dataset):
    return f'"rows_{identifier(dataset)}"'


def read_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise PipelineError(f"Duplicate JSON key: {key}")
        result[key] = value
    return result


def check_keys(value, required, optional=()):
    if not isinstance(value, dict) or not set(required) <= value.keys():
        raise PipelineError(f"Expected a JSON object with keys {', '.join(required)}.")
    unknown = value.keys() - set(required) - set(optional)
    if unknown:
        raise PipelineError(f"Unsupported configuration keys: {', '.join(sorted(unknown))}")


@dataclass(frozen=True)
class Column:
    source: str
    target: str
    kind: str
    required: bool
    optional_source: bool
    format: str | None = None
    precision: int | None = None
    scale: int | None = None

    @property
    def sql_type(self):
        if self.kind == "decimal":
            return f"DECIMAL({self.precision}, {self.scale})"
        return SQL_TYPES[self.kind]


@dataclass(frozen=True)
class Rule:
    code: str
    kind: str
    field: str
    minimum: Decimal | None = None
    maximum: Decimal | None = None
    when_field: str | None = None
    equals: str | None = None


@dataclass(frozen=True)
class Dataset:
    name: str
    version: int
    encoding: str
    delimiter: str
    columns: tuple[Column, ...]
    rules: tuple[Rule, ...]
    fingerprint: str


def parse_column(raw):
    check_keys(raw, ("source", "target", "type"), (
        "required", "optional_source", "format", "precision", "scale",
    ))
    source, kind = raw["source"], raw["type"]
    target = identifier(raw["target"])
    if not isinstance(source, str) or not source.strip() or "\x00" in source:
        raise PipelineError(f"Invalid source column name for {target}.")
    if target in {"delivery_id", "record_number"}:
        raise PipelineError(f"Reserved target column: {target}")
    if kind not in (*SQL_TYPES, "decimal"):
        raise PipelineError(f"Unsupported type for {target}.")
    required = raw.get("required", False)
    optional_source = raw.get("optional_source", False)
    if type(required) is not bool or type(optional_source) is not bool:
        raise PipelineError(f"Column flags must be booleans for {target}.")
    if required and optional_source:
        raise PipelineError(f"Required column {target} cannot have an optional source.")

    format_string = raw.get("format")
    if kind in {"date", "timestamp"}:
        # Limit formats to portable numeric calendar dates and naive timestamps.
        if not isinstance(format_string, str) or not re.fullmatch(
            r"(?:[^%]|%[YmdHMSf%])+", format_string
        ):
            raise PipelineError(f"Use a numeric, timezone-free format for {target}.")
        directives = set(re.findall(r"%([YmdHMSf%])", format_string))
        if not {"Y", "m", "d"} <= directives or (
            kind == "date" and directives & {"H", "M", "S", "f"}
        ):
            raise PipelineError(f"Invalid calendar or time directives for {target}.")
        try:
            example = datetime(2025, 1, 2, 3, 4, 5)
            datetime.strptime(example.strftime(format_string), format_string)
        except (ValueError, re.error):
            raise PipelineError(f"Invalid date or timestamp format for {target}.") from None
    elif "format" in raw:
        raise PipelineError(f"Format is only supported for dates and timestamps: {target}")

    precision, scale = raw.get("precision"), raw.get("scale")
    if kind == "decimal":
        if (
            type(precision) is not int or type(scale) is not int
            or not 1 <= precision <= 38 or not 0 <= scale <= precision
        ):
            raise PipelineError(f"Expected decimal precision from 1 to 38 and scale from 0 to precision for {target}.")
    elif {"precision", "scale"} & raw.keys():
        raise PipelineError(f"Precision and scale are only supported for decimals: {target}")
    return Column(source, target, kind, required, optional_source, format_string, precision, scale)


def numeric_bound(raw):
    if type(raw) not in (int, str):
        raise PipelineError("Numeric bounds must be integers or decimal strings.")
    try:
        bound = Decimal(raw)
    except InvalidOperation:
        raise PipelineError("Invalid numeric bound.") from None
    if not bound.is_finite():
        raise PipelineError("Numeric bounds must be finite.")
    return bound


def parse_rule(raw, columns):
    check_keys(raw, ("code", "type", "field"), ("min", "max", "when"))
    code, kind, field = raw["code"], raw["type"], identifier(raw["field"])
    if not isinstance(code, str) or not CHECK_CODE.fullmatch(code) or code in BUILTIN_CODES:
        raise PipelineError("Use a unique uppercase check code distinct from the built-in codes.")
    if field not in columns:
        raise PipelineError(f"Rule {code} refers to an unknown column: {field}")
    if kind == "numeric_bounds":
        check_keys(raw, ("code", "type", "field"), ("min", "max"))
        if columns[field].kind not in {"integer", "decimal"} or not {"min", "max"} & raw.keys():
            raise PipelineError(f"Rule {code} requires a numeric column and at least one bound.")
        minimum = numeric_bound(raw["min"]) if "min" in raw else None
        maximum = numeric_bound(raw["max"]) if "max" in raw else None
        if minimum is not None and maximum is not None and minimum > maximum:
            raise PipelineError(f"Rule {code} has a minimum greater than its maximum.")
        return Rule(code, kind, field, minimum, maximum)
    if kind == "warn_if_missing":
        check_keys(raw, ("code", "type", "field", "when"))
        check_keys(raw["when"], ("field", "equals"))
        when_field = identifier(raw["when"]["field"])
        equals = raw["when"]["equals"]
        if when_field not in columns or columns[when_field].kind != "text":
            raise PipelineError(f"Rule {code} requires a mapped text condition field.")
        if not isinstance(equals, str) or not equals or equals.strip() != equals:
            raise PipelineError(f"Rule {code} requires a nonblank, trimmed condition value.")
        return Rule(code, kind, field, when_field=when_field, equals=equals)
    raise PipelineError(f"Unsupported rule type for {code}.")


def load_config(path: Path):
    try:
        raw = json.loads(path.read_text(encoding="utf-8-sig"), object_pairs_hook=read_object)
    except (OSError, UnicodeError, ValueError) as error:
        raise PipelineError(f"Cannot read dataset configuration: {error}") from None
    check_keys(raw, ("dataset", "version", "csv", "columns", "rules"))
    name, version = identifier(raw["dataset"]), raw["version"]
    if type(version) is not int or not 1 <= version <= 2**31 - 1:
        raise PipelineError("Config version must be a positive 32-bit integer.")

    check_keys(raw["csv"], ("encoding", "delimiter"))
    encoding, delimiter = raw["csv"]["encoding"], raw["csv"]["delimiter"]
    if not isinstance(encoding, str):
        raise PipelineError("CSV encoding must be a text encoding name.")
    try:
        encoding = codecs.lookup(encoding).name
        b"".decode(encoding)
    except (LookupError, UnicodeError):
        raise PipelineError("CSV encoding must be a supported text encoding.") from None
    if not isinstance(delimiter, str) or len(delimiter) != 1 or delimiter in '\r\n"\x00':
        raise PipelineError("CSV delimiter must be one character other than a quote, newline, or NUL.")

    if not isinstance(raw["columns"], list) or not raw["columns"]:
        raise PipelineError("Configure at least one column.")
    columns = tuple(parse_column(column) for column in raw["columns"])
    by_target = {column.target: column for column in columns}
    if len(by_target) != len(columns) or len({column.source for column in columns}) != len(columns):
        raise PipelineError("Source and target column mappings must be unique.")
    if all(column.optional_source for column in columns):
        raise PipelineError("Configure at least one nonoptional source column.")
    if not isinstance(raw["rules"], list):
        raise PipelineError("Rules must be a JSON array.")
    rules = tuple(parse_rule(rule, by_target) for rule in raw["rules"])
    if len({rule.code for rule in rules}) != len(rules):
        raise PipelineError("Rule codes must be unique.")

    normalized = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    fingerprint = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    return Dataset(name, version, encoding, delimiter, columns, rules, fingerprint)
