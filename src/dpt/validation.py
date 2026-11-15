import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext


INTEGER = re.compile(r"[+-]?[0-9]+")
DECIMAL = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


@dataclass(frozen=True)
class Finding:
    field: str
    code: str
    severity: str


def is_missing(value):
    return value is None or (isinstance(value, str) and not value.strip())


def convert(value, column):
    if isinstance(value, str):
        value = value.strip()
    match column.kind:
        case "text":
            if "\x00" in value:
                raise ValueError("NUL in text")
            return value
        case "integer":
            if isinstance(value, str) and not INTEGER.fullmatch(value):
                raise ValueError("Invalid integer")
            number = int(value)
            if not -(2**63) <= number < 2**63:
                raise ValueError("Integer out of range")
            return number
        case "decimal":
            if isinstance(value, str) and not DECIMAL.fullmatch(value):
                raise ValueError("Invalid decimal")
            number = Decimal(value)
            with localcontext() as context:
                context.prec = column.precision
                quantized = number.quantize(Decimal(1).scaleb(-column.scale))
            if quantized != number:
                raise ValueError("Too many fractional digits")
            return quantized
        case "date":
            if isinstance(value, date):
                return value
            return datetime.strptime(value, column.format).date()
        case "timestamp":
            if isinstance(value, datetime):
                if getattr(value, "nanosecond", 0):
                    raise ValueError("Timestamp exceeds microsecond precision")
                return value
            return datetime.strptime(value, column.format)
    raise AssertionError(f"Unrecognized configured type: {column.kind}")


def validate(record, config):
    values = {}
    missing = set()
    findings = []
    for column in config.columns:
        value = record.get(column.source)
        if is_missing(value):
            missing.add(column.target)
            values[column.target] = None
            if column.required:
                findings.append(Finding(column.target, "REQUIRED", "error"))
        else:
            try:
                values[column.target] = convert(value, column)
            except (ValueError, InvalidOperation, OverflowError):
                values[column.target] = None
                findings.append(Finding(column.target, "INVALID_TYPE", "error"))

    for rule in config.rules:
        value = values[rule.field]
        if rule.kind == "numeric_bounds" and value is not None:
            if (rule.minimum is not None and value < rule.minimum) or (
                rule.maximum is not None and value > rule.maximum
            ):
                findings.append(Finding(rule.field, rule.code, "error"))
        elif rule.kind == "warn_if_missing" and rule.field in missing:
            if values[rule.when_field] == rule.equals:
                findings.append(Finding(rule.field, rule.code, "warning"))
    return values, findings
