# src/normalize.py
import json
import hashlib
from datetime import datetime
from dateutil import parser as dateparser
try:
    from src.schema import TABLE_CONFIG
except ModuleNotFoundError:
    from schema import TABLE_CONFIG


def safe_date_iso(raw):
    """Return ISO date string (YYYY-MM-DD) or None."""
    if not raw:
        return None
    try:
        d = dateparser.parse(raw).date()
        return d.isoformat()
    except Exception:
        return None


def canonicalize_value(v):
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        # stable representation for lists used in signature
        return "|".join(str(x).strip().lower() for x in v)
    return str(v).strip().lower()


def make_signature(item, keys):
    text = "||".join(canonicalize_value(item.get(k)) for k in keys)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _coerce_for_schema(field_name, value):
    """
    Coerce values into simple scalar types acceptable by BQ schema:
    - lists/tuples -> joined string (semicolon-separated)
    - dict -> compact JSON string
    - leave None / scalar as-is
    """
    if value is None:
        return None

    # lists -> join to semicolon-separated string
    if isinstance(value, (list, tuple)):
        try:
            return ";".join(str(x) for x in value)
        except Exception:
            return json.dumps(value, ensure_ascii=False)

    # dicts -> JSON string
    if isinstance(value, dict):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)

    # otherwise return as-is (string/number/bool)
    return value


def normalize_record(table_name, raw_item):
    """
    Generic, easy-to-read normalizer.
    Looks up the normalize_map and signature_keys for the given table_name.
    Coerces arrays/dicts to strings so BigQuery accepts the row.
    """
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unknown table name: {table_name}")

    cfg = TABLE_CONFIG[table_name]
    normalize_map = cfg["normalize_map"]
    signature_keys = cfg["signature_keys"]

    normalized = {}

    for field, (path, parent) in normalize_map.items():
        value = None
        if parent:
            parent_obj = raw_item.get(parent) or {}
            if isinstance(parent_obj, dict):
                value = parent_obj.get(path)
        else:
            value = raw_item.get(path)

        # handle date-like fields deterministically
        if field.startswith("date_"):
            value = safe_date_iso(value)

        # if field name ends with _json, store compact JSON string
        elif field.endswith("_json") and value is not None:
            try:
                value = json.dumps(value, ensure_ascii=False)
            except Exception:
                value = str(value)

        # coerce lists/dicts into string forms for BQ compatibility
        else:
            value = _coerce_for_schema(field, value)

        normalized[field] = value

    # housekeeping
    normalized["date_indexed"] = datetime.utcnow().isoformat()
    normalized["raw_json"] = json.dumps(raw_item, ensure_ascii=False)
    normalized["row_signature"] = make_signature(normalized, signature_keys)
    return normalized
