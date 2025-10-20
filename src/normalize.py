# src/normalize.py
import json
import hashlib
from datetime import datetime
from dateutil import parser as dateparser

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
    """Return a stable string representation for a value to be hashed."""
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return "|".join(str(x).strip().lower() for x in v)
    return str(v).strip().lower()

def make_signature(item, keys):
    """
    Create a deterministic signature (sha256 hex) from the values of `keys` in item.
    keys: list of field names to use in order.
    """
    parts = []
    for k in keys:
        parts.append(canonicalize_value(item.get(k)))
    base = "||".join(parts)
    return hashlib.sha256(base.encode("utf-8")).hexdigest()

def normalize_company_summary(raw_item):
    addr = raw_item.get("address") or {}
    rank_val = raw_item.get("rank")
    try:
        rank_val = int(rank_val) if rank_val is not None else None
    except Exception:
        rank_val = None

    normalized = {
        "company_number": raw_item.get("company_number"),
        "title": raw_item.get("title"),
        "kind": raw_item.get("kind"),
        "company_status": raw_item.get("company_status"),
        "company_type": raw_item.get("company_type"),
        "snippet": raw_item.get("snippet"),
        "address_snippet": raw_item.get("address_snippet"),
        "address_line_1": addr.get("address_line_1") if isinstance(addr, dict) else None,
        "address_locality": addr.get("locality") if isinstance(addr, dict) else None,
        "address_country": addr.get("country") if isinstance(addr, dict) else None,
        "address_postal_code": addr.get("postal_code") if isinstance(addr, dict) else None,
        "links_self": (raw_item.get("links") or {}).get("self"),
        "date_of_creation": safe_date_iso(raw_item.get("date_of_creation")),
        "date_of_cessation": safe_date_iso(raw_item.get("date_of_cessation")),
        "rank": rank_val,
        "date_indexed": datetime.utcnow().isoformat(),
        "raw_json": json.dumps(raw_item, ensure_ascii=False),
    }

    # fields to use for dedupe signature (the ones you requested)
    signature_keys = [
        "company_number",
        "title",
        "kind",
        "company_status",
        "company_type",
        "snippet",
        "address_snippet",
        "address_line_1",
        "address_locality",
        "address_country",
        "address_postal_code",
    ]
    # compute signature
    normalized["row_signature"] = make_signature(normalized, signature_keys)
    return normalized