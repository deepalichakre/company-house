# src/bq_writer.py
"""
Generic BigQuery writer using schema definitions from src/schema.py.

Usage:
  from bq_writer import insert_rows_for_table, ensure_table_exists
  ensure_table_exists("company_index")
  result = insert_rows_for_table("company_index", rows)  # rows: list[dict], must include row_signature
"""

import os
import logging
from google.cloud import bigquery
from typing import List, Dict, Set

# prefer schema-defined project/dataset but allow env override
try:
    from schema import TABLE_CONFIG, PROJECT_ID as SCHEMA_PROJECT, DATASET as SCHEMA_DATASET
except Exception:
    TABLE_CONFIG = {}
    SCHEMA_PROJECT = None
    SCHEMA_DATASET = None

PROJECT_ID = os.getenv("PROJECT_ID") or SCHEMA_PROJECT or os.getenv("GCP_PROJECT")
BQ_DATASET = os.getenv("BQ_DATASET") or SCHEMA_DATASET or "companies_house"

# initialize client using resolved project
bq = bigquery.Client(project=PROJECT_ID)

# -------------------------
# Helpers
# -------------------------
def _fq_table_id(table_name: str) -> str:
    """Return fully-qualified table id for a logical table_name (as in TABLE_CONFIG)."""
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unknown table_name '{table_name}'. Valid keys: {list(TABLE_CONFIG.keys())}")
    table = TABLE_CONFIG[table_name].get("table") or table_name
    return f"{PROJECT_ID}.{BQ_DATASET}.{table}"

def ensure_table_exists(table_name: str) -> str:
    """
    Ensure dataset and table exist for the given table_name (lookup in TABLE_CONFIG).
    Returns the fully-qualified table id string.
    """
    table_id = _fq_table_id(table_name)
    # check table exists
    try:
        bq.get_table(table_id)
        logging.info("Table %s already exists", table_id)
        return table_id
    except Exception:
        logging.info("Table %s not found, will attempt to create it.", table_id)

    # ensure dataset exists
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, BQ_DATASET)
    try:
        bq.get_dataset(dataset_ref)
    except Exception:
        logging.info("Dataset %s not found; creating in location asia-south1", BQ_DATASET)
        ds = bigquery.Dataset(dataset_ref)
        ds.location = "asia-south1"
        bq.create_dataset(ds, exists_ok=True)

    # build schema from TABLE_CONFIG entry
    schema_def = TABLE_CONFIG[table_name].get("schema")
    if not schema_def:
        raise ValueError(f"No schema defined for table_name '{table_name}' in TABLE_CONFIG")

    bq_schema = []
    for name, typ in schema_def:
        # create SchemaField objects; default mode is NULLABLE
        # BigQuery allows types like STRING, INT64, BOOL, DATE, TIMESTAMP, JSON
        bq_schema.append(bigquery.SchemaField(name, typ))

    table = bigquery.Table(table_id, schema=bq_schema)

    # if date_indexed present, set time partitioning
    if any(field_name == "date_indexed" for field_name, _ in schema_def):
        table.time_partitioning = bigquery.TimePartitioning(field="date_indexed")

    created = bq.create_table(table, exists_ok=True)
    logging.info("Created table %s (num_columns=%s)", table_id, len(bq_schema))
    return table_id

def fetch_existing_signatures(table_id: str, signatures: List[str]) -> Set[str]:
    """
    Query BigQuery for any row_signature values that already exist in the target table.
    Returns a set of signature strings.
    """
    if not signatures:
        return set()

    existing = set()
    client = bigquery.Client(project=PROJECT_ID)
    # batch signatures to avoid huge IN clauses
    chunk_size = 500
    for i in range(0, len(signatures), chunk_size):
        chunk = signatures[i : i + chunk_size]
        # Escape any single quotes inside signatures and wrap each in quotes
        escaped = [("'" + s.replace("'", "''") + "'") for s in chunk]
        placeholders = ", ".join(escaped)
        q = f"""
        SELECT row_signature FROM `{table_id}`
        WHERE row_signature IN ({placeholders})
        """
        job = client.query(q)
        for row in job:
            existing.add(row["row_signature"])
    return existing

# -------------------------
# Insert / dedupe logic
# -------------------------
def insert_rows_for_table(table_name: str, rows: List[Dict]) -> Dict:
    """
    Insert rows into the table identified by table_name (mapped in TABLE_CONFIG).
    - Rows must be dicts where keys match the table schema column names.
    - Each row should contain 'row_signature' which will be used for dedupe check.
    Returns: {"inserted": n, "skipped": m, "errors": [...]}
    """
    if table_name not in TABLE_CONFIG:
        raise ValueError(f"Unknown table_name '{table_name}'")

    # ensure table exists and get the FQ table id
    table_id = ensure_table_exists(table_name)

    # extract unique signatures from rows
    signatures = [r.get("row_signature") for r in rows if r.get("row_signature")]
    unique_signatures = list(dict.fromkeys(signatures))
    existing = fetch_existing_signatures(table_id, unique_signatures)

    # filter rows that are new
    to_insert = [r for r in rows if r.get("row_signature") not in existing]

    result = {"inserted": 0, "skipped": 0, "errors": []}
    result["skipped"] = len(rows) - len(to_insert)

    if not to_insert:
        logging.info("No new rows to insert into %s (all duplicates).", table_name)
        return result

    # Insert new rows in batches (BigQuery streaming insert)
    batch_size = 500
    for i in range(0, len(to_insert), batch_size):
        batch = to_insert[i : i + batch_size]
        errors = bq.insert_rows_json(table_id, batch)
        if errors:
            logging.error("BigQuery insert errors for table %s: %s", table_name, errors)
            result["errors"].extend(errors)
        else:
            result["inserted"] += len(batch)

    return result

# backwards-compatible small wrappers (optional)
def ensure_table_exists_default():
    """Compat wrapper for older code that used ensure_table_exists() with env BQ_TABLE"""
    default_table = os.getenv("BQ_TABLE")
    if not default_table:
        raise RuntimeError("No BQ_TABLE env var set for default ensure_table_exists_default()")
    return ensure_table_exists(default_table)

def insert_rows_default(rows: List[Dict]):
    default_table = os.getenv("BQ_TABLE")
    if not default_table:
        raise RuntimeError("No BQ_TABLE env var set for default insert_rows_default()")
    return insert_rows_for_table(default_table, rows)
