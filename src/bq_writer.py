# src/bq_writer.py
import os
import logging
from google.cloud import bigquery
import math

PROJECT_ID = os.getenv("PROJECT_ID", os.getenv("GCP_PROJECT", "companies-house-pipeline"))
BQ_DATASET = os.getenv("BQ_DATASET", "companies_house")
BQ_TABLE = os.getenv("BQ_TABLE", "company_index")

bq = bigquery.Client(project=PROJECT_ID)

def ensure_table_exists():
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        bq.get_table(table_id)
        return table_id
    except Exception:
        # create dataset if missing
        dataset_ref = bigquery.DatasetReference(PROJECT_ID, BQ_DATASET)
        try:
            bq.get_dataset(dataset_ref)
        except Exception:
            ds = bigquery.Dataset(dataset_ref)
            ds.location = "asia-south1"  # match your dataset location
            bq.create_dataset(ds, exists_ok=True)
        # create table
        schema = [
            bigquery.SchemaField("company_number", "STRING"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("kind", "STRING"),
            bigquery.SchemaField("company_status", "STRING"),
            bigquery.SchemaField("company_type", "STRING"),
            bigquery.SchemaField("snippet", "STRING"),
            bigquery.SchemaField("address_snippet", "STRING"),
            bigquery.SchemaField("address_line_1", "STRING"),
            bigquery.SchemaField("address_locality", "STRING"),
            bigquery.SchemaField("address_country", "STRING"),
            bigquery.SchemaField("address_postal_code", "STRING"),
            bigquery.SchemaField("links_self", "STRING"),
            bigquery.SchemaField("date_of_creation", "DATE"),
            bigquery.SchemaField("date_of_cessation", "DATE"),
            bigquery.SchemaField("rank", "INT64"),
            bigquery.SchemaField("date_indexed", "TIMESTAMP"),
            bigquery.SchemaField("raw_json", "JSON"),
        ]
        table = bigquery.Table(table_id, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="date_indexed")
        bq.create_table(table)
        return table_id

def fetch_existing_signatures(table_id, signatures):
    """
    Query BQ for any row_signature values that already exist in the table.
    Returns a set of signature strings.
    """
    if not signatures:
        return set()
    # Batch signatures to avoid too large queries
    existing = set()
    client = bigquery.Client()
    chunk_size = 500  # safe chunk size for IN (...)
    for i in range(0, len(signatures), chunk_size):
        chunk = signatures[i : i + chunk_size]
        # build a parameterized query for safety
        placeholders = ", ".join(f"'{s}'" for s in chunk)
        q = f"""
        SELECT row_signature FROM `{table_id}`
        WHERE row_signature IN ({placeholders})
        """
        job = client.query(q)
        for row in job:
            existing.add(row["row_signature"])
    return existing

def insert_rows(rows):
    """
    rows: list[dict] where 'row_signature' key must be present.
    We'll skip rows whose signature already exists in BigQuery.
    Returns a dict: {"inserted": n, "skipped": m, "errors": [...]}
    """
    table_id = ensure_table_exists()
    # extract signatures from rows
    signatures = [r.get("row_signature") for r in rows if r.get("row_signature")]
    unique_signatures = list(dict.fromkeys(signatures))  # preserve order, remove duplicates
    existing = fetch_existing_signatures(table_id, unique_signatures)

    # Filter rows to insert
    to_insert = [r for r in rows if r.get("row_signature") not in existing]

    result = {"inserted": 0, "skipped": 0, "errors": []}
    result["skipped"] = len(rows) - len(to_insert)

    if not to_insert:
        logging.info("No new rows to insert (all duplicates).")
        return result

    # Insert new rows in batches (avoid too-large single insert)
    batch_size = 500
    for i in range(0, len(to_insert), batch_size):
        batch = to_insert[i : i + batch_size]
        errors = bq.insert_rows_json(table_id, batch)
        if errors:
            logging.error("BigQuery insert errors: %s", errors)
            result["errors"].extend(errors)
        else:
            result["inserted"] += len(batch)

    return result