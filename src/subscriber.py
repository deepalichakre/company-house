# src/subscriber.py
"""
Cloud Run Pub/Sub push subscriber.

Expected to be deployed as a public-ish Cloud Run service, with a push subscription configured
to use a service account that has run.invoker on this service.

Behavior:
- Receives Pub/Sub push payload.
- Parses JSON message (company_number, links_self, index_row_signature).
- Quick defensive check: does company_details already have the same index_row_signature for this company_number?
  - If yes: return 200 (ACK) immediately.
  - If no: call Companies House detail endpoint, normalize the result (passing index_row_signature),
    and insert (upsert-ish) using insert_rows_for_table.
- Returns 200 on success, 500 on transient failure (Pub/Sub will retry). Keep processing short.
"""

import base64
import json
import logging
import os
import time
from flask import Flask, request, jsonify

# resilient imports to support running inside src package or directly
try:
    from src.ch_requests import fetch_company_detail
    from src.normalize import normalize_record
    from src.bq_writer import insert_rows_for_table
    from src.schema import PROJECT_ID as SCHEMA_PROJECT, DATASET as SCHEMA_DATASET
except Exception:
    from ch_requests import fetch_company_detail
    from normalize import normalize_record
    from bq_writer import insert_rows_for_table
    from schema import PROJECT_ID as SCHEMA_PROJECT, DATASET as SCHEMA_DATASET

from google.cloud import bigquery

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("subscriber")

# config
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID") or SCHEMA_PROJECT or "companies-house-pipeline"
DATASET = os.getenv("BQ_DATASET") or SCHEMA_DATASET or "companies_house"
BQ_LOCATION = os.getenv("BQ_LOCATION") or "asia-south1"
DETAILS_TABLE = f"{PROJECT}.{DATASET}.company_details"

bq = bigquery.Client(project=PROJECT)


def is_up_to_date(company_number: str, index_sig: str) -> bool:
    """Return True if company_details already has index_row_signature == index_sig for company_number."""
    if not company_number:
        return False
    q = f"""
    SELECT index_row_signature FROM `{DETAILS_TABLE}`
    WHERE company_number = @company_number
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("company_number", "STRING", company_number)]
    )
    job = bq.query(q, job_config=job_config, location=BQ_LOCATION)
    rows = list(job.result())
    if not rows:
        return False
    existing = rows[0].get("index_row_signature")
    return existing == index_sig


@app.route("/", methods=["POST"])
def receive_pubsub_push():
    """
    Handler for Pub/Sub push subscription POST.
    Pub/Sub push has JSON envelope: {"message": {"data": "<base64>", "attributes": {...}}, "subscription": "..."}
    """
    envelope = request.get_json(silent=True)
    if not envelope:
        logger.error("No JSON payload")
        return ("Bad Request: no JSON", 400)

    msg = envelope.get("message")
    if not msg:
        logger.error("No message in envelope")
        return ("Bad Request: no message", 400)

    data_b64 = msg.get("data")
    try:
        data_json = json.loads(base64.b64decode(data_b64).decode("utf-8")) if data_b64 else {}
    except Exception as e:
        logger.exception("Failed to decode Pub/Sub message data: %s", e)
        return ("Bad Request: invalid base64/data", 400)

    company_number = data_json.get("company_number")
    links_self = data_json.get("links_self")
    index_sig = data_json.get("index_row_signature")

    logger.info("Received message for company_number=%s index_sig=%s", company_number, index_sig)

    try:
        # Defensive quick-check: if details already up-to-date, ACK immediately
        if is_up_to_date(company_number, index_sig):
            logger.info("Already up-to-date for %s (sig matches). ACKing.", company_number)
            return ("", 200)

        # Not up-to-date -> fetch detail and insert
        # We prefer to fetch by company_number (fetch_company_detail handles both)
        detail_json = fetch_company_detail(company_number)
        if not detail_json:
            logger.info("No detail JSON for %s (maybe 404). ACKing.", company_number)
            return ("", 200)

        # Normalize and attach index signature
        normalized = normalize_record("company_details", detail_json, extra_fields={"index_row_signature": index_sig})

        # Insert (dedupe logic in insert_rows_for_table will skip duplicates)
        res = insert_rows_for_table("company_details", [normalized])
        if res.get("errors"):
            logger.error("BQ insert errors for %s: %s", company_number, res["errors"])
            # Return 500 so Pub/Sub can retry delivery (or route to DLQ)
            return (jsonify({"status": "error", "errors": res["errors"]}), 500)

        logger.info("Inserted/updated %s -> inserted=%s skipped=%s", company_number, res.get("inserted"), res.get("skipped"))
        return ("", 200)

    except Exception as e:
        logger.exception("Unhandled error processing %s: %s", company_number, e)
        # transient error -> ask Pub/Sub to retry by returning non-2xx
        return (jsonify({"status": "error", "message": str(e)}), 500)


if __name__ == "__main__":
    host = "0.0.0.0"
    port = int(os.getenv("PORT", "8080"))
    print(f"Starting subscriber on http://{host}:{port}")
    app.run(host=host, port=port, debug=False, use_reloader=False)
