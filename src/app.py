# src/app.py
#
import os
import time
import logging
from flask import Flask, request, jsonify
from google.cloud import bigquery
import json
# Local imports from src package
try:
    # package-style imports for production
    from src.normalize import normalize_record
    from src.bq_writer import insert_rows_for_table, ensure_table_exists
    from src.ch_requests import paginate_companies_house, fetch_company_detail
    from src.producer import publish_messages
except ModuleNotFoundError:
    # local run from inside src/
    from normalize import normalize_record
    from bq_writer import insert_rows_for_table, ensure_table_exists
    from ch_requests import paginate_companies_house, fetch_company_detail
    from producer import publish_messages
# ch_requests should expose paginate_companies_house and fetch_company_detail
# schema contains project/dataset defaults (optional)
try:
    from src.schema import PROJECT_ID as SCHEMA_PROJECT, DATASET as SCHEMA_DATASET
except Exception:
    
    from schema import PROJECT_ID as SCHEMA_PROJECT, DATASET as SCHEMA_DATASET

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


@app.route("/")
def root():
    return jsonify({"service": "companies-house-pipeline", "status": "ready"})


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/index", methods=["GET", "POST"])
def index():
    """
    Replaces previous /run-once. Paginates Companies House search endpoint,
    normalizes into company_index schema, and inserts into BigQuery.
    Query param:
      q - search query (default "a")
      max_pages - optional int to limit pages (dev)
    """
    if paginate_companies_house is None:
        return jsonify({"status": "error", "message": "ch_requests.paginate_companies_house not available"}), 500

    q = request.args.get("q") or (request.get_json(silent=True) or {}).get("q", "a")
    max_pages = request.args.get("max_pages")
    try:
        max_pages = int(max_pages) if max_pages else None
    except Exception:
        max_pages = None

    inserted_total = 0
    skipped_total = 0
    page_no = 0

    try:
        # ensure table exists before inserting
        ensure_table_exists("company_index")

        for items in paginate_companies_house(query=q, items_per_page=100, sleep_sec=1.0):
            page_no += 1
            # Normalize each item for company_index
            rows = [normalize_record("company_index", it) for it in items]

            # Insert using generic writer (dedupe on row_signature)
            res = insert_rows_for_table("company_index", rows)
            if res.get("errors"):
                logger.error("BigQuery insert errors on page %s: %s", page_no, res["errors"])
                return jsonify({"status": "error", "page": page_no, "errors": res["errors"]}), 500

            inserted = res.get("inserted", 0)
            skipped = res.get("skipped", 0)
            inserted_total += inserted
            skipped_total += skipped

            logger.info("Page %s: inserted=%s skipped=%s", page_no, inserted, skipped)

            # optional page limit for dev/testing
            if max_pages and page_no >= max_pages:
                logger.info("Reached max_pages=%s, stopping.", max_pages)
                break

        return jsonify({"status": "ok", "inserted": inserted_total, "skipped": skipped_total}), 200

    except Exception as exc:
        logger.exception("Failed to run index pipeline: %s", exc)
        return jsonify({"status": "error", "message": str(exc)}), 500


@app.route("/details", methods=["POST", "GET"])
def details():
    """
    Fetch company detail records for all active companies in company_index.
    This endpoint:
      - queries BQ for company_index rows with company_status='active' and non-null links_self
      - for each record, calls the detail endpoint and normalizes/inserts into company_details
    Optional JSON body or query params:
      limit - integer limit for number of companies to process (for testing)
      sleep_sec - float seconds to wait between requests (politeness / rate limiting)
    """
    if fetch_company_detail is None:
        return jsonify({"status": "error", "message": "ch_requests.fetch_company_detail not available"}), 500

    # read params
    params = request.get_json(silent=True) or {}
    limit = request.args.get("limit") or params.get("limit")
    sleep_sec = request.args.get("sleep_sec") or params.get("sleep_sec") or 0.5
    try:
        limit = int(limit) if limit is not None else None
    except Exception:
        limit = None
    try:
        sleep_sec = float(sleep_sec)
    except Exception:
        sleep_sec = 0.5

    # prepare BigQuery client to fetch active companies
    project = SCHEMA_PROJECT or os.getenv("PROJECT_ID")
    dataset = SCHEMA_DATASET or os.getenv("BQ_DATASET") or "companies_house"
    client = bigquery.Client(project=project)

    query = f"""
    SELECT company_number, links_self, row_signature
    FROM `{project}.{dataset}.company_index`
    WHERE company_status = 'active' AND links_self IS NOT NULL
    ORDER BY date_indexed DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    try:
        # ensure details table exists
        ensure_table_exists("company_details")

        logger.info("Querying active companies to process (limit=%s)...", limit)
        job = client.query(query)
        rows_iter = job.result()

        processed = 0
        inserted_total = 0
        skipped_total = 0
        errors = []

        for r in rows_iter:
            company_number = r.get("company_number")
            links_self = r.get("links_self")
            try:
                # Use company_number to fetch detail (fetch_company_detail uses /company/{number})
                detail_json = fetch_company_detail(company_number)
                if not detail_json:
                    logger.info("No detail found for %s (skipping).", company_number)
                    continue

                # normalize for company_details and insert
                detail_row = normalize_record("company_details", detail_json, extra_fields={"index_row_signature": r.get("row_signature")})
                res = insert_rows_for_table("company_details", [detail_row])
                if res.get("errors"):
                    logger.error("Insert errors for %s: %s", company_number, res["errors"])
                    errors.append({"company_number": company_number, "errors": res["errors"]})
                else:
                    inserted_total += res.get("inserted", 0)
                    skipped_total += res.get("skipped", 0)

                processed += 1
                # polite pause between API calls
                time.sleep(sleep_sec)
                logger.info("processed %s: inserted=%s skipped=%s inserted_total=%s skipped_total=%s", processed,  res.get("inserted", 0), res.get("skipped", 0),inserted_total, skipped_total)

            except Exception as e:
                logger.exception("Failed processing company %s: %s", company_number, e)
                errors.append({"company_number": company_number, "exception": str(e)})
                # don't abort entire run; continue with next company
                continue

        result = {
            "status": "ok" if not errors else "partial",
            "processed": processed,
            "inserted": inserted_total,
            "skipped": skipped_total,
            "errors_count": len(errors),
            "errors": errors[:10],  # sample first 10 errors for brevity
        }
        logger.info("processed %s: inserted=%s skipped=%s", processed, inserted, skipped)
        return jsonify(result), (200 if not errors else 207)

    except Exception as exc:
        logger.exception("Failed to run details pipeline: %s", exc)
        return jsonify({"status": "error", "message": str(exc)}), 500

# ---------- /producer endpoint: trigger the diff -> publish to Pub/Sub ----------
@app.route("/producer", methods=["POST", "GET"])
def producer_endpoint():
    """
    Trigger the producer which diffs company_index -> company_details and publishes
    messages to Pub/Sub. Accepts:
      - limit (query param or JSON body) to limit messages published (for testing)
    Returns JSON: {"status":"ok","published":N}
    """
    if publish_messages is None:
        return jsonify({"status": "error", "message": "producer.publish_messages not available"}), 500

    # read limit from query or JSON body
    params = request.get_json(silent=True) or {}
    limit = request.args.get("limit") or params.get("limit")
    try:
        limit = int(limit) if limit is not None else None
    except Exception:
        limit = None

    try:
        published = publish_messages(limit=limit)
        return jsonify({"status": "ok", "published": published}), 200
    except Exception as exc:
        logger.exception("Producer failed: %s", exc)
        return jsonify({"status": "error", "message": str(exc)}), 500

# ---------- /subscriber endpoint: Pub/Sub push target ----------
@app.route("/subscriber", methods=["POST", "GET"])
def subscriber_endpoint():
    """
    Pub/Sub push target. Expects Pub/Sub push envelope JSON:
      {"message": {"data": "<base64>", "attributes": {...}}, "subscription": "..."}
    This handler decodes message.data -> JSON payload with:
      company_number, links_self, index_row_signature
    Then it:
      - does a quick BigQuery check if company_details already has the same index signature
      - if up-to-date: returns 200 (ACK)
      - otherwise: fetch detail, normalize with index_row_signature, and insert_rows_for_table
    """
    try:
        envelope = request.get_json(silent=True)
        if not envelope:
            logger.error("No JSON payload in subscriber")
            return ("Bad Request: no JSON", 400)

        msg = envelope.get("message")
        if not msg:
            logger.error("No message in envelope")
            return ("Bad Request: no message", 400)

        data_b64 = msg.get("data")
        try:
            payload = json.loads(base64.b64decode(data_b64).decode("utf-8")) if data_b64 else {}
        except Exception as e:
            logger.exception("Failed to decode Pub/Sub message data: %s", e)
            return ("Bad Request: invalid base64/data", 400)

        company_number = payload.get("company_number")
        links_self = payload.get("links_self")
        index_sig = payload.get("index_row_signature")

        logger.info("subscriber: message for company_number=%s index_sig=%s", company_number, index_sig)

        # Defensive quick-check: does company_details already have same index_row_signature?
        project = SCHEMA_PROJECT or os.getenv("PROJECT_ID")
        dataset = SCHEMA_DATASET or os.getenv("BQ_DATASET") or "companies_house"
        details_table = f"{project}.{dataset}.company_details"
        bq_client = bigquery.Client(project=project)

        if company_number:
            q = f"SELECT index_row_signature FROM `{details_table}` WHERE company_number = @company_number LIMIT 1"
            job_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("company_number", "STRING", company_number)]
            )
            job = bq_client.query(q, job_config=job_config, location=os.getenv("BQ_LOCATION", "asia-south1"))
            rows = list(job.result())
            if rows:
                existing_sig = rows[0].get("index_row_signature")
                if existing_sig == index_sig:
                    logger.info("subscriber: %s is already up-to-date (sig matches). ACKing.", company_number)
                    return ("", 200)

        # Not up-to-date -> fetch detail and insert
        detail_json = fetch_company_detail(company_number if company_number else links_self)
        if not detail_json:
            logger.info("subscriber: no detail JSON for %s (ACKing).", company_number)
            return ("", 200)

        # Normalize and attach index signature
        normalized = normalize_record("company_details", detail_json, extra_fields={"index_row_signature": index_sig})

        # Insert via generic BQ writer
        res = insert_rows_for_table("company_details", [normalized])
        if res.get("errors"):
            logger.error("subscriber: BQ insert errors for %s: %s", company_number, res["errors"])
            return (jsonify({"status": "error", "errors": res["errors"]}), 500)

        logger.info("subscriber: processed %s -> inserted=%s skipped=%s", company_number, res.get("inserted"), res.get("skipped"))
        return ("", 200)

    except Exception as exc:
        logger.exception("subscriber: unhandled error %s", exc)
        return (jsonify({"status": "error", "message": str(exc)}), 500)

if __name__ == "__main__":
    # helpful debug info printed to console so you can verify binding
    import sys
    host = "0.0.0.0"
    port = int(os.getenv("PORT", "8080"))
    print(f"DEBUG: starting app on http://{host}:{port}/ (process pid={os.getpid()})")
    # run without reloader to avoid double-spawn on Windows
    app.run(host=host, port=port, debug=False, use_reloader=False)
