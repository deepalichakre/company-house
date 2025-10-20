# src/app.py
import os
import logging
from flask import Flask, request, jsonify
from src.normalize import normalize_company_summary
from src.bq_writer import insert_rows
# try import; if it fails, log the error so we know why
try:
    # ch_requests.py lives in the same folder (src)
    from src.ch_requests import paginate_companies_house
    
except Exception as ex:
    paginate_companies_house = None
    logging.exception("Unable to import ch_requests.paginate_companies_house: %s", ex)

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)


@app.route("/")
def root():
    return jsonify({"service": "companies-house-pipeline", "status": "ready"})


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/run-once", methods=["GET", "POST"])
def run_once():
    if paginate_companies_house is None:
        return jsonify({"status": "error", "message": "ch_requests not available"}), 500

    q = request.args.get("q") or (request.get_json(silent=True) or {}).get("q", "a")

    try:
        inserted_total = 0
        page_no = 0
        for items in paginate_companies_house(query=q, items_per_page=100, sleep_sec=1.0):
            page_no += 1
            rows = [normalize_company_summary(it) for it in items]
            ins_result = insert_rows(rows)
            if ins_result.get("errors"):
                # if any insertion errors, return error
                return jsonify({"status": "error", "errors": ins_result["errors"]}), 500

            inserted = ins_result.get("inserted", 0)
            skipped = ins_result.get("skipped", 0)
            logging.info("Inserted %s rows, skipped %s duplicates on this page", inserted, skipped)

            if ins_result.get("errors"):
                logging.error("BQ insert errors page %s: %s", page_no, errors)
                return jsonify({"status": "error", "page": page_no, "errors": errors}), 500
            inserted_total += len(rows)
            logging.info("Inserted page %s (%s rows)", page_no, len(rows))

        return jsonify({"status": "ok", "inserted": inserted_total}), 200

    except Exception as e:
        logging.exception("Failed to fetch companies house data")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    # helpful debug info printed to console so you can verify binding
    host = "0.0.0.0"
    port = int(os.getenv("PORT", "8080"))
    print(f"DEBUG: starting app on http://{host}:{port}/ (process pid={os.getpid()})")
    # run without reloader to avoid double-spawn on Windows
    app.run(host=host, port=port, debug=False, use_reloader=False)
