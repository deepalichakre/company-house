# src/app.py
import os
import logging
from flask import Flask, request, jsonify

# try import; if it fails, log the error so we know why
try:
    # ch_requests.py lives in the same folder (src)
    from ch_requests import call_companies_house
except Exception as ex:
    call_companies_house = None
    logging.exception("Unable to import ch_requests.call_companies_house: %s", ex)

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
    if call_companies_house is None:
        return jsonify({"status": "error", "message": "ch_requests not available"}), 500

    q = request.args.get("q")
    if not q:
        data = request.get_json(silent=True) or {}
        q = data.get("q", "tesco")

    try:
        resp_json = call_companies_house(query=q, items_per_page=25, start_index=0)
        items = resp_json.get("items", [])
        return jsonify({"status": "ok", "items_returned": len(items)}), 200
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
