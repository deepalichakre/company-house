# src/ch_requests.py
import time
import logging
import requests
from requests.auth import HTTPBasicAuth
from google.cloud import secretmanager
import os

# optional: move to config later
SECRET_NAME = os.getenv("CH_SECRET_NAME", "companies-house-api-key")
PROJECT_ID = os.getenv("PROJECT_ID", "companies-house-pipeline")
CH_API_BASE = "https://api.company-information.service.gov.uk"


def get_secret(secret_name: str = SECRET_NAME, project_id: str = PROJECT_ID) -> str:
    """Fetch Companies House API key from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def call_companies_house(query="a", items_per_page=100, start_index=0):
    """Return one page of results (kept for backward-compat)."""
    api_key = get_secret()
    auth = HTTPBasicAuth(api_key, "")
    url = f"{CH_API_BASE}/search/companies"
    params = {"q": query, "items_per_page": items_per_page, "start_index": start_index}
    resp = requests.get(url, params=params, auth=auth, timeout=30)
    resp.raise_for_status()
    return resp.json()


def paginate_companies_house(query="a",
                             items_per_page=100,
                             sleep_sec=1.0,
                             max_retries=5):
    """
    Generator that yields every page of /search/companies until no data remains.
    Stops when:
      • an empty page is returned, or
      • HTTP 416 is raised, or
      • fewer than `items_per_page` results come back.
    Includes retry + back-off for transient errors and 429 rate limits.
    """
    api_key = get_secret()
    auth = HTTPBasicAuth(api_key, "")
    url = f"{CH_API_BASE}/search/companies"
    start_index = 0
    consecutive_errors = 0
    page = 0

    while True:
        try:
            params = {"q": query, "items_per_page": items_per_page, "start_index": start_index}
            resp = requests.get(url, params=params, auth=auth, timeout=30)

            if resp.status_code == 416:
                logging.info("416 at start_index=%s — end of results.", start_index)
                break

            if resp.status_code == 429:
                wait = min(60, (2 ** consecutive_errors) * sleep_sec)
                logging.warning("Rate-limited (429). Sleeping %.1fs ...", wait)
                time.sleep(wait)
                consecutive_errors += 1
                continue

            resp.raise_for_status()
            data = resp.json()
            consecutive_errors = 0

        except requests.RequestException:
            consecutive_errors += 1
            if consecutive_errors >= max_retries:
                logging.error("Too many consecutive HTTP errors; aborting pagination.")
                break
            backoff = min(60, (2 ** consecutive_errors) * sleep_sec)
            logging.warning("HTTP error, retrying after %.1fs (attempt %s/%s)...",
                            backoff, consecutive_errors, max_retries)
            time.sleep(backoff)
            continue

        items = data.get("items", [])
        if not items:
            logging.info("Empty page at start_index=%s — stopping.", start_index)
            break

        page += 1
        yield items
        logging.info("Yielded page %s (%s items)", page, len(items))

        # prepare next request
        start_index += len(items)

        # stop if we got a short page (< items_per_page)
        if len(items) < items_per_page:
            logging.info("Last partial page (%s < %s). Done.", len(items), items_per_page)
            break

        # polite delay
        time.sleep(sleep_sec)

# For standalone test (local run)
if __name__ == "__main__":
    data = call_companies_house("a", 3)
    print(f"Received {len(data.get('items', []))} items")
