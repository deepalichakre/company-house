# ch_requests.py
import os
import requests
from google.cloud import secretmanager
from requests.auth import HTTPBasicAuth

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


def call_companies_house(query: str = "a", items_per_page: int = 5, start_index: int = 0) -> dict:
    """Call Companies House /search/companies endpoint and return parsed JSON."""
    api_key = get_secret()
    auth = HTTPBasicAuth(api_key, "")
    url = f"{CH_API_BASE}/search/companies"
    params = {"q": query, "items_per_page": items_per_page, "start_index": start_index}
    resp = requests.get(url, params=params, auth=auth, timeout=20)
    resp.raise_for_status()
    return resp.json()


# For standalone test (local run)
if __name__ == "__main__":
    data = call_companies_house("a", 3)
    print(f"Received {len(data.get('items', []))} items")
