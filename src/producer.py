# src/producer.py
"""
Producer: diff company_index -> company_details and publish only new/changed rows to Pub/Sub.

Usage (local or Cloud Shell):
  python src/producer.py --limit 50

Environment:
  GOOGLE_CLOUD_PROJECT (optional, will fallback to companies-house-pipeline)
  BQ_DATASET (optional, default companies_house)
  TOPIC (optional, default projects/<PROJECT>/topics/company-details-topic)
"""

import os
import json
import argparse
from google.cloud import bigquery
from google.cloud import pubsub_v1

PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("PROJECT_ID") or "companies-house-pipeline"
DATASET = os.getenv("BQ_DATASET") or "companies_house"
TOPIC = os.getenv("TOPIC") or f"projects/{PROJECT}/topics/company-details-topic"
LOCATION = "asia-south1"

# BigQuery diff SQL — returns only rows missing/changed in company_details
DIFF_SQL = f"""
WITH idx AS (
  SELECT company_number, links_self, row_signature AS index_row_signature, date_indexed
  FROM `{PROJECT}.{DATASET}.company_index`
),
det AS (
  SELECT company_number, index_row_signature AS details_index_sig
  FROM `{PROJECT}.{DATASET}.company_details`
)
SELECT i.company_number, i.links_self, i.index_row_signature, i.date_indexed
FROM idx i
LEFT JOIN det d
  ON i.company_number = d.company_number
WHERE d.details_index_sig IS NULL OR d.details_index_sig != i.index_row_signature
ORDER BY i.date_indexed DESC
"""

def publish_messages(limit=None):
    bq = bigquery.Client(project=PROJECT)
    publisher = pubsub_v1.PublisherClient()
    # Query
    query_job = bq.query(DIFF_SQL, location=LOCATION)
    it = query_job.result()
    count = 0
    for row in it:
        payload = {
            "company_number": row.company_number,
            "links_self": row.links_self,
            "index_row_signature": row.index_row_signature,
            "date_indexed": row.date_indexed.isoformat() if row.date_indexed else None,
        }
        # Publish JSON-encoded message
        data = json.dumps(payload).encode("utf-8")
        future = publisher.publish(TOPIC, data)
        # Optional: you could add attributes: future = publisher.publish(TOPIC, data, company_number=row.company_number)
        future.result()  # wait for publish to complete (keeps producer simple)
        count += 1
        if limit and count >= int(limit):
            break
    return count

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Max messages to publish (for testing)")
    args = parser.parse_args()
    n = publish_messages(limit=args.limit)
    print(f"Published {n} messages to {TOPIC}")
