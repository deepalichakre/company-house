# src/insurance_mock.py
"""
Generate mock health insurance data and load into BigQuery.

Usage:
  from src.insurance_mock import generate_and_load
  generate_and_load(project="my-project", dataset="insurance", location="EU", num_policies=1000)

This module:
 - creates/ensures BQ tables (dimensions + facts)
 - generates synthetic rows with UUID PKs and referential integrity
 - inserts rows into BigQuery using insert_rows_json
"""

import uuid
import random
import time
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Tuple

from faker import Faker
import numpy as np
import pandas as pd
from google.cloud import bigquery

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)


# ---------------------------
# Table schemas (BigQuery)
# ---------------------------
def get_table_schemas() -> Dict[str, List[bigquery.SchemaField]]:
    S = bigquery.SchemaField
    return {
        "dim_policyholder": [
            S("policyholder_id", "STRING", mode="REQUIRED"),
            S("first_name", "STRING"),
            S("last_name", "STRING"),
            S("dob", "DATE"),
            S("gender", "STRING"),
            S("email", "STRING"),
            S("phone", "STRING"),
            S("postcode", "STRING"),
            S("region_id", "STRING"),
            S("smoker", "BOOLEAN"),
            S("created_at", "TIMESTAMP"),
        ],
        "dim_policy": [
            S("policy_id", "STRING", mode="REQUIRED"),
            S("policy_number", "STRING"),
            S("policy_type", "STRING"),
            S("plan_id", "STRING"),
            S("start_date", "DATE"),
            S("end_date", "DATE"),
            S("status", "STRING"),
            S("sum_insured", "NUMERIC"),
            S("deductible", "NUMERIC"),
            S("co_pay_percent", "NUMERIC"),
            S("policyholder_id", "STRING"),
            S("premium_frequency", "STRING"),
            S("created_at", "TIMESTAMP"),
        ],
        "dim_plan": [
            S("plan_id", "STRING", mode="REQUIRED"),
            S("plan_name", "STRING"),
            S("inpatient_limit", "NUMERIC"),
            S("outpatient_limit", "NUMERIC"),
            S("maternity_limit", "NUMERIC"),
            S("waiting_period_days", "INTEGER"),
        ],
        "dim_provider": [
            S("provider_id", "STRING", mode="REQUIRED"),
            S("provider_name", "STRING"),
            S("provider_type", "STRING"),
            S("postcode", "STRING"),
            S("region_id", "STRING"),
            S("contracted", "BOOLEAN"),
            S("rating", "NUMERIC"),
        ],
        "dim_diagnosis_procedure": [
            S("code_id", "STRING", mode="REQUIRED"),
            S("code_type", "STRING"),
            S("code", "STRING"),
            S("short_description", "STRING"),
        ],
        "dim_region": [
            S("region_id", "STRING", mode="REQUIRED"),
            S("region_name", "STRING"),
            S("country", "STRING"),
        ],
        "fact_claim": [
            S("claim_id", "STRING", mode="REQUIRED"),
            S("policy_id", "STRING"),
            S("policyholder_id", "STRING"),
            S("provider_id", "STRING"),
            S("claim_date", "DATE"),
            S("admission_date", "DATE"),
            S("discharge_date", "DATE"),
            S("claim_type", "STRING"),
            S("diagnosis_code", "STRING"),
            S("procedure_code", "STRING"),
            S("billed_amount", "NUMERIC"),
            S("allowed_amount", "NUMERIC"),
            S("deductible_applied", "NUMERIC"),
            S("copay_amount", "NUMERIC"),
            S("paid_amount", "NUMERIC"),
            S("claim_status", "STRING"),
            S("submission_date", "DATE"),
            S("settlement_date", "DATE"),
        ],
        "fact_premium_payment": [
            S("payment_id", "STRING", mode="REQUIRED"),
            S("policy_id", "STRING"),
            S("policyholder_id", "STRING"),
            S("payment_date", "DATE"),
            S("period_start", "DATE"),
            S("period_end", "DATE"),
            S("amount_due", "NUMERIC"),
            S("amount_paid", "NUMERIC"),
            S("payment_method", "STRING"),
            S("payment_status", "STRING"),
        ],
        "fact_enrollment_event": [
            S("enrollment_event_id", "STRING", mode="REQUIRED"),
            S("policy_id", "STRING"),
            S("policyholder_id", "STRING"),
            S("event_type", "STRING"),
            S("event_date", "DATE"),
            S("reason", "STRING"),
        ],
    }


# ---------------------------
# Helpers: table creation & insertion
# ---------------------------
def ensure_table(client: bigquery.Client, project: str, dataset: str, table_name: str, schema: List[bigquery.SchemaField], location: str = None):
    table_id = f"{project}.{dataset}.{table_name}"
    try:
        tbl = bigquery.Table(table_id, schema=schema)
        if location:
            tbl.location = location
        client.get_table(table_id)
        # exists -> nothing
    except Exception:
        # create dataset if missing
        ds_id = f"{project}.{dataset}"
        try:
            client.get_dataset(ds_id)
        except Exception:
            ds = bigquery.Dataset(ds_id)
            if location:
                ds.location = location
            client.create_dataset(ds, exists_ok=True)
        client.create_table(tbl, exists_ok=True)


def insert_json_rows(client: bigquery.Client, project: str, dataset: str, table_name: str, rows: List[Dict[str, Any]]) -> Tuple[int, List]:
    """Insert rows via insert_rows_json. Returns (inserted_count, errors_list)"""
    if not rows:
        return 0, []
    table_id = f"{project}.{dataset}.{table_name}"
    errors = client.insert_rows_json(table_id, rows)
    inserted = 0 if errors else len(rows)
    return inserted, errors


# ---------------------------
# Data generation
# ---------------------------
def uuid_str() -> str:
    return str(uuid.uuid4())


def random_date_between(start_date: date, end_date: date) -> date:
    delta = (end_date - start_date).days
    if delta <= 0:
        return start_date
    return start_date + timedelta(days=random.randint(0, delta))


def generate_regions(n=6) -> List[Dict[str, Any]]:
    regions = []
    sample = ["LON-CEN", "LON-W", "LON-E", "LON-N", "LON-S", "LON-NE"]
    for i in range(n):
        rid = sample[i % len(sample)]
        regions.append({
            "region_id": rid,
            "region_name": rid,
            "country": "UK",
        })
    return regions


def generate_plans() -> List[Dict[str, Any]]:
    plans = []
    templates = [
        ("Basic Health", 5000, 2000, 0, 90),
        ("Standard Health", 20000, 5000, 2000, 60),
        ("Premier Health", 100000, 25000, 5000, 30),
    ]
    for name, inp, outp, mat, wait in templates:
        plans.append({
            "plan_id": uuid_str(),
            "plan_name": name,
            "inpatient_limit": inp,
            "outpatient_limit": outp,
            "maternity_limit": mat,
            "waiting_period_days": wait,
        })
    return plans


def generate_providers(n=200, regions=None) -> List[Dict[str, Any]]:
    regions = regions or []
    providers = []
    types = ["Hospital", "Clinic", "GP", "Diagnostic"]
    for i in range(n):
        pid = uuid_str()
        providers.append({
            "provider_id": pid,
            "provider_name": fake.company() + " Medical",
            "provider_type": random.choice(types),
            "postcode": fake.postcode(),
            "region_id": random.choice(regions)["region_id"] if regions else None,
            "contracted": random.random() < 0.7,
            "rating": round(random.uniform(2.5, 5.0), 2),
        })
    return providers


def generate_diag_proc(n=300) -> List[Dict[str, Any]]:
    codes = []
    for i in range(n):
        code = f"I{random.randint(10,99)}.{random.randint(0,9)}"
        codes.append({
            "code_id": uuid_str(),
            "code_type": "Diagnosis" if random.random() < 0.7 else "Procedure",
            "code": code,
            "short_description": fake.sentence(nb_words=4),
        })
    return codes


def generate_policyholders(n: int, regions: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    rows = []
    for _ in range(n):
        phid = uuid_str()
        dob = fake.date_of_birth(minimum_age=18, maximum_age=85)
        rows.append({
            "policyholder_id": phid,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "dob": dob.isoformat(),
            "gender": random.choice(["Male", "Female", "Other"]),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "postcode": fake.postcode(),
            "region_id": random.choice(regions)["region_id"],
            "smoker": random.random() < 0.12,
            "created_at": datetime.utcnow().isoformat(),
        })
    return rows


def generate_policies(policyholders: List[Dict[str,Any]], plans: List[Dict[str,Any]], num_policies: int) -> List[Dict[str,Any]]:
    policies = []
    types = ["Individual", "Family", "Corporate"]
    for i in range(num_policies):
        pid = uuid_str()
        holder = random.choice(policyholders)
        plan = random.choice(plans)
        start = date.today() - timedelta(days=random.randint(0, 365*3))
        length_days = 365
        end = start + timedelta(days=length_days)
        policies.append({
            "policy_id": pid,
            "policy_number": f"POL{random.randint(1000000,9999999)}",
            "policy_type": random.choice(types),
            "plan_id": plan["plan_id"],
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "status": "Active" if end >= date.today() else "Expired",
            "sum_insured": plan["inpatient_limit"],
            "deductible": round(random.choice([0,100,250,500]),2),
            "co_pay_percent": round(random.choice([0,5,10,20]),2),
            "policyholder_id": holder["policyholder_id"],
            "premium_frequency": random.choice(["Monthly", "Yearly"]),
            "created_at": datetime.utcnow().isoformat(),
        })
    return policies


def generate_claims(policies: List[Dict[str,Any]], policyholders: List[Dict[str,Any]], providers: List[Dict[str,Any]], diagproc: List[Dict[str,Any]], avg_claims_per_policy=0.5) -> List[Dict[str,Any]]:
    claims = []
    for pol in policies:
        # Poisson number of claims
        n_claims = np.random.poisson(avg_claims_per_policy)
        for _ in range(n_claims):
            claim_id = uuid_str()
            ph = next((p for p in policyholders if p["policyholder_id"]==pol["policyholder_id"]), random.choice(policyholders))
            provider = random.choice(providers)
            diag = random.choice(diagproc)
            proc = random.choice(diagproc)
            claim_date = random_date_between(date.fromisoformat(pol["start_date"]), min(date.fromisoformat(pol["end_date"]), date.today()))
            admission = claim_date if random.random() < 0.2 else None
            discharge = admission + timedelta(days=random.randint(1,7)) if admission else None
            billed = round(abs(np.random.normal(loc=1500, scale=2000)) + 50, 2)
            allowed = round(billed * (0.6 + random.random()*0.35), 2)
            deductible = pol["deductible"] if random.random() < 0.15 else 0.0
            copay = round((pol["co_pay_percent"]/100.0) * allowed, 2)
            paid = max(0.0, round(allowed - deductible - copay, 2))
            submission = claim_date + timedelta(days=random.randint(0,7))
            settlement = submission + timedelta(days=random.randint(5,45))
            claims.append({
                "claim_id": claim_id,
                "policy_id": pol["policy_id"],
                "policyholder_id": ph["policyholder_id"],
                "provider_id": provider["provider_id"],
                "claim_date": claim_date.isoformat(),
                "admission_date": admission.isoformat() if admission else None,
                "discharge_date": discharge.isoformat() if discharge else None,
                "claim_type": "Inpatient" if admission else random.choice(["Outpatient", "Pharmacy", "Diagnostic"]),
                "diagnosis_code": diag["code_id"],
                "procedure_code": proc["code_id"],
                "billed_amount": billed,
                "allowed_amount": allowed,
                "deductible_applied": deductible,
                "copay_amount": copay,
                "paid_amount": paid,
                "claim_status": random.choice(["Paid", "Denied", "Pending"]),
                "submission_date": submission.isoformat(),
                "settlement_date": settlement.isoformat(),
            })
    return claims


def generate_premium_payments(policies: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    payments = []
    for pol in policies:
        start = date.fromisoformat(pol["start_date"])
        end = min(date.fromisoformat(pol["end_date"]), date.today())
        freq = pol.get("premium_frequency", "Monthly")
        interval_days = 30 if freq == "Monthly" else 365
        dt = start
        while dt <= end:
            pid = uuid_str()
            amount_due = round(pol["sum_insured"] * 0.0015 if freq=="Monthly" else pol["sum_insured"]*0.018, 2)
            paid = amount_due if random.random() < 0.95 else round(amount_due * random.uniform(0.0, 1.0), 2)
            payments.append({
                "payment_id": pid,
                "policy_id": pol["policy_id"],
                "policyholder_id": pol["policyholder_id"],
                "payment_date": dt.isoformat(),
                "period_start": dt.isoformat(),
                "period_end": (dt + timedelta(days=interval_days-1)).isoformat(),
                "amount_due": amount_due,
                "amount_paid": paid,
                "payment_method": random.choice(["Card", "DirectDebit", "BankTransfer"]),
                "payment_status": "Paid" if paid >= amount_due else "Failed",
            })
            dt = dt + timedelta(days=interval_days)
    return payments


def generate_enrollment_events(policies: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    events = []
    for pol in policies:
        eid = uuid_str()
        events.append({
            "enrollment_event_id": eid,
            "policy_id": pol["policy_id"],
            "policyholder_id": pol["policyholder_id"],
            "event_type": "NewPolicy",
            "event_date": pol["start_date"],
            "reason": None,
        })
        # possible renewal event if active more than a year
    return events


# ---------------------------
# Public function
# ---------------------------
def generate_and_load(project: str,
                      dataset: str =  "health_insurance",
                      location: str = "asia-south1",
                      num_policyholders: int = 1000,
                      num_policies: int = 1000,
                      ensure_tables: bool = True):
    """
    Generate mock data and load into BigQuery tables.
    """
    client = bigquery.Client(project=project)
    schemas = get_table_schemas()

    # ensure tables
    if ensure_tables:
        for tname, schema in schemas.items():
            ensure_table(client, project, dataset, tname, schema, location=location)

    # generate master dims
    regions = generate_regions()
    plans = generate_plans()
    providers = generate_providers(n=300, regions=regions)
    diagproc = generate_diag_proc(n=400)
    policyholders = generate_policyholders(num_policyholders, regions)
    policies = generate_policies(policyholders, plans, num_policies)

    # generate facts
    claims = generate_claims(policies, policyholders, providers, diagproc, avg_claims_per_policy=0.6)
    payments = generate_premium_payments(policies)
    enrollments = generate_enrollment_events(policies)

    # Insert ordering: dims first, facts later
    insert_order = [
        ("dim_region", regions),
        ("dim_plan", plans),
        ("dim_provider", providers),
        ("dim_diagnosis_procedure", diagproc),
        ("dim_policyholder", policyholders),
        ("dim_policy", policies),
        ("fact_enrollment_event", enrollments),
        ("fact_premium_payment", payments),
        ("fact_claim", claims),
    ]

    results = {}
    for table_name, rows in insert_order:
        # BigQuery expects native types; None removed where needed
        sanitized = []
        for r in rows:
            # remove keys with value None to let BQ accept missingable fields
            sanitized.append({k: (v if v is not None else None) for k, v in r.items()})
        inserted, errors = insert_json_rows(client, project, dataset, table_name, sanitized)
        results[table_name] = {"attempted": len(rows), "inserted": inserted, "errors": errors}
        # small pause to avoid BQ throttling on dev projects
        time.sleep(0.2)

    return results


if __name__ == "__main__":
    # quick local test runner
    import os
    project = os.getenv("PROJECT_ID") or input("Enter GCP project id: ")
    dataset = "health_insurance"
    #os.getenv("BQ_DATASET") or input("Enter BigQuery dataset name: ")
    print("Generating and loading small sample (100 policyholders / 100 policies)...")
    res = generate_and_load(project=project, dataset=dataset, location="asia-south1", num_policyholders=100, num_policies=100)
    print(res)
