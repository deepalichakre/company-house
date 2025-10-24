# src/schema.py
# Central source for BigQuery schemas, signature keys, and normalization mapping.

PROJECT_ID = "companies-house-pipeline"
DATASET = "companies_house"

COMPANY_INDEX_TABLE = "company_index"
COMPANY_DETAILS_TABLE = "company_details"

# -----------------------------
# BigQuery Schemas
# -----------------------------

COMPANY_INDEX_SCHEMA = [
    ("company_number", "STRING"),
    ("title", "STRING"),
    ("kind", "STRING"),
    ("company_status", "STRING"),
    ("company_type", "STRING"),
    ("snippet", "STRING"),
    ("address_snippet", "STRING"),
    ("address_line_1", "STRING"),
    ("address_locality", "STRING"),
    ("address_country", "STRING"),
    ("address_postal_code", "STRING"),
    ("links_self", "STRING"),
    ("date_of_creation", "DATE"),
    ("date_of_cessation", "DATE"),
    ("rank", "INT64"),
    ("date_indexed", "TIMESTAMP"),
    ("raw_json", "STRING"),
    ("row_signature", "STRING"),
]

COMPANY_DETAILS_SCHEMA = [
    ("company_number", "STRING"),
    ("company_name", "STRING"),
    ("company_status", "STRING"),
    ("date_of_creation", "DATE"),
    ("etag", "STRING"),
    ("has_been_liquidated", "BOOL"),
    ("has_charges", "BOOL"),
    ("has_insolvency_history", "BOOL"),
    ("jurisdiction", "STRING"),
    ("last_full_members_list_date", "DATE"),
    ("registered_address_line_1", "STRING"),
    ("registered_address_line_2", "STRING"),
    ("registered_address_locality", "STRING"),
    ("registered_address_country", "STRING"),
    ("registered_address_postal_code", "STRING"),
    ("sic_codes", "STRING"),
    ("type", "STRING"),
    ("registered_office_is_in_dispute", "BOOL"),
    ("undeliverable_registered_office_address", "BOOL"),
    ("has_super_secure_pscs", "BOOL"),
    ("links_self", "STRING"),
    ("links_persons_with_significant_control", "STRING"),
    ("links_filing_history", "STRING"),
    ("links_officers", "STRING"),
    ("accounts_json", "STRING"),
    ("confirmation_statement_json", "STRING"),
    ("date_indexed", "TIMESTAMP"),
    ("raw_json", "STRING"),
    ("row_signature", "STRING"),
    ("index_row_signature", "STRING"), 
]

# -----------------------------
# Signature keys
# -----------------------------

COMPANY_INDEX_SIGNATURE_KEYS = [
    "company_number", "title", "kind", "company_status", "company_type",
    "snippet", "address_snippet", "address_line_1",
    "address_locality", "address_country", "address_postal_code"
]

COMPANY_DETAILS_SIGNATURE_KEYS = [
    "company_number", "company_name", "company_status",
    "date_of_creation", "registered_address_line_1", "registered_address_postal_code"
]

# -----------------------------
# Normalization mapping
# -----------------------------

# Format: "target_field": ("field_name", "parent_dict") — parent_dict can be None
COMPANY_INDEX_NORMALIZE_MAP = {
    "company_number": ("company_number", None),
    "title": ("title", None),
    "kind": ("kind", None),
    "company_status": ("company_status", None),
    "company_type": ("company_type", None),
    "snippet": ("snippet", None),
    "address_snippet": ("address_snippet", None),
    "address_line_1": ("address_line_1", "address"),
    "address_locality": ("locality", "address"),
    "address_country": ("country", "address"),
    "address_postal_code": ("postal_code", "address"),
    "links_self": ("self", "links"),
    "date_of_creation": ("date_of_creation", None),
    "date_of_cessation": ("date_of_cessation", None),
    "rank": ("rank", None),
}

COMPANY_DETAILS_NORMALIZE_MAP = {
    "company_number": ("company_number", None),
    "company_name": ("company_name", None),
    "company_status": ("company_status", None),
    "date_of_creation": ("date_of_creation", None),
    "etag": ("etag", None),
    "has_been_liquidated": ("has_been_liquidated", None),
    "has_charges": ("has_charges", None),
    "has_insolvency_history": ("has_insolvency_history", None),
    "jurisdiction": ("jurisdiction", None),
    "last_full_members_list_date": ("last_full_members_list_date", None),
    "registered_address_line_1": ("address_line_1", "registered_office_address"),
    "registered_address_line_2": ("address_line_2", "registered_office_address"),
    "registered_address_locality": ("locality", "registered_office_address"),
    "registered_address_country": ("country", "registered_office_address"),
    "registered_address_postal_code": ("postal_code", "registered_office_address"),
    "sic_codes": ("sic_codes", None),
    "type": ("type", None),
    "registered_office_is_in_dispute": ("registered_office_is_in_dispute", None),
    "undeliverable_registered_office_address": ("undeliverable_registered_office_address", None),
    "has_super_secure_pscs": ("has_super_secure_pscs", None),
    "links_self": ("self", "links"),
    "links_persons_with_significant_control": ("persons_with_significant_control", "links"),
    "links_filing_history": ("filing_history", "links"),
    "links_officers": ("officers", "links"),
    "accounts_json": ("accounts", None),
    "confirmation_statement_json": ("confirmation_statement", None),
}

# -----------------------------
# Table configuration lookup
# -----------------------------
TABLE_CONFIG = {
    "company_index": {
        "schema": COMPANY_INDEX_SCHEMA,
        "normalize_map": COMPANY_INDEX_NORMALIZE_MAP,
        "signature_keys": COMPANY_INDEX_SIGNATURE_KEYS,
    },
    "company_details": {
        "schema": COMPANY_DETAILS_SCHEMA,
        "normalize_map": COMPANY_DETAILS_NORMALIZE_MAP,
        "signature_keys": COMPANY_DETAILS_SIGNATURE_KEYS,
    },
}

def fq_table(project: str, dataset: str, table: str) -> str:
    return f"{project}.{dataset}.{table}"
# src/schema.py
# Central source for BigQuery schemas, signature keys, and normalization mapping.

PROJECT_ID = "companies-house-pipeline"
DATASET = "companies_house"

COMPANY_INDEX_TABLE = "company_index"
COMPANY_DETAILS_TABLE = "company_details"

# -----------------------------
# BigQuery Schemas
# -----------------------------

COMPANY_INDEX_SCHEMA = [
    ("company_number", "STRING"),
    ("title", "STRING"),
    ("kind", "STRING"),
    ("company_status", "STRING"),
    ("company_type", "STRING"),
    ("snippet", "STRING"),
    ("address_snippet", "STRING"),
    ("address_line_1", "STRING"),
    ("address_locality", "STRING"),
    ("address_country", "STRING"),
    ("address_postal_code", "STRING"),
    ("links_self", "STRING"),
    ("date_of_creation", "DATE"),
    ("date_of_cessation", "DATE"),
    ("rank", "INT64"),
    ("date_indexed", "TIMESTAMP"),
    ("raw_json", "STRING"),
    ("row_signature", "STRING"),
]

COMPANY_DETAILS_SCHEMA = [
    ("company_number", "STRING"),
    ("company_name", "STRING"),
    ("company_status", "STRING"),
    ("date_of_creation", "DATE"),
    ("etag", "STRING"),
    ("has_been_liquidated", "BOOL"),
    ("has_charges", "BOOL"),
    ("has_insolvency_history", "BOOL"),
    ("jurisdiction", "STRING"),
    ("last_full_members_list_date", "DATE"),
    ("registered_address_line_1", "STRING"),
    ("registered_address_line_2", "STRING"),
    ("registered_address_locality", "STRING"),
    ("registered_address_country", "STRING"),
    ("registered_address_postal_code", "STRING"),
    ("sic_codes", "STRING"),
    ("type", "STRING"),
    ("registered_office_is_in_dispute", "BOOL"),
    ("undeliverable_registered_office_address", "BOOL"),
    ("has_super_secure_pscs", "BOOL"),
    ("links_self", "STRING"),
    ("links_persons_with_significant_control", "STRING"),
    ("links_filing_history", "STRING"),
    ("links_officers", "STRING"),
    ("accounts_json", "STRING"),
    ("confirmation_statement_json", "STRING"),
    ("date_indexed", "TIMESTAMP"),
    ("raw_json", "STRING"),
    ("row_signature", "STRING"),
]

# -----------------------------
# Signature keys
# -----------------------------

COMPANY_INDEX_SIGNATURE_KEYS = [
    "company_number", "title", "kind", "company_status", "company_type",
    "snippet", "address_snippet", "address_line_1",
    "address_locality", "address_country", "address_postal_code"
]

COMPANY_DETAILS_SIGNATURE_KEYS = [
    "company_number", "company_name", "company_status",
    "date_of_creation", "registered_address_line_1", "registered_address_postal_code"
]

# -----------------------------
# Normalization mapping
# -----------------------------

# Format: "target_field": ("field_name", "parent_dict") — parent_dict can be None
COMPANY_INDEX_NORMALIZE_MAP = {
    "company_number": ("company_number", None),
    "title": ("title", None),
    "kind": ("kind", None),
    "company_status": ("company_status", None),
    "company_type": ("company_type", None),
    "snippet": ("snippet", None),
    "address_snippet": ("address_snippet", None),
    "address_line_1": ("address_line_1", "address"),
    "address_locality": ("locality", "address"),
    "address_country": ("country", "address"),
    "address_postal_code": ("postal_code", "address"),
    "links_self": ("self", "links"),
    "date_of_creation": ("date_of_creation", None),
    "date_of_cessation": ("date_of_cessation", None),
    "rank": ("rank", None),
}

COMPANY_DETAILS_NORMALIZE_MAP = {
    "company_number": ("company_number", None),
    "company_name": ("company_name", None),
    "company_status": ("company_status", None),
    "date_of_creation": ("date_of_creation", None),
    "etag": ("etag", None),
    "has_been_liquidated": ("has_been_liquidated", None),
    "has_charges": ("has_charges", None),
    "has_insolvency_history": ("has_insolvency_history", None),
    "jurisdiction": ("jurisdiction", None),
    "last_full_members_list_date": ("last_full_members_list_date", None),
    "registered_address_line_1": ("address_line_1", "registered_office_address"),
    "registered_address_line_2": ("address_line_2", "registered_office_address"),
    "registered_address_locality": ("locality", "registered_office_address"),
    "registered_address_country": ("country", "registered_office_address"),
    "registered_address_postal_code": ("postal_code", "registered_office_address"),
    "sic_codes": ("sic_codes", None),
    "type": ("type", None),
    "registered_office_is_in_dispute": ("registered_office_is_in_dispute", None),
    "undeliverable_registered_office_address": ("undeliverable_registered_office_address", None),
    "has_super_secure_pscs": ("has_super_secure_pscs", None),
    "links_self": ("self", "links"),
    "links_persons_with_significant_control": ("persons_with_significant_control", "links"),
    "links_filing_history": ("filing_history", "links"),
    "links_officers": ("officers", "links"),
    "accounts_json": ("accounts", None),
    "confirmation_statement_json": ("confirmation_statement", None),
}

# -----------------------------
# Table configuration lookup
# -----------------------------
TABLE_CONFIG = {
    "company_index": {
        "schema": COMPANY_INDEX_SCHEMA,
        "normalize_map": COMPANY_INDEX_NORMALIZE_MAP,
        "signature_keys": COMPANY_INDEX_SIGNATURE_KEYS,
    },
    "company_details": {
        "schema": COMPANY_DETAILS_SCHEMA,
        "normalize_map": COMPANY_DETAILS_NORMALIZE_MAP,
        "signature_keys": COMPANY_DETAILS_SIGNATURE_KEYS,
    },
}

def fq_table(project: str, dataset: str, table: str) -> str:
    return f"{project}.{dataset}.{table}"
