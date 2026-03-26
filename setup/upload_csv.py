import csv

from google.cloud import bigquery

PROJECT_ID = "gorgias-case-study-491217"
DATASET_ID = "lead-enrichment"
TABLE_ID = "leads"

bq = bigquery.Client(project=PROJECT_ID)
table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

def load_leads_from_csv(filepath: str) -> list[dict]:
    with open(filepath, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return [
            {
                "domain": row["domain"].strip(),
                "company_name": row["company_name"].strip(),
                "industry": row["industry"].strip(),
                "country": row["country"].strip(),
                "employee_count": int(row["employee_count"]) if row["employee_count"].strip() else None,
                "year_founded": int(row["year_founded"]) if row["year_founded"].strip() else None
            }
            for row in reader
        ]

def upload_leads(filepath: str) -> None:
    leads = load_leads_from_csv(filepath)
    if not leads:
        raise ValueError("No leads found")
    
    errors =  bq.insert_rows_json(table_ref, leads)
    if errors:
        raise RuntimeError("BigQuery insert error: {errors}")
    
    print(f"Succesfully uploaded {len(leads)} leads to {table_ref}")

if __name__ == "__main__":
    upload_leads("./leads/mock_leads.csv")