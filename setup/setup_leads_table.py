from google.cloud import bigquery

PROJECT_ID = "gorgias-case-study-491217"
DATASET_ID = "lead-enrichment"

def setup_companies_table():
    client = bigquery.Client(project=PROJECT_ID)

    dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
    dataset.location = "FR"
    client.create_dataset(dataset, exists_ok=TRUE)
    
    schema = [
        bigquery.SchemaField("domain", "STRING", mode="REQUIRED")
        bigquery.SchemaField("company_name", "STRING", mode="REQUIRED")
        bigquery.SchemaField("industry", "STRING", mode="NULLABLE")
        bigquery.SchemaField("country", "STRING", mode="NULLABLE")
        bigquery.SchemaField("employee_count", "INTEGER", mode="NULLABLE")
        bigquery.SchemaField("year_founded", "INTEGER", mode="NULLABLE")
    ]

    bigquery.Table(f"{PROJECT_ID}.{DATASET_ID}.leads", schema=schema)
    client.create_table(table, exists_ok=TRUE)

if __name__ == "__main__":
    setup_companies_table()