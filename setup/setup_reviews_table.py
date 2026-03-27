from google.cloud import bigquery

PROJECT_ID = "gorgias-case-study-491217"
DATASET_ID = "lead-enrichment"

def setup_companies_table():
    client = bigquery.Client(project=PROJECT_ID)
    
    schema = [
        bigquery.SchemaField("domain", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("text", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("star_rating", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("date_published", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("reviewer_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("comapny_replied", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("language", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("has_text", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("scraped_at", "TIMESTAMP", mode="REQUIRED")
    ]

    table = bigquery.Table(f"{PROJECT_ID}.{DATASET_ID}.reviews", schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        type=bigquery.TimePartitioningType.DAY,
        field="date_published"
    )
    table.clustering_fields = ["domain", "star_rating"]
    client.create_table(table, exists_ok=TRUE)

if __name__ == "__main__":
    setup_companies_table()