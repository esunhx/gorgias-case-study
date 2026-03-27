from google.cloud import bigquery

PROJECT_ID = "gorgias-case-study-491217"
DATASET_ID = "lead_enrichment"
TABLE_ID = "reviews"

bq = bigquery.Client(project=PROJECT_ID)
table = bq.get_table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")

enrichment_fields = [
    bigquery.SchemaField("sentiment", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("category", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("pain_point", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("insight", "STRING", mode="NULLABLE")
]

table.schema = table.schema + enrichment_fields
bq.update_table(table, ["schema"])
