from google.cloud import storage, bigquery
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json
import re

# Initialize Spark Session
spark = SparkSession.builder.appName("RetailerMySQLToLanding").getOrCreate()

# Google Cloud Storage (GCS) Configuration
GCS_BUCKET = "gcsprojectbkt"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/retailer-db/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/retailer-db/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/retailer_config.csv"

# BigQuery Configuration
BQ_PROJECT = "ashuproj-454704"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"

# MySQL Configuration
MYSQL_CONFIG = {
    "url": "jdbc:mysql://35.184.144.178:3306/retailerDB?useSSL=false&allowPublicKeyRetrieval=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "mypass"
}

# Initialize GCS & BigQuery Clients
storage_client = storage.Client()
bq_client = bigquery.Client()

# Logging Mechanism
log_entries = []  

def log_event(event_type, message, table=None):
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")

def move_existing_files_to_archive(table):
    blobs = list(storage_client.bucket(GCS_BUCKET).list_blobs(prefix=f"landing/retailer-db/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]

    for file in existing_files:
        match = re.search(r"(\d{2})(\d{2})(\d{4})", file)
        if match:
            day, month, year = match.groups()
            archive_path = f"landing/retailer-db/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
            
            source_blob = storage_client.bucket(GCS_BUCKET).blob(file)
            destination_blob = storage_client.bucket(GCS_BUCKET).blob(archive_path)
            storage_client.bucket(GCS_BUCKET).copy_blob(source_blob, storage_client.bucket(GCS_BUCKET), destination_blob.name)
            source_blob.delete()

            log_event("INFO", f"Moved {file} to {archive_path}", table=table)

def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        df = (spark.read
                .format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", f"(SELECT * FROM {table}) AS t")
                .load())

        if df.isEmpty():
            log_event("WARNING", f"No data extracted from {table}. Skipping file write.", table=table)
            return

        pandas_df = df.toPandas()
        json_data = pandas_df.to_json(orient="records", lines=True)

        JSON_FILE_PATH = f"landing/retailer-db/{table}/{table}_{datetime.datetime.today().strftime('%d%m%Y')}.json"

        bucket = storage_client.bucket(GCS_BUCKET)
        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(json_data, content_type="application/json")

        log_event("SUCCESS", f"JSON file saved at gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)

    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)
