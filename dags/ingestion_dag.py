from airflow.sdk import dag, task
from datetime import datetime, timedelta
from ingestion.ingest import ingest


default_args = {
    "owner":"dag_owner",
    "retries":3,
    "retries_delay":timedelta(25)
}

@dag(
    dag_id = "ingestion_dag",
    start_date = datetime(2026,1,1 ),
    schedule = "@daily", 
    catchup=False,
    default_args=default_args)
def ingest_crypto_data():
    @task
    def extract_raw_data():
        ingest()

    extract_raw_data()

ingest_crypto_data()
