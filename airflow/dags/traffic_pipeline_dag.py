from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# -----------------------------------
# DEFAULT CONFIG
# -----------------------------------
default_args = {
    "owner": "dharshan",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

# -----------------------------------
# DAG DEFINITION
# -----------------------------------
with DAG(
    dag_id="traffic_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    # -----------------------------------
    # OPTIONAL (MANUAL ONLY)
    # -----------------------------------
    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command="cd /opt/project && python data_ingestion/crash_api_extract.py"
    )

    youtube = BashOperator(
        task_id="youtube_ingestion",
        bash_command="cd /opt/project && python data_ingestion/youtube_api_extract.py"
    )

    # -----------------------------------
    # MAIN PIPELINE (DAILY)
    # -----------------------------------
    silver = BashOperator(
        task_id="silver_processing",
        bash_command="cd /opt/project && python data_ingestion/spark_jobs/cleaning_jobs.py"
    )

    gold = BashOperator(
        task_id="gold_processing",
        bash_command="cd /opt/project && python data_ingestion/spark_jobs/gold_layer.py"
    )

    warehouse = BashOperator(
        task_id="warehouse_loading",
        bash_command="cd /opt/project && python data_ingestion/spark_jobs/star_schema.py"
    )

    scd = BashOperator(
        task_id="scd_processing",
        bash_command="cd /opt/project && python data_ingestion/spark_jobs/scd_dimension.py"
    )

    # -----------------------------------
    # FLOW (ONLY MAIN PIPELINE)
    # -----------------------------------
    silver >> gold >> warehouse >> scd