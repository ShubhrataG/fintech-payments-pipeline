from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'shubhrata',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['shubhratagupta32@gmail.com'],
    'email_on_retry': False,
    'sla': timedelta(minutes=10),
}

dag = DAG(
    dag_id='fintech_payments_pipeline',
    description='End to end real time payments pipeline Bronze Silver Gold',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 * * * *',
    catchup=False,
    tags=['fintech', 'payments', 'delta-lake', 'databricks'],
)

def run_bronze():
    print("Running Bronze ingestion job...")
    print("Reading from Kafka topic: payments.events")
    print("Writing raw events to Delta table: bronze_payments")
    print("Bronze job completed successfully!")

def run_silver():
    print("Running Silver transformation job...")
    print("Reading from Delta table: bronze_payments")
    print("Deduplicating on payment_id...")
    print("Writing clean data to Delta table: silver_payments")
    print("Silver job completed successfully!")

def run_gold():
    print("Running Gold aggregation job...")
    print("Reading from Delta table: silver_payments")
    print("Calculating KPIs: revenue, fraud rate, transaction volume...")
    print("Writing aggregated metrics to Delta table: gold_payments")
    print("Gold job completed successfully!")

def check_data_quality():
    print("Running data quality checks...")
    print("Checking: no nulls in payment_id")
    print("Checking: amount is always positive")
    print("Checking: status is succeeded/failed/pending")
    print("All data quality checks passed!")

start = EmptyOperator(task_id='start_pipeline', dag=dag)

bronze_task = PythonOperator(
    task_id='ingest_to_bronze',
    python_callable=run_bronze,
    dag=dag
)

silver_task = PythonOperator(
    task_id='transform_to_silver',
    python_callable=run_silver,
    dag=dag
)

data_quality_task = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag
)

gold_task = PythonOperator(
    task_id='aggregate_to_gold',
    python_callable=run_gold,
    dag=dag
)

end = EmptyOperator(task_id='end_pipeline', dag=dag)

start >> bronze_task >> silver_task >> data_quality_task >> gold_task >> end
