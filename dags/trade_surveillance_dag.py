from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Default args ──────────────────────────────────────────
default_args = {
    'owner': 'soham',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
}

# ── DAG Definition ────────────────────────────────────────
with DAG(
    dag_id='trade_surveillance_pipeline',
    description='End to end trade surveillance pipeline -Portfolio project',
    default_args=default_args,
    schedule_interval='0 6 * * 1-5',  # weekdays at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['trade_surveillance', 'finance', 'surveillance']
) as dag:

    # ── Task 1: Run Kafka Producer ────────────────────────
    run_producer = BashOperator(
        task_id='run_kafka_producer',
        bash_command=(
            'cd /opt/airflow && '
            'python3 /opt/airflow/src/kafka/producer.py'
        ),
        env={
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
            'KAFKA_TOPIC': 'trades',
            'TRADES_PER_SECOND': '5',
            'PYTHONPATH': '/opt/airflow',
        },
        execution_timeout=timedelta(minutes=6),
    )

    # ── Task 2: Run PySpark Processor ────────────────────
    run_spark = BashOperator(
        task_id='run_spark_processor',
        bash_command=(
            'docker exec spark spark-submit '
            '--master local[*] '
            '/app/src/spark/processor.py'
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── Task 3: Run dbt Models ───────────────────────────
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "cd /opt/airflow/dbt/trade_surveillance && "
            "/home/airflow/dbt_venv/bin/dbt run --profiles-dir /opt/airflow/dbt"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── Task 4: Run dbt Tests ────────────────────────────
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command=(
            'cd /opt/airflow/dbt/trade_surveillance && '
            '/home/airflow/dbt_venv/bin/dbt test --profiles-dir /opt/airflow/dbt'
        ),
        execution_timeout=timedelta(minutes=5),
    )

    # ── Task 5: Pipeline Complete ────────────────────────
    def log_completion(**context):
        print("=" * 50)
        print("TRADE SURVEILLANCE PIPELINE COMPLETE")
        print(f"DAG Run ID: {context['run_id']}")
        print(f"Execution Date: {context['execution_date']}")
        print("=" * 50)

    pipeline_complete = PythonOperator(
        task_id='pipeline_complete',
        python_callable=log_completion,
    )

    # ── Sequential Dependency Flow ────────────────────────
    # Producer generates data -> Spark reads & loads to DuckDB -> dbt transforms -> dbt tests -> complete
    run_producer >> run_spark >> run_dbt_models >> run_dbt_tests >> pipeline_complete

    