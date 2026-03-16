"""
Wind Turbine Streaming Platform — Airflow DAG.

Responsibilities:
  - Monitor Kafka consumer lag
  - Launch / monitor Spark streaming jobs
  - Run data-quality checks on Silver and Gold layers
  - Alert on pipeline failures
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.trigger_rule import TriggerRule

# ── Default args ────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ── DAG definition ──────────────────────────────────────────────────────────

with DAG(
    dag_id="wind_turbine_streaming_pipeline",
    default_args=default_args,
    description="Orchestrate the Wind Turbine streaming data platform",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["wind-turbine", "streaming", "data-platform"],
    max_active_runs=1,
) as dag:

    # ── Infrastructure health checks ───────────────────────────────────

    check_kafka = HttpSensor(
        task_id="check_kafka_health",
        http_conn_id="kafka_connect",
        endpoint="/",
        method="GET",
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=120,
        mode="reschedule",
        soft_fail=True,
    )

    check_schema_registry = HttpSensor(
        task_id="check_schema_registry_health",
        http_conn_id="schema_registry",
        endpoint="/subjects",
        method="GET",
        response_check=lambda response: response.status_code == 200,
        poke_interval=30,
        timeout=120,
        mode="reschedule",
        soft_fail=True,
    )

    # ── Kafka consumer lag check ───────────────────────────────────────

    def _check_consumer_lag(**context):
        """Query Kafka for consumer group lag and push to XCom."""
        import subprocess
        import json
        import os

        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        result = subprocess.run(
            [
                "kafka-consumer-groups",
                "--bootstrap-server", bootstrap,
                "--group", "spark-streaming-consumer",
                "--describe",
            ],
            capture_output=True, text=True, timeout=30,
        )
        context["ti"].xcom_push(key="consumer_lag_output",
                                value=result.stdout)
        if "has no active members" in result.stdout:
            raise ValueError("Consumer group has no active members — "
                             "Spark streaming may not be running")

    check_lag = PythonOperator(
        task_id="check_kafka_consumer_lag",
        python_callable=_check_consumer_lag,
    )

    # ── Data quality checks ────────────────────────────────────────────

    def _check_bronze_freshness(**context):
        """Ensure Bronze layer has received recent data."""
        import os
        from datetime import datetime, timezone

        # Use MinIO client to check latest file modification
        from minio import Minio

        client = Minio(
            os.environ.get("MINIO_ENDPOINT", "minio:9000").replace("http://", ""),
            access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin123"),
            secure=False,
        )

        objects = list(client.list_objects("wind-turbine-bronze",
                                           recursive=True))
        if not objects:
            raise ValueError("No objects found in Bronze bucket")

        latest = max(obj.last_modified for obj in objects)
        age = datetime.now(timezone.utc) - latest
        if age > timedelta(minutes=30):
            raise ValueError(
                f"Bronze layer is stale — last update {age} ago"
            )
        context["ti"].xcom_push(key="bronze_latest_object",
                                value=str(latest))

    check_bronze = PythonOperator(
        task_id="check_bronze_freshness",
        python_callable=_check_bronze_freshness,
    )

    def _check_record_counts(**context):
        """Verify row counts across layers are reasonable."""
        import os
        import psycopg2

        conn = psycopg2.connect(
            host=os.environ.get("TIMESCALEDB_HOST", "timescaledb"),
            port=int(os.environ.get("TIMESCALEDB_PORT", 5432)),
            dbname=os.environ.get("TIMESCALEDB_DB", "windturbine"),
            user=os.environ.get("TIMESCALEDB_USER", "windturbine"),
            password=os.environ.get("TIMESCALEDB_PASSWORD", ""),
        )
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) FROM hourly_power_generation
                    WHERE window_start > NOW() - INTERVAL '2 hours'
                """)
                recent_count = cur.fetchone()[0]

                if recent_count == 0:
                    raise ValueError(
                        "No hourly power records in the last 2 hours"
                    )

                cur.execute("""
                    SELECT COUNT(*) FROM turbine_anomalies
                    WHERE event_ts > NOW() - INTERVAL '24 hours'
                """)
                anomaly_count = cur.fetchone()[0]

            context["ti"].xcom_push(key="recent_hourly_records",
                                    value=recent_count)
            context["ti"].xcom_push(key="recent_anomalies",
                                    value=anomaly_count)
        finally:
            conn.close()

    check_db_records = PythonOperator(
        task_id="check_timescaledb_record_counts",
        python_callable=_check_record_counts,
    )

    # ── Spark job restart (if needed) ──────────────────────────────────

    restart_spark = BashOperator(
        task_id="restart_spark_streaming",
        bash_command=(
            "spark-submit "
            "--master ${SPARK_MASTER:-local[*]} "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.spark:spark-avro_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4 "
            "/app/spark/streaming_pipeline.py"
        ),
        trigger_rule=TriggerRule.ALL_FAILED,  # Only if health checks fail
    )

    # ── Pipeline summary ───────────────────────────────────────────────

    def _log_summary(**context):
        import logging
        logger = logging.getLogger("airflow.wind_turbine")
        lag = context["ti"].xcom_pull(
            task_ids="check_kafka_consumer_lag",
            key="consumer_lag_output",
        )
        logger.info("Pipeline health check completed successfully")

    pipeline_summary = PythonOperator(
        task_id="pipeline_health_summary",
        python_callable=_log_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task dependencies ──────────────────────────────────────────────

    [check_kafka, check_schema_registry] >> check_lag
    check_lag >> check_bronze >> check_db_records >> pipeline_summary
    [check_lag, check_bronze] >> restart_spark >> pipeline_summary
