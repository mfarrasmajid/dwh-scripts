from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta

import uuid
from clickhouse_driver import Client

# ---------------------
# 1) Default args & DAG
# ---------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='DL_exware_generate_raw_kafka',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['dl', 'exware', 'postgres', 'init'],
)

# ---------------------
# 2) Python callable
# ---------------------
def main():
    # --- ClickHouse Connection ---
    ch_client = Client(
        host=Variable.get("clickhouse_datalake_host"),
        user=Variable.get("clickhouse_datalake_user"),
        password=Variable.get("clickhouse_datalake_pass"),
    )

    # Databases
    ch_client.execute("CREATE DATABASE IF NOT EXISTS kafka_mass")
    ch_client.execute("CREATE DATABASE IF NOT EXISTS kafka_connector")
    ch_client.execute("CREATE DATABASE IF NOT EXISTS exware")

    # Unique consumer group
    group_name = f"ch_consumer_pg_{uuid.uuid4().hex[:16]}"

    kafka_brokers = Variable.get("kafka_broker_list", default_var="192.168.101.164:9092")
    pg_topic = Variable.get("debezium_pg_topic", default_var="postgres-mass-server-4.all-dbs")

    # Kafka Source Table
    ch_client.execute(f"""
    CREATE TABLE IF NOT EXISTS kafka_mass.kafka_raw_exware
    (
        _raw String
    )
    ENGINE = Kafka
    SETTINGS kafka_broker_list = %(brokers)s,
             kafka_topic_list = %(topic)s,
             kafka_group_name = %(group)s,
             kafka_format = 'RawBLOB'
    """, {'brokers': kafka_brokers, 'topic': pg_topic, 'group': group_name})

    # Target Table (ReplacingMergeTree)
    ch_client.execute("""
    CREATE TABLE IF NOT EXISTS exware.all_mysql_data
    (
        source_database String,
        source_schema String,
        source_table String,
        pk_value String,
        payload String,
        is_deleted UInt8 DEFAULT 0,
        _op String,
        _ts DateTime
    )
    ENGINE = ReplacingMergeTree(_ts)
    ORDER BY (source_database, source_schema, source_table, pk_value)
    """)

    # Materialized View
    ch_client.execute("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_connector.mv_exware_data
    TO exware.all_mysql_data
    AS
    WITH
        JSONExtractRaw(_raw, 'after')  AS j_after,
        JSONExtractRaw(_raw, 'before') AS j_before,
        JSONExtractRaw(_raw, 'source') AS j_src,
        JSON_VALUE(JSONExtractRaw(_raw, 'op'), '$') AS op
    SELECT
        JSON_VALUE(j_src, '$.db')      AS source_database,
        JSON_VALUE(j_src, '$.schema')  AS source_schema,
        JSON_VALUE(j_src, '$.table')   AS source_table,
        if(
            op = 'd',
            coalesce(
                JSON_VALUE(j_before, '$.id'),
                arrayElement(JSONExtractKeysAndValues(j_before, 'String').1, 1)
            ),
            coalesce(
                JSON_VALUE(j_after, '$.id'),
                arrayElement(JSONExtractKeysAndValues(j_after, 'String').1, 1)
            )
        ) AS pk_value,

        if(op = 'd', j_before, j_after) AS payload,

        op = 'd' AS is_deleted,
        op       AS _op,
        now()    AS _ts
    FROM kafka_mass.kafka_raw_exware
    """)

    print("✅ Setup selesai! PostgreSQL Debezium events direplikasi ke ClickHouse (exware.all_mysql_data).")

# ---------------------
# 3) Task
# ---------------------
generate_task = PythonOperator(
    task_id='generate_raw_kafka_pg',
    python_callable=main,
    dag=dag,
)