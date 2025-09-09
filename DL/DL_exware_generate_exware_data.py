#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum

from clickhouse_driver import Client
import re

# ===== DAG =====
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 8, 1, tz="Asia/Jakarta"),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='DL_exware_generate_pg_tables',
    default_args=default_args,
    schedule_interval='0 17 * * *',   # 17:00 Asia/Jakarta
    catchup=False,
    tags=['DL', 'exware', 'postgres'],
)

# ===== UTIL =====
def qident(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"

def sanitize_name(name: str, append_underscore=False) -> str:
    x = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if re.match(r"^\d", x):
        x = "_" + x
    if append_underscore:
        x = "_ex_" + x
    return x

def mv_name_for(db: str, schema: str, tbl: str) -> str:
    return f"_mv__{sanitize_name(db)}__{sanitize_name(schema)}__{sanitize_name(tbl)}__from_raw"

# ===== CALLABLE =====
def build_all_pg_tables():
    # Import Variable inside callable
    from airflow.models import Variable

    CH_HOST = Variable.get("clickhouse_datalake_host")
    CH_PORT = int(Variable.get("clickhouse_datalake_port", default_var="9000"))
    CH_USER = Variable.get("clickhouse_datalake_user")
    CH_PASS = Variable.get("clickhouse_datalake_pass")
    CH_DB   = Variable.get("clickhouse_datalake_db", default_var="default")

    # Raw table produced by Debezium→Kafka→CH MV for PostgreSQL
    RAW_DB_TABLE = "exware.all_mysql_data"  # name as requested earlier
    MAX_KEYS_PER_TABLE = 5000  # beware very wide tables

    client = Client(
        host=CH_HOST, port=CH_PORT, user=CH_USER, password=CH_PASS, database=CH_DB,
        settings={"insert_deduplicate": 1}
    )

    # 1) Collect (db, schema, table, keys)
    groups = client.execute(f"""
        SELECT
            source_database,
            source_schema,
            source_table,
            arraySlice(arrayDistinct(arrayFlatten(groupArray(k))), 1, {MAX_KEYS_PER_TABLE}) AS keys
        FROM
        (
            SELECT
                source_database,
                source_schema,
                source_table,
                arrayMap(x -> x.1, JSONExtractKeysAndValues(payload, 'String')) AS k
            FROM {RAW_DB_TABLE}
        )
        GROUP BY source_database, source_schema, source_table
        ORDER BY source_database, source_schema, source_table
    """)

    print(f"Found {len(groups)} (db,schema,table) groups in raw")

    for source_db, source_schema, source_tbl, keys in groups:
        # Target database is a combo of db + schema to keep tables grouped cleanly
        tgt_db = sanitize_name(f"{source_db}__{source_schema}", append_underscore=True)
        tgt_tbl = sanitize_name(source_tbl)
        mv_name = mv_name_for(source_db, source_schema, source_tbl)

        # 2) Ensure target DB exists
        client.execute(f"CREATE DATABASE IF NOT EXISTS {qident(tgt_db)}")

        # 3) Read existing columns (if table exists)
        table_exists = bool(client.execute("""
            SELECT 1 FROM system.tables
            WHERE database = %(db)s AND name = %(tbl)s
            LIMIT 1
        """, {"db": tgt_db, "tbl": tgt_tbl}))
        existing_cols = set()
        if table_exists:
            rows = client.execute("""
                SELECT name FROM system.columns
                WHERE database = %(db)s AND table = %(tbl)s
            """, {"db": tgt_db, "tbl": tgt_tbl})
            existing_cols = {r[0] for r in rows}

        # 4) Create target table if needed
        if not table_exists:
            col_defs = [f"{qident('pk_value')} String"]
            for k in keys:
                if not k or not isinstance(k, str):
                    continue
                col_defs.append(f"{qident(k)} Nullable(String)")
            col_defs += [
                f"{qident('src_op')} LowCardinality(String)",
                f"{qident('is_deleted_clickhouse')} UInt8 MATERIALIZED if({qident('src_op')} = 'd', 1, 0)",
                f"{qident('version_clickhouse')} DateTime64(3)"
            ]
            client.execute(f"""
                CREATE TABLE IF NOT EXISTS {qident(tgt_db)}.{qident(tgt_tbl)} (
                    {', '.join(col_defs)}
                )
                ENGINE = ReplacingMergeTree({qident('version_clickhouse')})
                ORDER BY {qident('pk_value')}
                SETTINGS allow_nullable_key = 1
            """)
            rows = client.execute("""
                SELECT name FROM system.columns
                WHERE database = %(db)s AND table = %(tbl)s
            """, {"db": tgt_db, "tbl": tgt_tbl})
            existing_cols = {r[0] for r in rows}
            print(f"Created table {tgt_db}.{tgt_tbl} with {len(existing_cols)} columns")

        # 5) Add new columns if new keys appear
        new_cols = []
        for k in keys:
            if not k or not isinstance(k, str):
                continue
            if k not in existing_cols:
                new_cols.append(k)
        if new_cols:
            client.execute(
                f"ALTER TABLE {qident(tgt_db)}.{qident(tgt_tbl)} " +
                ", ".join(f"ADD COLUMN IF NOT EXISTS {qident(k)} Nullable(String)" for k in new_cols)
            )
            print(f"Added {len(new_cols)} new columns to {tgt_db}.{tgt_tbl}")

        # 6) Create/refresh MV
        mv_exists = bool(client.execute("""
            SELECT 1 FROM system.tables
            WHERE database = %(db)s AND name = %(mv)s AND engine = 'MaterializedView'
            LIMIT 1
        """, {"db": tgt_db, "mv": mv_name}))

        select_cols = ["pk_value"]
        select_cols += [f"JSONExtractString(payload, {repr(k)}) AS {qident(k)}" for k in keys if k and isinstance(k, str)]
        select_cols += ["_op AS src_op", "now64(3) AS version_clickhouse"]

        rows = client.execute("""
            SELECT name FROM system.columns
            WHERE database = %(db)s AND table = %(tbl)s
            ORDER BY position
        """, {"db": tgt_db, "tbl": tgt_tbl})
        target_col_order = [r[0] for r in rows if r[0] != "is_deleted_clickhouse"]  # MATERIALIZED

        base_create_mv = f"""
        CREATE MATERIALIZED VIEW {{if_not_exists}} {qident(tgt_db)}.{qident(mv_name)}
        TO {qident(tgt_db)}.{qident(tgt_tbl)}
        AS
        SELECT
            {', '.join(select_cols)}
        FROM {RAW_DB_TABLE}
        WHERE source_database = %(sdb)s
          AND source_schema   = %(ssch)s
          AND source_table    = %(stbl)s
        """

        params = {"sdb": source_db, "ssch": source_schema, "stbl": source_tbl}

        if not mv_exists:
            client.execute(base_create_mv.format(if_not_exists="IF NOT EXISTS"), params)
            print(f"Created MV {tgt_db}.{mv_name} -> {tgt_db}.{tgt_tbl}")
        else:
            if new_cols:
                client.execute(f"DROP VIEW {qident(tgt_db)}.{qident(mv_name)}")
                client.execute(base_create_mv.format(if_not_exists=""), params)
                print(f"Recreated MV {tgt_db}.{mv_name} with new columns")
            else:
                print(f"MV {tgt_db}.{mv_name} already up-to-date")

        # 7) One-time backfill if target empty
        target_row_count = client.execute(
            f"SELECT count() FROM {qident(tgt_db)}.{qident(tgt_tbl)}"
        )[0][0]
        if target_row_count == 0:
            print(f"Backfilling {tgt_db}.{tgt_tbl} once from raw ...")
            client.execute(f"""
                INSERT INTO {qident(tgt_db)}.{qident(tgt_tbl)} (
                    {', '.join(qident(c) for c in target_col_order)}
                )
                SELECT
                    {', '.join(select_cols)}
                FROM {RAW_DB_TABLE}
                WHERE source_database = %(sdb)s
                  AND source_schema   = %(ssch)s
                  AND source_table    = %(stbl)s
            """, params)
            print("Backfill done.")
        else:
            print(f"Skip backfill: {tgt_db}.{tgt_tbl} already has data.")

    print("All done.")

# ===== TASK =====
generate_task = PythonOperator(
    task_id='generate_all_pg_tables',
    python_callable=build_all_pg_tables,
    dag=dag,
)