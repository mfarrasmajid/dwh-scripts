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

def array_literal_str(str_list):
    # Build a ClickHouse Array(String) literal safely
    return "[" + ",".join(repr(s) for s in str_list if isinstance(s, str) and s) + "]"

def pick_ch_type(row):
    """
    Decide ClickHouse type for a key based on profiling row dict.
    row keys: has_uuid, has_dt, has_float, has_int, has_boolish, uniq, max_len, min_int, max_int
    """
    # Highest-precedence types first
    if row.get("has_uuid"):
        return "UUID"
    if row.get("has_dt"):
        return "DateTime64(3)"   # change to Date if you prefer date-only

    # Numeric
    if row.get("has_float"):
        return "Float64"
    if row.get("has_int"):
        # If you want UInt64 when >=0:
        if row.get("min_int") is not None and row["min_int"] >= 0:
            return "UInt64"
        return "Int64"

    # Booleanish → UInt8 (0/1)
    if row.get("has_boolish"):
        return "UInt8"

    # Small-cardinality short strings → LowCardinality(String)
    if (row.get("uniq", 0) <= 50) and (row.get("max_len", 0) <= 64):
        return "LowCardinality(String)"

    return "String"

def cast_expr_for_type(k: str, ch_type: str) -> str:
    key_lit = repr(k)

    if ch_type == "UUID":
        return f"toUUIDOrNull(JSONExtractString(payload, {key_lit})) AS {qident(k)}"

    if ch_type == "DateTime64(3)":
        # universal: parse dari string
        return (f"parseDateTime64BestEffortOrNull(JSONExtractString(payload, {key_lit}), 3) "
                f"AS {qident(k)}")

    if ch_type == "Float64":
        return f"toFloat64OrNull(JSONExtractString(payload, {key_lit})) AS {qident(k)}"

    if ch_type in ("Int64", "UInt64"):
        base = f"toInt64OrNull(JSONExtractString(payload, {key_lit}))"
        if ch_type == "UInt64":
            base = f"toUInt64OrNull(JSONExtractString(payload, {key_lit}))"
        return f"{base} AS {qident(k)}"

    if ch_type == "UInt8":
        # true/false/1/0 → 0/1; jika bukan itu, coba cast angka
        return (f"multiIf(lowerUTF8(JSONExtractString(payload, {key_lit})) IN ('true','1'), toUInt8(1), "
                f"lowerUTF8(JSONExtractString(payload, {key_lit})) IN ('false','0'), toUInt8(0), "
                f"toUInt8OrNull(JSONExtractString(payload, {key_lit}))) AS {qident(k)}")

    # String / LowCardinality(String)
    return f"JSONExtractString(payload, {key_lit}) AS {qident(k)}"


# ===== CALLABLE =====
def build_all_pg_tables():
    from airflow.models import Variable

    CH_HOST = Variable.get("clickhouse_datalake_host")
    CH_PORT = int(Variable.get("clickhouse_datalake_port", default_var="9000"))
    CH_USER = Variable.get("clickhouse_datalake_user")
    CH_PASS = Variable.get("clickhouse_datalake_pass")
    CH_DB   = Variable.get("clickhouse_datalake_db", default_var="default")

    RAW_DB_TABLE = "exware.all_mysql_data"
    MAX_KEYS_PER_TABLE = 5000
    SAMPLE_ROWS = 200000  # sample to infer types

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
        keys = [k for k in keys if isinstance(k, str) and k]  # sanitize
        if not keys:
            continue

        tgt_db = sanitize_name(f"{source_db}__{source_schema}", append_underscore=True)
        tgt_tbl = sanitize_name(source_tbl)
        mv_name = mv_name_for(source_db, source_schema, source_tbl)

        client.execute(f"CREATE DATABASE IF NOT EXISTS {qident(tgt_db)}")

        # 2) Infer per-key types with a single profiling query
        keys_arr_sql = array_literal_str(keys)

        prof_sql = f"""
        SELECT
        key,
        max(has_uuid)       AS has_uuid,
        max(has_dt)         AS has_dt,
        max(has_float)      AS has_float,
        max(has_int)        AS has_int,
        max(has_boolish)    AS has_boolish,
        anyLast(uniq_cnt)   AS uniq,
        anyLast(max_len)    AS max_len,
        anyLast(min_int)    AS min_int
        FROM
        (
        SELECT
            key,
            countIf(toUUIDOrNull(v) IS NOT NULL)                  AS has_uuid,
            countIf(parseDateTimeBestEffortOrNull(v) IS NOT NULL) AS has_dt,
            countIf(toFloat64OrNull(v) IS NOT NULL)               AS has_float,
            countIf(toInt64OrNull(v) IS NOT NULL)                 AS has_int,
            countIf(lowerUTF8(v) IN ('true','false','0','1'))     AS has_boolish,
            uniqExact(v)                                          AS uniq_cnt,
            max(lengthUTF8(v))                                    AS max_len,
            minOrNull(toInt64OrNull(v))                           AS min_int
        FROM
        (
            SELECT
            arrayJoin({keys_arr_sql}) AS key,
            JSONExtractString(payload, key) AS v
            FROM {RAW_DB_TABLE}
            WHERE source_database = %(sdb)s
            AND source_schema   = %(ssch)s
            AND source_table    = %(stbl)s
            LIMIT %(sample)s
        )
        GROUP BY key
        )
        GROUP BY key
        ORDER BY key
        """

        prof_rows = client.execute(prof_sql, {"sdb": source_db, "ssch": source_schema, "stbl": source_tbl, "sample": SAMPLE_ROWS})
        # Map: key -> ch_type
        inferred_types = {}
        for r in prof_rows:
            key = r[0]
            row = {
                "has_uuid": r[1] > 0,
                "has_dt": r[2] > 0,
                "has_float": r[3] > 0,
                "has_int": r[4] > 0,
                "has_boolish": r[5] > 0,
                "uniq": int(r[6]) if r[6] is not None else 0,
                "max_len": int(r[7]) if r[7] is not None else 0,
                "min_int": int(r[8]) if r[8] is not None else None,
            }
            inferred_types[key] = pick_ch_type(row)

        # Fallback for keys not seen in sample
        for k in keys:
            if k not in inferred_types:
                inferred_types[k] = "String"

        # 3) Read existing columns
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

        # 4) Create table (typed)
        if not table_exists:
            col_defs = [f"{qident('pk_value')} String"]  # keep pk as String unless you have composite PK logic
            for k in keys:
                ch_type = inferred_types[k]
                # LowCardinality(String) + Nullable supported as LowCardinality(Nullable(String))
                if ch_type == "LowCardinality(String)":
                    col_defs.append(f"{qident(k)} LowCardinality(Nullable(String))")
                else:
                    col_defs.append(f"{qident(k)} Nullable({ch_type})")
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
                ORDER BY position
            """, {"db": tgt_db, "tbl": tgt_tbl})
            existing_cols = {r[0] for r in rows}
            print(f"Created table {tgt_db}.{tgt_tbl} with {len(existing_cols)} columns")

        # 5) Add new columns w/ types if new keys found
        new_cols = []
        for k in keys:
            if k not in existing_cols:
                ch_type = inferred_types[k]
                if ch_type == "LowCardinality(String)":
                    new_cols.append(f"ADD COLUMN IF NOT EXISTS {qident(k)} LowCardinality(Nullable(String))")
                else:
                    new_cols.append(f"ADD COLUMN IF NOT EXISTS {qident(k)} Nullable({ch_type})")
        if new_cols:
            client.execute(f"ALTER TABLE {qident(tgt_db)}.{qident(tgt_tbl)} " + ", ".join(new_cols))
            print(f"Added {len(new_cols)} new columns to {tgt_db}.{tgt_tbl}")

        # 6) Create / refresh MV with typed casts
        mv_exists = bool(client.execute("""
            SELECT 1 FROM system.tables
            WHERE database = %(db)s AND name = %(mv)s AND engine = 'MaterializedView'
            LIMIT 1
        """, {"db": tgt_db, "mv": mv_name}))

        select_cols = ["pk_value"]
        for k in keys:
            ch_type = inferred_types[k]
            select_cols.append(cast_expr_for_type(k, ch_type))
        select_cols += ["_op AS src_op", "now64(3) AS version_clickhouse"]

        rows = client.execute("""
            SELECT name FROM system.columns
            WHERE database = %(db)s AND table = %(tbl)s
            ORDER BY position
        """, {"db": tgt_db, "tbl": tgt_tbl})
        target_col_order = [r[0] for r in rows if r[0] != "is_deleted_clickhouse"]

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