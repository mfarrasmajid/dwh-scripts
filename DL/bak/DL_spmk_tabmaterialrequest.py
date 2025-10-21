import os
import json
import math
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal
from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

# ========= Editable constants =========
MYSQL_CONN_ID = "db_oneflux_tms"
CLICKHOUSE_CONN_ID = "clickhouse_mitratel"
PG_LOG_CONN_ID = "airflow_logs_mitratel"

MYSQL_DATABASE = "db_tms"
MYSQL_TABLE = "tabMaterial Request"          # has a space → quoted with backticks
CLICKHOUSE_DATABASE = "spmk"
CLICKHOUSE_TABLE = "tabmaterialrequest"

# Primary key column
PK_COL = "name"
# Version column (incremental pivot & ReplacingMergeTree version)
VERSION_COL = "modified"

# Parallelism & chunking
CHUNK_SIZE = 10000        # rows per connection
MAX_PARALLEL = 4           # up to 4 connections at a time
TMP_DIR = "/tmp"
NDJSON_PREFIX = "DL_spmk_tabmaterialrequest_"

# Logging
LOG_TABLE = "airflow_logs"
LOG_TYPE = "incremental"
LOG_KATEGORI = "Data Lake"

# DAG
DAG_ID = "DL_spmk_tabmaterialrequest"
DAG_INTERVAL = "1-59/5 * * * *"
TAGS = ["dl", "spmk", "tabmaterialrequest", "incremental"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
}

def _base_ch_type(t: str) -> str:
    t = t.strip()
    if t.startswith("Nullable(") and t.endswith(")"):
        return t[9:-1]
    return t

def _mk_converter(ch_type: str):
    """
    Return a callable that converts a JSON value (str/None) to an object clickhouse_driver expects.
    """
    t = _base_ch_type(ch_type)

    if t.startswith("DateTime64") or t == "DateTime":
        def conv(v):
            if v is None or v == "":
                return None
            ts = pd.to_datetime(v, errors="coerce")
            if pd.isna(ts):
                return None
            py = ts.to_pydatetime()
            # clickhouse_driver expects tz-naive
            if getattr(py, "tzinfo", None) is not None:
                py = py.replace(tzinfo=None)
            return py
        return conv

    if t == "Date":
        def conv(v):
            if v is None or v == "":
                return None
            ts = pd.to_datetime(v, errors="coerce").date()
            return None if pd.isna(pd.Timestamp(ts)) else ts
        return conv

    if t in ("Int8","Int16","Int32","Int64"):
        def conv(v):
            if v is None or v == "":
                return None
            return int(v)
        return conv

    if t in ("Float32","Float64"):
        def conv(v):
            if v is None or v == "":
                return None
            return float(v)
        return conv

    if t.startswith("Decimal("):
        def conv(v):
            if v is None or v == "":
                return None
            return Decimal(str(v))
        return conv

    # default: String/other
    def conv(v):
        return None if v is None else str(v)
    return conv

# ========= Logging helper (your pattern) =========
def log_status(process_name: str, mark: str, status: str, error_message: Optional[str] = None):
    pg_hook = PostgresHook(postgres_conn_id=PG_LOG_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dag_name = f"{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}"

    if status == "pending":
        cursor.execute(
            f"""
            INSERT INTO {LOG_TABLE} (process_name, dag_name, type, status, start_time, mark, kategori)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (process_name, dag_name, LOG_TYPE, status, now, mark, LOG_KATEGORI),
        )
    else:
        cursor.execute(
            f"""
            UPDATE {LOG_TABLE}
            SET status = %s, end_time = %s, error_message = %s
            WHERE process_name = %s AND status = 'pending' AND dag_name = %s AND mark = %s AND kategori = %s
            """,
            (status, now, error_message, process_name, dag_name, mark, LOG_KATEGORI),
        )

    conn.commit()
    cursor.close()
    conn.close()

# ========= Utils: MySQL & CH schema =========
def _mysql_columns(mysql: MySqlHook) -> List[Dict[str, Any]]:
    sql = """
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    ORDER BY ORDINAL_POSITION
    """
    return [
        dict(zip(
            ["COLUMN_NAME","DATA_TYPE","CHARACTER_MAXIMUM_LENGTH","NUMERIC_PRECISION","NUMERIC_SCALE","DATETIME_PRECISION"],
            row
        ))
        for row in mysql.get_records(sql, parameters=(MYSQL_DATABASE, MYSQL_TABLE))
    ]

def _map_mysql_to_ch(col: Dict[str, Any]) -> str:
    t = (col["DATA_TYPE"] or "").lower()
    if t in ("varchar", "char", "text", "tinytext", "mediumtext", "longtext", "json", "enum", "set"):
        return "String"
    if t in ("datetime", "timestamp"):
        precision = int(col.get("DATETIME_PRECISION") or 6)
        precision = max(0, min(6, precision))
        return f"DateTime64({precision})" if precision > 0 else "DateTime"
    if t in ("date",):
        return "Date"
    if t in ("time",):
        return "String"
    if t in ("tinyint",):
        return "Int8"
    if t in ("smallint",):
        return "Int16"
    if t in ("mediumint", "int", "integer"):
        return "Int32"
    if t in ("bigint",):
        return "Int64"
    if t in ("decimal", "numeric"):
        prec = int(col.get("NUMERIC_PRECISION") or 38)
        scale = int(col.get("NUMERIC_SCALE") or 9)
        prec = max(1, min(76, prec))
        scale = max(0, min(prec - 1, scale))
        return f"Decimal({prec},{scale})"
    if t in ("float",):
        return "Float32"
    if t in ("double", "real", "double precision"):
        return "Float64"
    if t in ("blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary"):
        return "String"
    return "String"

def _normalize_ch_type(t: str) -> Tuple[str, bool]:
    t = t.strip()
    if t.startswith("Nullable(") and t.endswith(")"):
        return t[9:-1], True
    return t, False

def _ch_columns(click: ClickHouseHook) -> Dict[str, str]:
    conn = click.get_conn()

    def _lit(s: str) -> str:
        return s.replace("'", "\\'")

    sql = f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{_lit(CLICKHOUSE_DATABASE)}'
          AND table    = '{_lit(CLICKHOUSE_TABLE)}'
        ORDER BY position
    """
    rows = conn.execute(sql)
    return {name: t for (name, t) in rows}


def _widen_type_if_needed(src: str, dst: str) -> str:
    if src == dst:
        return dst
    ranks = {"Int8":1,"Int16":2,"Int32":3,"Int64":4}
    if dst in ranks and src in ranks and ranks[src] > ranks[dst]:
        return src
    if dst.startswith("Float") and src == "Float64":
        return "Float64"
    if dst.startswith("Decimal(") and src.startswith("Decimal("):
        def ps(x):
            p,s = x[8:-1].split(",")
            return int(p), int(s)
        dp, ds = ps(dst); sp, ss = ps(src)
        return f"Decimal({max(dp,sp)},{max(ds,ss)})"
    if dst == "DateTime" and src.startswith("DateTime64("):
        return src
    if src == "String" or dst == "String":
        return "String"
    return dst

# ========= DAG definition =========
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=DAG_INTERVAL,
    catchup=False,
    tags=TAGS,
    concurrency=16,                # allow enough parallel tasks
    max_active_runs=1,                  # one run at a time
) as dag:

    @task
    def make_run_mark() -> str:
        mark = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return mark

    @task
    def ensure_clickhouse_table(mark: str):
        process = "ensure_clickhouse_table"
        log_status(process, mark, "pending")
        try:
            mysql = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
            cols = _mysql_columns(mysql)
            if not cols:
                raise RuntimeError("No columns discovered from MySQL table.")

            pk = PK_COL
            has_version = any(c["COLUMN_NAME"] == VERSION_COL for c in cols)

            col_lines = []
            for c in cols:
                name = c["COLUMN_NAME"]
                ch_type = _map_mysql_to_ch(c)
                if name == pk:
                    col_lines.append(f"`{name}` {ch_type}")                      # PK non-null
                else:
                    col_lines.append(f"`{name}` Nullable({ch_type})")            # keep everything else Nullable

            # --- NEW: add a non-nullable materialized version column if we'll use versioning
            if has_version:
                # make sure the source VERSION_COL is present above (Nullable or not)
                # add a computed non-nullable version column for the engine
                col_lines.append(
                    f"`{VERSION_COL}_v` DateTime64(6) MATERIALIZED ifNull({VERSION_COL}, toDateTime64(0, 6))"
                )
                version_clause = f"ENGINE = ReplacingMergeTree({VERSION_COL}_v)"
            else:
                version_clause = "ENGINE = ReplacingMergeTree()"

            cols_join = ",\n                ".join(col_lines)

            db_sql = f"CREATE DATABASE IF NOT EXISTS `{CLICKHOUSE_DATABASE}`"
            table_sql = f"""
            CREATE TABLE IF NOT EXISTS `{CLICKHOUSE_DATABASE}`.`{CLICKHOUSE_TABLE}`
            (
                            {cols_join}
            )
            {version_clause}
            PRIMARY KEY `{pk}`
            ORDER BY `{pk}`
            SETTINGS index_granularity = 8192
            """

            ch = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
            conn = ch.get_conn()
            conn.execute(db_sql)
            conn.execute(table_sql)

            log_status(process, mark, "success")
        except Exception as e:
            log_status(process, mark, "failed", str(e))
            raise


    @task
    def sync_clickhouse_schema(mark: str):
        process = "sync_clickhouse_schema"
        log_status(process, mark, "pending")
        try:
            mysql = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
            ch = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
            conn = ch.get_conn()

            src_cols = _mysql_columns(mysql)
            if not src_cols:
                raise RuntimeError("No columns metadata from MySQL.")

            ch_cols = _ch_columns(ch)  # {name: type}
            alters = []

            if any(c["COLUMN_NAME"] == VERSION_COL for c in src_cols):
                version_mat_col = f"{VERSION_COL}_v"
                if version_mat_col not in ch_cols:
                    alters.append(
                        f"ADD COLUMN IF NOT EXISTS `{version_mat_col}` DateTime64(6) "
                        f"MATERIALIZED ifNull({VERSION_COL}, toDateTime64(0, 6))"
                    )
                else:
                    alters.append(
                        f"MODIFY COLUMN `{version_mat_col}` DateTime64(6) "
                        f"MATERIALIZED ifNull({VERSION_COL}, toDateTime64(0, 6))"
                    )

            for stmt in alters:
                conn.execute(f"ALTER TABLE `{CLICKHOUSE_DATABASE}`.`{CLICKHOUSE_TABLE}` {stmt}")


            log_status(process, mark, "success")
        except Exception as e:
            log_status(process, mark, "failed", str(e))
            raise

    @task
    def get_last_modified_in_clickhouse(mark: str) -> Optional[str]:
        process = "get_last_modified_in_clickhouse"
        log_status(process, mark, "pending")
        try:
            last_iso = None
            ch = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
            conn = ch.get_conn()
            rows = conn.execute(
                f"SELECT max({VERSION_COL}) FROM `{CLICKHOUSE_DATABASE}`.`{CLICKHOUSE_TABLE}`"
            )
            last_val = rows[0][0] if rows and rows[0] else None
            if last_val is not None:
                last_iso = last_val.isoformat() if hasattr(last_val, "isoformat") else str(last_val)

            log_status(process, mark, "success")
            return last_iso
        except Exception as e:
            log_status(process, mark, "failed", str(e))
            raise

    @task
    def plan_chunks(mark: str, last_modified_iso: Optional[str]) -> List[Dict[str, Any]]:
        """
        Plan up to 4 chunks (offset/limit) for parallel extraction.
        Ordered by (modified, PK) to keep results stable.
        Each chunk = CHUNK_SIZE rows; we only schedule the first 4.
        """
        process = "plan_chunks"
        log_status(process, mark, "pending")
        try:
            mysql = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
            # Count eligible rows
            if last_modified_iso:
                cnt_sql = f"""
                    SELECT COUNT(*)
                    FROM `{MYSQL_DATABASE}`.`{MYSQL_TABLE}`
                    WHERE `{VERSION_COL}` > %s
                """
                total = mysql.get_first(cnt_sql, parameters=(last_modified_iso,))[0]
            else:
                cnt_sql = f"SELECT COUNT(*) FROM `{MYSQL_DATABASE}`.`{MYSQL_TABLE}`"
                total = mysql.get_first(cnt_sql)[0]

            if total == 0:
                log_status(process, mark, "success")
                return []

            # Compute up to 4 chunks
            possible_chunks = math.ceil(total / CHUNK_SIZE)
            num_chunks = min(possible_chunks, MAX_PARALLEL)

            chunks = []
            for i in range(num_chunks):
                chunks.append({
                    "chunk_index": i,
                    "offset": i * CHUNK_SIZE,
                    "limit": CHUNK_SIZE,
                    "last_modified_iso": last_modified_iso,
                })

            log_status(process, mark, "success")
            return chunks
        except Exception as e:
            log_status(process, mark, "failed", str(e))
            raise

    @task
    def extract_chunk(mark: str, chunk: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Extract one chunk using LIMIT/OFFSET on an ORDER BY (modified, PK).
        Writes NDJSON and returns metadata {file_path, columns, row_count, chunk_index}.
        """
        idx = chunk["chunk_index"]
        process = f"extract_mysql_chunk_{idx}"
        log_status(process, mark, "pending")

        try:
            last_iso = chunk["last_modified_iso"]
            offset = int(chunk["offset"])
            limit = int(chunk["limit"])

            mysql = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
            cols_meta = _mysql_columns(mysql)
            if not cols_meta:
                raise RuntimeError("No columns metadata from MySQL.")
            col_names = [f"`{c['COLUMN_NAME']}`" for c in cols_meta]

            # Stable order
            order_cols = f"`{VERSION_COL}` ASC, `{PK_COL}` ASC"

            if last_iso:
                sql = f"""
                    SELECT {", ".join(col_names)}
                    FROM `{MYSQL_DATABASE}`.`{MYSQL_TABLE}`
                    WHERE `{VERSION_COL}` > %s
                    ORDER BY {order_cols}
                    LIMIT %s OFFSET %s
                """
                params = (last_iso, limit, offset)
            else:
                sql = f"""
                    SELECT {", ".join(col_names)}
                    FROM `{MYSQL_DATABASE}`.`{MYSQL_TABLE}`
                    ORDER BY {order_cols}
                    LIMIT %s OFFSET %s
                """
                params = (limit, offset)

            engine = mysql.get_sqlalchemy_engine()
            out_path = os.path.join(
                TMP_DIR, f"{NDJSON_PREFIX}{datetime.now().strftime('%Y%m%d%H%M%S')}_chunk{idx}.ndjson"
            )

            count = 0
            with engine.connect() as conn, conn.execution_options(stream_results=True):
                result = conn.execute(sql, params)
                with open(out_path, "w", encoding="utf-8") as f:
                    while True:
                        rows = result.fetchmany(CHUNK_SIZE)  # stream inside chunk too
                        if not rows:
                            break
                        df = pd.DataFrame(rows, columns=result.keys())
                        for _, rec in df.iterrows():
                            obj = {k: (None if pd.isna(v) else str(v)) for k, v in rec.to_dict().items()}
                            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                        count += len(df)

            if count == 0:
                # nothing in this chunk
                log_status(process, mark, "success")
                return None

            payload = {
                "file_path": out_path,
                "columns": [c["COLUMN_NAME"] for c in cols_meta],
                "row_count": count,
                "chunk_index": idx,
            }
            log_status(process, mark, "success")
            return payload
        except Exception as e:
            log_status(process, mark, "failed", str(e))
            raise

    @task
    def load_chunk_to_clickhouse(mark: str, chunk_payload: Optional[Dict[str, Any]]) -> int:
        if not chunk_payload:
            log_status("load_clickhouse_chunk_skip", mark, "success")
            return 0

        idx = chunk_payload["chunk_index"]
        process = f"load_clickhouse_chunk_{idx}"
        log_status(process, mark, "pending")
        try:
            file_path = chunk_payload["file_path"]
            columns = chunk_payload["columns"]

            ch = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
            conn = ch.get_conn()

            # build per-column converters from CH types
            ch_types = _ch_columns(ch)  # {col: "Type" or "Nullable(Type)"}
            converters = [ _mk_converter(ch_types.get(col, "String")) for col in columns ]

            columns_quoted = ", ".join(f"`{c}`" for c in columns)
            insert_sql = f"INSERT INTO `{CLICKHOUSE_DATABASE}`.`{CLICKHOUSE_TABLE}` ({columns_quoted}) VALUES"

            batch, inserted = [], 0

            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    obj = json.loads(line)
                    # convert each field to expected Python type
                    row = tuple(conv(obj.get(col, None)) for conv, col in zip(converters, columns))
                    batch.append(row)
                    if len(batch) >= CHUNK_SIZE:
                        conn.execute(insert_sql, batch)
                        inserted += len(batch)
                        batch = []
            if batch:
                conn.execute(insert_sql, batch)
                inserted += len(batch)

            log_status(process, mark, "success")
            return inserted
        except Exception as e:
            log_status(process, mark, "failed", str(e))
            raise

    @task
    def summarize_load(mark: str, inserted_counts: List[int]) -> int:
        process = "summarize_load"
        log_status(process, mark, "pending")
        try:
            total = sum([c for c in inserted_counts if isinstance(c, int)])
            log_status(process, mark, "success")
            return total
        except Exception as e:
            log_status(process, mark, "failed", str(e))
            raise

    # ===== Orchestration (no iteration over XComArg) =====
    mark = make_run_mark()
    ensure = ensure_clickhouse_table(mark)
    sync = sync_clickhouse_schema(mark)
    last = get_last_modified_in_clickhouse(mark)
    chunks = plan_chunks(mark, last)

    # parallel extract → mapped
    extracted_payloads = extract_chunk.partial(mark=mark).expand(chunk=chunks)

    # parallel load → mapped (accepts None payloads)
    inserted_counts = load_chunk_to_clickhouse.partial(mark=mark).expand(
        chunk_payload=extracted_payloads
    )

    total_inserted = summarize_load(mark, inserted_counts)

    [ensure, sync] >> last >> chunks >> extracted_payloads >> inserted_counts >> total_inserted
