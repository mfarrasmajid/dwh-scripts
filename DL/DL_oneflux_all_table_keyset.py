import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from decimal import Decimal
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from croniter import croniter
from airflow.exceptions import AirflowSkipException
from MySQLdb import OperationalError as MySQLOperationalError

# ========= DAG meta =========
DAG_ID = "DL_oneflux_all_table_keyset"
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 10,
    "retry_delay": timedelta(seconds=30),
}
DAG_SCHEDULE = "* * * * *"   # tick every minute; per-row cadence handled via next_run_at
REGISTRY_CONN_ID = "airflow_logs_mitratel"  # Postgres where dwh_ingestion_registry lives
REGISTRY_TABLE = "dwh_ingestion_registry"

# === add this helper near other helpers ===
def _sync_all_columns(
    ch: ClickHouseHook,
    target_db: str,
    target_table: str,
    mysql_cols: List[Dict[str, Any]],
    pk: str,
    version_col: Optional[str]
):
    """Ensure CH has every MySQL column (nullable for non-PK), and keep version_v materialized column consistent."""
    conn_ch = ch.get_conn()
    ch_cols = _ch_columns(ch, target_db, target_table)

    alters = []

    # 1) Add any missing MySQL columns to CH
    for c in mysql_cols:
        name = c["COLUMN_NAME"]
        mapped = _map_mysql_to_ch(c)  # e.g. DateTime64(6), Decimal(20,6), String, etc.
        # PK stays NOT NULL, others are Nullable
        ch_def = f"`{name}` {mapped}" if name == pk else f"`{name}` Nullable({mapped})"
        if name not in ch_cols:
            alters.append(f"ADD COLUMN IF NOT EXISTS {ch_def}")

    # 2) Ensure/refresh version_v if needed
    has_version = bool(version_col) and any(c["COLUMN_NAME"] == version_col for c in mysql_cols)
    if has_version:
        vmat = f"{version_col}_v"
        vdef = (
            f"`{vmat}` DateTime64(6) "
            f"MATERIALIZED ifNull({version_col}, toDateTime64(0, 6))"
        )
        if vmat not in ch_cols:
            alters.append(f"ADD COLUMN IF NOT EXISTS {vdef}")
        else:
            alters.append(f"MODIFY COLUMN {vdef}")

    # 3) Apply ALTERs
    for stmt in alters:
        conn_ch.execute(f"ALTER TABLE `{target_db}`.`{target_table}` {stmt}")

    # Return fresh map
    return _ch_columns(ch, target_db, target_table)

def _ch_insertable_columns(ch, db: str, table: str):
    conn = ch.get_conn()
    def _lit(s: str) -> str: return s.replace("'", "\\'")
    rows = conn.execute(f"""
        SELECT name, type, COALESCE(default_kind, '')
        FROM system.columns
        WHERE database='{_lit(db)}' AND table='{_lit(table)}'
        ORDER BY position
    """)
    cols, types = [], {}
    for name, t, kind in rows:
        types[name] = t
        k = (kind or '').upper()
        if k not in ('MATERIALIZED', 'ALIAS'):
            cols.append(name)
    return cols, types

def _is_mysql_quota_error(e: Exception) -> bool:
    return isinstance(e, MySQLOperationalError) and getattr(e, "args", None) and e.args[0] in (1226, 1203)

def _mark_success_and_bump(job: dict, proc: str):
    # close pending log as success
    _log_status(job, proc, job["_current_mark"], "success")
    # bump registry as success, schedule next
    pg = PostgresHook(postgres_conn_id=REGISTRY_CONN_ID)
    now = datetime.now(timezone.utc)
    next_run = _compute_next_run(job, now)
    pg.run(
        f"""
        UPDATE {REGISTRY_TABLE}
        SET last_run_at=%s, next_run_at=%s, last_status=%s, last_error=%s
        WHERE id=%s
        """,
        parameters=(now.isoformat(), next_run.isoformat(), 'success', None, job["id"])
    )

def _dt_to_iso(v: Any):
    if isinstance(v, datetime):
        # convert tz-aware/naive to ISO8601 string (keeps 'Z' offset if present)
        return v.isoformat()
    return v

def _sanitize_job_row(d: dict) -> dict:
    # Convert any datetime-like fields to ISO strings to make XCom JSON-safe
    for k in ("last_watermark", "next_run_at", "last_run_at"):
        if k in d:
            d[k] = _dt_to_iso(d[k])
    return d

# ========= Helpers =========
def _base_ch_type(t: str) -> str:
    t = t.strip()
    if t.startswith("Nullable(") and t.endswith(")"):
        return t[9:-1]
    return t

def _mk_converter(ch_type: str):
    import pandas as pd
    t = _base_ch_type(ch_type)
    if t.startswith("DateTime64") or t == "DateTime":
        def conv(v):
            if v is None or v == "": return None
            ts = pd.to_datetime(v, errors="coerce")
            if pd.isna(ts): return None
            py = ts.to_pydatetime()
            if getattr(py, "tzinfo", None) is not None: py = py.replace(tzinfo=None)
            return py
        return conv
    if t == "Date":
        def conv(v):
            if v is None or v == "": return None
            ts = pd.to_datetime(v, errors="coerce")
            if ts is None or str(ts) == "NaT": return None
            return ts.date()
        return conv
    if t in ("Int8","Int16","Int32","Int64"):
        return (lambda v: None if v in (None, "") else int(v))
    if t in ("Float32","Float64"):
        return (lambda v: None if v in (None, "") else float(v))
    if t.startswith("Decimal("):
        return (lambda v: None if v in (None, "") else Decimal(str(v)))
    return (lambda v: None if v in (None, "") else str(v))

def _mysql_columns(mysql: MySqlHook, db: str, table: str):
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
        for row in mysql.get_records(sql, parameters=(db, table))
    ]

def _map_mysql_to_ch(col: Dict[str, Any]) -> str:
    t = (col["DATA_TYPE"] or "").lower()
    if t in ("varchar","char","text","tinytext","mediumtext","longtext","json","enum","set"):
        return "String"
    if t in ("datetime","timestamp"):
        precision = int(col.get("DATETIME_PRECISION") or 6)
        precision = max(0, min(6, precision))
        return f"DateTime64({precision})" if precision > 0 else "DateTime"
    if t in ("date",): return "Date"
    if t in ("time",): return "String"
    if t in ("tinyint",): return "Int8"
    if t in ("smallint",): return "Int16"
    if t in ("mediumint","int","integer"): return "Int32"
    if t in ("bigint",): return "Int64"
    if t in ("decimal","numeric"):
        prec = int(col.get("NUMERIC_PRECISION") or 38)
        scale = int(col.get("NUMERIC_SCALE") or 9)
        prec = max(1, min(76, prec)); scale = max(0, min(prec - 1, scale))
        return f"Decimal({prec},{scale})"
    if t in ("float",): return "Float32"
    if t in ("double","real","double precision"): return "Float64"
    return "String"

def _ch_columns(ch: ClickHouseHook, db: str, table: str) -> Dict[str, str]:
    conn = ch.get_conn()
    def _lit(s: str) -> str: return s.replace("'", "\\'")
    rows = conn.execute(f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{_lit(db)}'
          AND table    = '{_lit(table)}'
        ORDER BY position
    """)
    return {name: t for (name, t) in rows}

def _is_due(row: Dict[str, Any], now: datetime) -> bool:
    if not row["enabled"]: return False
    nxt = row.get("next_run_at")
    if nxt and now < nxt: return False
    return True

def _compute_next_run(row: Dict[str, Any], base: datetime) -> datetime:
    stype = (row.get("schedule_type") or "interval").lower()
    if stype == "interval":
        minutes = row.get("interval_minutes") or 15
        return base + timedelta(minutes=minutes)
    expr = row.get("cron_expr") or "*/15 * * * *"
    itr = croniter(expr, base)
    return itr.get_next(datetime)

def _log_status(job: Dict[str, Any], process_name: str, mark: str, status: str, error_message: Optional[str] = None):
    pg_hook = PostgresHook(postgres_conn_id=job["pg_log_conn_id"])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dag_name = f"{job['target_db']}.{job['target_table']}"
    log_table = job["log_table"]
    if status == "pending":
        cursor.execute(
            f"""
            INSERT INTO {log_table} (process_name, dag_name, type, status, start_time, mark, kategori)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (process_name, dag_name, job["log_type"], status, now, mark, job["log_kategori"]),
        )
    else:
        cursor.execute(
            f"""
            UPDATE {log_table}
            SET status = %s, end_time = %s, error_message = %s
            WHERE process_name = %s AND status = 'pending' AND dag_name = %s AND mark = %s AND kategori = %s
            """,
            (status, now, error_message, process_name, dag_name, mark, job["log_kategori"]),
        )
    conn.commit()
    cursor.close(); conn.close()

# ========= DAG =========
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE,
    catchup=False,
    tags=["dl","mariadb","clickhouse","oneflux","metadata","keyset","replacingmergetree"],
    concurrency=64,
    max_active_runs=1,
) as dag:

    @task
    def fetch_due_jobs() -> List[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        pg = PostgresHook(postgres_conn_id=REGISTRY_CONN_ID)
        rows = pg.get_records(f"""
            SELECT
            id, enabled,
            source_mysql_conn_id, target_ch_conn_id, pg_log_conn_id,
            source_db, source_table, target_db, target_table,
            pk_col, version_col,
            schedule_type, interval_minutes, cron_expr,
            chunk_rows, max_parallel, tmp_dir, ndjson_prefix,
            log_table, log_type, log_kategori,
            last_watermark, next_run_at, last_run_at
            FROM {REGISTRY_TABLE}
            WHERE enabled = true
            AND (next_run_at IS NULL OR next_run_at <= now())
            ORDER BY COALESCE(next_run_at, now()) ASC, id ASC
            LIMIT 20
        """)
        cols = [
            "id","enabled",
            "source_mysql_conn_id","target_ch_conn_id","pg_log_conn_id",
            "source_db","source_table","target_db","target_table",
            "pk_col","version_col",
            "schedule_type","interval_minutes","cron_expr",
            "chunk_rows","max_parallel","tmp_dir","ndjson_prefix",
            "log_table","log_type","log_kategori",
            "last_watermark","next_run_at","last_run_at"
        ]
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(zip(cols, r))
            if _is_due(d, now):
                out.append(_sanitize_job_row(d))
        return out

    @task
    def assert_has_due_jobs(due_jobs):
        if not due_jobs:
            raise AirflowSkipException("No jobs due this tick.")
        return True

    @task(trigger_rule="none_failed")
    def run_one_job(job: Dict[str, Any]) -> int:
        mark = f"{job['id']}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        job["_current_mark"] = mark  # <-- so helpers can access the mark
        inserted_total = 0

        def ok(proc): _log_status(job, proc, mark, "success")
        def pend(proc): _log_status(job, proc, mark, "pending")
        def fail(proc, e): _log_status(job, proc, mark, "failed", str(e))

        try:
            # 1) Ensure table (this step needs MySQL for column list)
            proc = f"{job['id']}_ensure_clickhouse_table"; pend(proc)
            mysql = MySqlHook(mysql_conn_id=job["source_mysql_conn_id"])
            try:
                cols = _mysql_columns(mysql, job["source_db"], job["source_table"])
            except MySQLOperationalError as e:
                if _is_mysql_quota_error(e):
                    _mark_success_and_bump(job, proc)
                    return 0  # treat as success/skip
                raise
            if not cols:
                raise RuntimeError("No columns discovered from MySQL table.")

            pk = job["pk_col"]; version_col = job.get("version_col")
            col_lines = []
            for c in cols:
                name = c["COLUMN_NAME"]; ch_type = _map_mysql_to_ch(c)
                col_lines.append(f"`{name}` {ch_type}" if name == pk else f"`{name}` Nullable({ch_type})")
            has_version = bool(version_col) and any(c["COLUMN_NAME"] == version_col for c in cols)
            if has_version:
                col_lines.append(
                    f"`{version_col}_v` DateTime64(6) MATERIALIZED ifNull({version_col}, toDateTime64(0, 6))"
                )
                engine_clause = f"ENGINE = ReplacingMergeTree({version_col}_v)"
            else:
                engine_clause = "ENGINE = ReplacingMergeTree()"

            cols_join = ",\n                ".join(col_lines)
            db_sql = f"CREATE DATABASE IF NOT EXISTS `{job['target_db']}`"
            table_sql = f"""
            CREATE TABLE IF NOT EXISTS `{job['target_db']}`.`{job['target_table']}`
            (
                {cols_join}
            )
            {engine_clause}
            ORDER BY `{pk}`
            SETTINGS index_granularity = 8192
            """
            ch = ClickHouseHook(clickhouse_conn_id=job["target_ch_conn_id"])
            conn_ch = ch.get_conn()
            conn_ch.execute(db_sql); conn_ch.execute(table_sql)
            ok(proc)

            # 2) Sync schema
            proc = f"{job['id']}_sync_clickhouse_schema"; pend(proc)
            ch_cols = _sync_all_columns(
                ch=ch,
                target_db=job["target_db"],
                target_table=job["target_table"],
                mysql_cols=cols,
                pk=pk,
                version_col=version_col
            )
            ok(proc)

            # 3) Get last modified in CH
            proc = f"{job['id']}_get_last_modified_in_clickhouse"; pend(proc)
            last_iso = None
            if has_version:
                rows = conn_ch.execute(
                    f"SELECT max({version_col}) FROM `{job['target_db']}`.`{job['target_table']}`"
                )
                last_val = rows[0][0] if rows and rows[0] else None
                if last_val is not None:
                    last_iso = last_val.isoformat() if hasattr(last_val, "isoformat") else str(last_val)
            ok(proc)

            # 4) Extract & load
            proc = f"{job['id']}_extract_load"; pend(proc)
            try:
                conn_my = mysql.get_conn()
            except MySQLOperationalError as e:
                if _is_mysql_quota_error(e):
                    _mark_success_and_bump(job, proc)
                    return 0
                raise

            cur = conn_my.cursor()
            try:
                cur.execute("SET SESSION net_read_timeout=900, net_write_timeout=900")
                cur.execute("SET SESSION wait_timeout=7200, interactive_timeout=7200")
            except Exception:
                pass

            db, tbl = job["source_db"], job["source_table"]
            chunk_rows = int(job.get("chunk_rows") or 10000)
            # re-use cols (we already fetched); but if you want fresh:
            # wrap again with quota guard:
            # try: cols_meta = _mysql_columns(mysql, db, tbl)
            # except MySQLOperationalError as e:
            #     if _is_mysql_quota_error(e):
            #         _mark_success_and_bump(job, proc); return 0
            #     raise
            cols_meta = cols
            col_names = [c["COLUMN_NAME"] for c in cols_meta]
            select_cols = ", ".join(f"`{c}`" for c in col_names)

            mysql_type_map = {c["COLUMN_NAME"]: (c["DATA_TYPE"] or "").lower() for c in cols_meta}
            pk_mysql_type = mysql_type_map.get(pk, "")
            _STRING_TYPES = {"varchar","char","text","tinytext","mediumtext","longtext","json","enum","set"}
            pk_is_string = pk_mysql_type in _STRING_TYPES
            CAST_PK_NUMERIC = False

            # ch_types = _ch_columns(ch, job["target_db"], job["target_table"])
            # col_names_insert = [c for c in col_names if c in ch_types]
            col_names_insert, ch_types = _ch_insertable_columns(ch, job["target_db"], job["target_table"])
            converters = [_mk_converter(ch_types.get(col, "String")) for col in col_names_insert]
            quoted_cols = ", ".join(f"`{c}`" for c in col_names_insert)
            insert_sql = f"INSERT INTO `{job['target_db']}`.`{job['target_table']}` ({quoted_cols}) VALUES"

            cursor_pk = None
            cursor_ver = None
            if has_version and last_iso:
                cursor_ver = last_iso
                cursor_pk = "" if pk_is_string else 0

            if has_version:
                if CAST_PK_NUMERIC:
                    order_by = f"`{version_col}` ASC, CAST(`{pk}` AS UNSIGNED) ASC"
                elif pk_is_string:
                    order_by = f"`{version_col}` ASC, `{pk}` COLLATE utf8mb4_bin ASC"
                else:
                    order_by = f"`{version_col}` ASC, `{pk}` ASC"
            else:
                if CAST_PK_NUMERIC:
                    order_by = f"CAST(`{pk}` AS UNSIGNED) ASC"
                elif pk_is_string:
                    order_by = f"`{pk}` COLLATE utf8mb4_bin ASC"
                else:
                    order_by = f"`{pk}` ASC"

            while True:
                params = []
                if has_version:
                    if cursor_ver is None or cursor_pk is None:
                        where_sql = "1=1"
                    else:
                        if CAST_PK_NUMERIC:
                            where_sql = f"(`{version_col}`, CAST(`{pk}` AS UNSIGNED)) > (%s, %s)"
                        elif pk_is_string:
                            where_sql = f"(`{version_col}`, `{pk}` COLLATE utf8mb4_bin) > (%s, %s)"
                        else:
                            where_sql = f"(`{version_col}`, `{pk}`) > (%s, %s)"
                        params.extend([cursor_ver, cursor_pk])
                else:
                    if cursor_pk is None:
                        where_sql = "1=1"
                    else:
                        if CAST_PK_NUMERIC:
                            where_sql = f"CAST(`{pk}` AS UNSIGNED) > %s"
                        elif pk_is_string:
                            where_sql = f"`{pk}` COLLATE utf8mb4_bin > %s"
                        else:
                            where_sql = f"`{pk}` > %s"
                        params.append(cursor_pk)

                sql = f"""
                    SELECT {select_cols}
                    FROM `{db}`.`{tbl}`
                    WHERE {where_sql}
                    ORDER BY {order_by}
                    LIMIT %s
                """
                params.append(chunk_rows)

                try:
                    cur.execute(sql, params)
                except MySQLOperationalError as e:
                    if _is_mysql_quota_error(e):
                        _mark_success_and_bump(job, proc)
                        try: cur.close(); conn_my.close()
                        except Exception: pass
                        return 0
                    raise

                rows = cur.fetchall()
                if not rows:
                    break

                src_index = {name: idx for idx, name in enumerate(col_names)}
                batch = []
                for r in rows:
                    vals = []
                    for cname, conv in zip(col_names_insert, converters):
                        vals.append(conv(r[src_index[cname]]))
                    batch.append(tuple(vals))

                conn_ch.execute(insert_sql, batch, types_check=True)
                inserted_total += len(batch)

                last_row = rows[-1]
                pk_idx = col_names.index(pk)
                cursor_pk = last_row[pk_idx]
                if has_version:
                    ver_idx = col_names.index(version_col)
                    cursor_ver = last_row[ver_idx]

            try:
                cur.close(); conn_my.close()
            except Exception:
                pass
            ok(proc)

            # 5) Summarize
            proc = f"{job['id']}_summarize"; pend(proc)
            pg = PostgresHook(postgres_conn_id=REGISTRY_CONN_ID)
            now = datetime.now(timezone.utc)
            next_run = _compute_next_run(job, now)
            pg.run(
                f"""
                UPDATE {REGISTRY_TABLE}
                SET last_run_at=%s, next_run_at=%s, last_status=%s, last_error=%s
                WHERE id=%s
                """,
                parameters=(now.isoformat(), next_run.isoformat(), 'success', None, job["id"])
            )
            ok(proc)
            return inserted_total

        except Exception as e:
            fail(proc, e)
            try:
                pg = PostgresHook(postgres_conn_id=REGISTRY_CONN_ID)
                now = datetime.now(timezone.utc)
                safe_next = now + timedelta(minutes=5)
                pg.run(
                    f"""
                    UPDATE {REGISTRY_TABLE}
                    SET last_run_at=%s, next_run_at=%s, last_status='failed', last_error=%s
                    WHERE id=%s
                    """,
                    parameters=(now.isoformat(), safe_next.isoformat(), str(e)[:2000], job["id"])
                )
            finally:
                pass
            raise

    # --------- Wiring (single level of mapping) ---------
    due_jobs = fetch_due_jobs()
    gate = assert_has_due_jobs(due_jobs)
    results = run_one_job.expand(job=due_jobs)
    gate >> results
