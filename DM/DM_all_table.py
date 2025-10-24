# dags/DM_all_table.py
from __future__ import annotations

import math
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone, date
from typing import List, Dict, Any, Optional, Tuple

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.exceptions import AirflowSkipException
from croniter import croniter

# ---- Python logging
import logging
logger = logging.getLogger("ck2pg_upsert")
if not logger.handlers:
    handler = logging.StreamHandler()
    fmt = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
    handler.setFormatter(fmt)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ========= DAG meta =========
DAG_ID = "DM_all_table"
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}
DAG_SCHEDULE = "* * * * *"
REGISTRY_CONN_ID = "airflow_logs_mitratel"
REGISTRY_TABLE = "public.ck2pg_ingestion_registry"
SAFE_LOOKBACK_MINUTES_DEFAULT = 1440
SKEW_TOLERANCE_MINUTES_DEFAULT = 10

# ========= Utilities =========
def _dt_to_iso(v):
    return v.isoformat() if isinstance(v, datetime) else v

def _sanitize_job_row(d: dict) -> dict:
    for k in ("next_run_at", "last_run_at"):
        if k in d:
            d[k] = _dt_to_iso(d[k])
    return d

def _is_due(row: Dict[str, Any], now: datetime) -> bool:
    return bool(row["enabled"]) and (not row.get("next_run_at") or now >= row["next_run_at"])

def _compute_next_run(row: Dict[str, Any], base: datetime) -> datetime:
    stype = (row.get("schedule_type") or "interval").lower()
    if stype == "interval":
        minutes = row.get("interval_minutes") or 30
        return base + timedelta(minutes=int(minutes))
    expr = row.get("cron_expr") or "*/30 * * * *"
    return croniter(expr, base).get_next(datetime)

def _log_status(job: Dict[str, Any], process_name: str, mark: str, status: str, error_message: Optional[str] = None):
    pg_hook = PostgresHook(postgres_conn_id=job["pg_log_conn_id"])
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dag_name = f"{job['target_schema']}.{job['target_table']}"
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

# ========= ClickHouse helpers (query-only mode) =========
def _ch_ident(name: str) -> str:
    return f"`{name.replace('`','``')}`"

def _ch_lit(v) -> str:
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        return '1' if v else '0'
    if isinstance(v, (int, float)):
        return str(v)
    if isinstance(v, datetime):
        return f"toDateTime64('{v.strftime('%Y-%m-%d %H:%M:%S.%f')}', 6)" if v.microsecond else f"toDateTime('{v.strftime('%Y-%m-%d %H:%M:%S')}')"
    if isinstance(v, date):
        return f"toDate('{v.strftime('%Y-%m-%d')}')"
    s = str(v).replace("'", "\\'")
    return f"'{s}'"

def _ch_columns_from_query(ch: ClickHouseHook, source_sql: str) -> List[Tuple[str, str]]:
    conn = ch.get_conn()
    wrapped = f"SELECT * FROM ({source_sql}) AS sub LIMIT 0"
    _data, columns = conn.execute(wrapped, with_column_types=True)  # type: ignore
    out = []
    for col in columns:
        name = col[0]
        ch_type = col[1]
        out.append((name, ch_type))
    return out

def _base_ch_type(t: str) -> str:
    t = t.strip()
    if t.startswith("Nullable(") and t.endswith(")"):
        return t[9:-1]
    if t.startswith("LowCardinality(") and t.endswith(")"):
        return t[15:-1]
    return t

def _map_ch_to_pg(ch_type: str) -> str:
    t = _base_ch_type(ch_type)
    if re.match(r"^U?Int8$", t, re.I):  return "SMALLINT"
    if re.match(r"^U?Int16$", t, re.I): return "INTEGER"
    if re.match(r"^U?Int32$", t, re.I): return "BIGINT"
    if re.match(r"^U?Int64$", t, re.I): return "NUMERIC(20,0)"
    if re.match(r"^Int\d+$", t, re.I):
        return {"Int8": "SMALLINT", "Int16": "SMALLINT", "Int32": "INTEGER", "Int64": "BIGINT"}.get(t, "BIGINT")
    if re.match(r"^Float32$", t, re.I): return "REAL"
    if re.match(r"^Float64$", t, re.I): return "DOUBLE PRECISION"
    m = re.match(r"^Decimal\((\d+),\s*(\d+)\)$", t, re.I)
    if m: return f"NUMERIC({m.group(1)},{m.group(2)})"
    if re.match(r"^Date(32)?$", t, re.I):  return "DATE"
    if re.match(r"^DateTime", t, re.I):    return "TIMESTAMP"
    if re.match(r"^String$", t, re.I):     return "TEXT"
    if re.match(r"^FixedString\(\d+\)$", t, re.I): return "TEXT"
    if re.match(r"^UUID$", t, re.I):      return "UUID"
    if re.match(r"^(Array|Map|Tuple)\(", t, re.I): return "JSONB"
    if re.match(r"^JSON$", t, re.I):      return "JSONB"
    if re.match(r"^Bool$", t, re.I):      return "BOOLEAN"
    if re.match(r"^IPv[46]$", t, re.I):   return "INET"
    return "TEXT"

def _pg_type_map_from_ch(ch_cols: List[Tuple[str, str]]) -> Dict[str, str]:
    return {n: _map_ch_to_pg(t) for (n, t) in ch_cols}

# ========= Postgres helpers =========
def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def _ensure_pg_schema(pg: PostgresHook, schema: str):
    pg.run(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(schema)}")

def _pg_table_exists(pg: PostgresHook, schema: str, table: str) -> bool:
    sql = """
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = %s AND table_name = %s
    """
    return bool(pg.get_first(sql, parameters=(schema, table)))

def _pg_columns(pg: PostgresHook, schema: str, table: str) -> Dict[str, str]:
    sql = """
    SELECT column_name, data_type, udt_name, character_maximum_length,
           numeric_precision, numeric_scale
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    out = {}
    for col, data_type, udt, charlen, prec, scale in pg.get_records(sql, parameters=(schema, table)):
        typ = data_type.upper()
        if typ == "NUMERIC" and prec and scale is not None:
            typ = f"NUMERIC({prec},{scale})"
        out[col] = typ
    return out

def _pg_current_pk_cols(pg: PostgresHook, schema: str, table: str) -> List[str]:
    sql = """
    SELECT kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu
      ON tc.constraint_name = kcu.constraint_name
     AND tc.table_schema = kcu.table_schema
    WHERE tc.constraint_type = 'PRIMARY KEY'
      AND tc.table_schema = %s
      AND tc.table_name   = %s
    ORDER BY kcu.ordinal_position
    """
    rows = pg.get_records(sql, parameters=(schema, table))
    return [r[0] for r in rows]

def _create_pg_table(pg: PostgresHook, schema: str, table: str,
                     ch_cols: List[Tuple[str, str]], pk_cols: List[str]):
    # Drop internal/system/projection helper columns (e.g., __modified_v)
    ch_cols = [(n, t) for (n, t) in ch_cols if not n.startswith("__")]
    cols_sql = [f"{_quote_ident(n)} {_map_ch_to_pg(t)}" for n, t in ch_cols]
    pk_clause = f", PRIMARY KEY ({', '.join(_quote_ident(c) for c in pk_cols)})" if pk_cols else ""
    sql = f"""CREATE TABLE IF NOT EXISTS {_quote_ident(schema)}.{_quote_ident(table)} ({", ".join(cols_sql)}{pk_clause})"""
    pg.run(sql)

def _ensure_pg_pk(pg: PostgresHook, schema: str, table: str, pk_cols: List[str]):
    if not pk_cols: return
    if _pg_current_pk_cols(pg, schema, table): return
    pg.run(f'ALTER TABLE {_quote_ident(schema)}.{_quote_ident(table)} '
           f'ADD PRIMARY KEY ({", ".join(_quote_ident(c) for c in pk_cols)})')

def _sync_pg_schema(pg: PostgresHook, schema: str, table: str,
                    ch_cols: List[Tuple[str, str]], drop_extra: bool,
                    pk_cols: List[str]):
    ch_cols = [(n, t) for (n, t) in ch_cols if not n.startswith("__")]
    pg_cols = _pg_columns(pg, schema, table)
    src_names = [c[0] for c in ch_cols]
    src_map = {c[0]: _map_ch_to_pg(c[1]) for c in ch_cols}
    for name in src_names:
        if name not in pg_cols:
            pg.run(f'ALTER TABLE {_quote_ident(schema)}.{_quote_ident(table)} '
                   f'ADD COLUMN {_quote_ident(name)} {src_map[name]}')
    if drop_extra:
        for name in list(pg_cols.keys()):
            if name not in src_names:
                pg.run(f'ALTER TABLE {_quote_ident(schema)}.{_quote_ident(table)} '
                       f'DROP COLUMN {_quote_ident(name)}')
    _ensure_pg_pk(pg, schema, table, pk_cols)

def _iter_slices(seq, size: int):
    n = len(seq)
    for i in range(0, n, size):
        yield seq[i:i+size]

# --------- UPSERT SQL helpers ----------
def _build_projection_with_casts(columns: List[str], pg_type_by_col: Dict[str, str]) -> List[str]:
    proj = []
    for c in columns:
        pg_t = (pg_type_by_col.get(c) or "TEXT").upper()
        qc = f'v."{c}"'
        if pg_t.startswith("TIMESTAMP"):
            expr = f"CAST({qc} AS TIMESTAMP)"
        elif pg_t == "DATE":
            expr = f"CAST({qc} AS DATE)"
        elif pg_t.startswith("NUMERIC"):
            expr = f"CAST({qc} AS NUMERIC)"
        elif pg_t in ("DOUBLE PRECISION", "REAL", "BIGINT", "INTEGER", "SMALLINT", "BOOLEAN", "UUID", "INET"):
            expr = f"CAST({qc} AS {pg_t})"
        elif pg_t == "JSONB":
            expr = f"CAST({qc} AS JSONB)"
        else:
            expr = qc
        proj.append(expr + f' AS "{c}"')
    return proj

# --------- UPSERT executor ----------
from psycopg2.extras import execute_values

def _build_upsert_sql(schema: str, table: str, cols: List[str], pk_cols: List[str],
                      version_col: Optional[str]) -> str:
    col_list = ", ".join(f'"{c}"' for c in cols)
    if not pk_cols:
        return f'INSERT INTO "{schema}"."{table}" ({col_list}) VALUES %s'
    conflict_cols = ", ".join(f'"{c}"' for c in pk_cols)
    non_pk = [c for c in cols if c not in pk_cols]
    distinct_on = ", ".join(f'v."{c}"' for c in pk_cols)
    order_by_elems = [f'v."{c}"' for c in pk_cols]
    if version_col and version_col in cols:
        order_by_elems.append(f'v."{version_col}" DESC NULLS LAST')
    select_cols = ", ".join(f'v."{c}"' for c in cols)
    dedup = (
        f'SELECT DISTINCT ON ({distinct_on}) {select_cols} '
        f'FROM (VALUES %s) AS v ({col_list}) '
        f'ORDER BY {", ".join(order_by_elems)}'
    )
    base = f'INSERT INTO "{schema}"."{table}" ({col_list}) {dedup}'
    if not non_pk:
        return f"{base} ON CONFLICT ({conflict_cols}) DO NOTHING"
    set_list = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk)
    tgtv = f'"{version_col}"' if version_col and version_col in cols else None
    guard = ""
    if tgtv:
        tgt = f'"{schema}"."{table}"'
        guard = (f' WHERE EXCLUDED.{tgtv} IS NOT NULL '
                 f'AND ({tgt}.{tgtv} IS NULL OR EXCLUDED.{tgtv} >= {tgt}.{tgtv})')
    return f"{base} ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_list}{guard}"

def _upsert_rows_pg(
    pg_conn,
    schema: str,
    table: str,
    columns: list,
    rows: list,
    conflict_cols: list,
    pg_type_by_col: Dict[str, str],
    version_col: Optional[str] = None,
    page_size: int = 20000,
):
    if not rows:
        return
    columns = [c for c in columns if not c.startswith("__")]
    conflict_cols = [c for c in conflict_cols if c in columns]
    qcols = [f'"{c}"' for c in columns]
    cols_csv = ", ".join(qcols)
    conflict_csv = ", ".join(f'"{c}"' for c in conflict_cols)
    select_proj = _build_projection_with_casts(columns, pg_type_by_col)
    select_csv = ", ".join(select_proj)
    if version_col and version_col in columns:
        order_by = ", ".join([f'v."{c}"' for c in conflict_cols] + [f'v."{version_col}" DESC NULLS LAST'])
    else:
        order_by = ", ".join(f'v."{c}"' for c in conflict_cols) if conflict_cols else "1"
    set_list = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in columns if c not in conflict_cols)
    tgt = f'"{schema}"."{table}"'
    diff_pred = " OR ".join(
        f'{tgt}."{c}" IS DISTINCT FROM EXCLUDED."{c}"'
        for c in columns if c not in conflict_cols
    )
    where_guard = f" WHERE {diff_pred}" if diff_pred else ""
    sql = f'''
        INSERT INTO "{schema}"."{table}" ({cols_csv})
        SELECT DISTINCT ON ({conflict_csv}) {select_csv}
        FROM (VALUES %s) AS v ({cols_csv})
        ORDER BY {order_by}
        ON CONFLICT ({conflict_csv}) DO UPDATE
        SET {set_list}
        {where_guard};
    '''
    with pg_conn.cursor() as cur:
        cur.execute("SET LOCAL synchronous_commit = off")
        execute_values(cur, sql, rows, page_size=page_size)

# ========= DAG =========
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE,
    catchup=False,
    tags=["dl", "clickhouse", "postgres", "keyset", "upsert", "sql-only", "single-batch"],
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
              source_ch_conn_id,
              source_sql,
              pk_value,
              target_pg_conn_id, target_schema, target_table,
              cursor_col,
              schedule_type, interval_minutes, cron_expr,
              chunk_rows, max_parallel, drop_extra_columns,
              copy_timeout_seconds,
              insert_page_size,
              commit_every_chunks,
              pg_log_conn_id, log_table, log_type, log_kategori,
              next_run_at, last_run_at, last_status, last_error
            FROM {REGISTRY_TABLE}
            WHERE enabled = TRUE
              AND (next_run_at IS NULL OR next_run_at <= NOW())
            ORDER BY COALESCE(next_run_at, NOW()) ASC, id ASC
            LIMIT 20
        """)
        cols = [
            "id","enabled",
            "source_ch_conn_id",
            "source_sql","pk_value",
            "target_pg_conn_id","target_schema","target_table",
            "cursor_col",
            "schedule_type","interval_minutes","cron_expr",
            "chunk_rows","max_parallel","drop_extra_columns",
            "copy_timeout_seconds",
            "insert_page_size","commit_every_chunks",
            "pg_log_conn_id","log_table","log_type","log_kategori",
            "next_run_at","last_run_at","last_status","last_error"
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
        inserted_total = 0

        def ok(proc): _log_status(job, proc, mark, "success")
        def pend(proc): _log_status(job, proc, mark, "pending")
        def fail(proc, e): _log_status(job, proc, mark, "failed", str(e))

        proc = "init"
        try:
            job_start = time.time()

            # 1) Discover source schema
            proc = f"{job['id']}_discover_source_schema"; pend(proc)
            ch_probe = ClickHouseHook(clickhouse_conn_id=job["source_ch_conn_id"])
            src_sql = (job.get("source_sql") or "").strip()
            if not src_sql:
                raise RuntimeError("source_sql is required in query-only mode.")
            t0 = time.time()
            ch_cols_raw = _ch_columns_from_query(ch_probe, src_sql)
            if not ch_cols_raw:
                raise RuntimeError("No columns discovered from source_sql.")
            # strip internal cols
            ch_cols = [(n, t) for (n, t) in ch_cols_raw if not n.startswith("__")]
            cols = [c[0] for c in ch_cols]
            pg_type_by_col = _pg_type_map_from_ch(ch_cols)

            pk_cols = [c.strip() for c in (job.get("pk_value") or "").split(",") if c.strip()]
            pk_cols = [c for c in pk_cols if c in cols]

            cursor_col = (job.get("cursor_col") or "").strip() or None
            if cursor_col and cursor_col not in cols:
                raise RuntimeError(f"cursor_col '{cursor_col}' must be present in SELECT columns")

            logger.info("JOB %s: query-only. Found %d cols. PK cols=%s", job["id"], len(cols), pk_cols or "[]")
            logger.info("Discover schema took %.2fs", time.time() - t0)
            ok(proc)

            # 2) Ensure target schema/table + sync
            proc = f"{job['id']}_ensure_target_schema_table"; pend(proc)
            t0 = time.time()
            pg = PostgresHook(postgres_conn_id=job["target_pg_conn_id"])
            _ensure_pg_schema(pg, job["target_schema"])
            if not _pg_table_exists(pg, job["target_schema"], job["target_table"]):
                _create_pg_table(pg, job["target_schema"], job["target_table"], ch_cols, pk_cols)
                logger.info("Created target table %s.%s", job["target_schema"], job["target_table"])
            else:
                _sync_pg_schema(pg, job["target_schema"], job["target_table"], ch_cols, bool(job["drop_extra_columns"]), pk_cols)
                logger.info("Synced target table %s.%s (drop_extra=%s)",
                            job["target_schema"], job["target_table"], bool(job["drop_extra_columns"]))
            logger.info("Ensure/sync took %.2fs", time.time() - t0)
            ok(proc)

            # 3) Single-batch Extract & Upsert
            proc = f"{job['id']}_extract_upsert"; pend(proc)
            chunk_rows = int(job.get("chunk_rows") or 10000)
            workers = 1  # <<<<<< enforce single-batch, single-writer per run
            insert_page_size = int(job.get("insert_page_size") or job.get("chunk_rows") or 5000)
            insert_page_size = max(100, min(insert_page_size, 20000))
            commit_every = max(1, int(job.get("commit_every_chunks") or 10))

            select_cols = ", ".join(_ch_ident(c) for c in cols)
            base_from = f"FROM ({src_sql}) AS sub"

            logger.info(
                "Extract/Upsert (single-batch) params: chunk_rows=%d, page_size=%d, commit_every_chunks=%d, cursor_col=%r",
                chunk_rows, insert_page_size, commit_every, cursor_col
            )

            conn_ch = ClickHouseHook(clickhouse_conn_id=job["source_ch_conn_id"]).get_conn()
            writer_pg = PostgresHook(postgres_conn_id=job["target_pg_conn_id"])
            conn_pg = writer_pg.get_conn()
            with conn_pg.cursor() as _cur:
                _cur.execute("SET LOCAL synchronous_commit = off")

            try:
                # Build ONE query only
                where_sql = ""
                order_sql = ""

                if cursor_col:
                    # Registry-driven overrides (optional: add these columns in registry to tune per job)
                    lookback_minutes = int(job.get("lookback_minutes") or SAFE_LOOKBACK_MINUTES_DEFAULT)
                    skew_tol_minutes = int(job.get("skew_tolerance_minutes") or SKEW_TOLERANCE_MINUTES_DEFAULT)

                    # 1) read watermark from target
                    wm_row = writer_pg.get_first(
                        f'SELECT max("{cursor_col}") FROM "{job["target_schema"]}"."{job["target_table"]}"'
                    )
                    wm = wm_row[0] if wm_row else None

                    # (NEW) detect if target is empty
                    has_rows_row = writer_pg.get_first(
                        f'SELECT EXISTS (SELECT 1 FROM "{job["target_schema"]}"."{job["target_table"]}" LIMIT 1)'
                    )
                    is_target_empty = bool(has_rows_row and has_rows_row[0])

                    # 2) probe source bounds
                    src_bounds_sql = f"SELECT min({_ch_ident(cursor_col)}), max({_ch_ident(cursor_col)}) {base_from}"
                    src_min, src_max = conn_ch.execute(src_bounds_sql)[0]

                    # 3) normalize watermark
                    now_utc = datetime.utcnow()
                    if isinstance(wm, datetime) and wm > (now_utc + timedelta(minutes=skew_tol_minutes)):
                        wm = now_utc

                    # 4) compute lower bound
                    if is_target_empty:
                        # INITIAL FULL LOAD: no lower bound (pull from the very beginning)
                        lower_bound = None
                        # (optional) for strict determinism, start from the earliest timestamp
                        # lower_bound = src_min
                    else:
                        if wm is None:
                            # fallback (shouldn’t hit if is_target_empty is true)
                            lower_bound = (src_max - timedelta(minutes=lookback_minutes)) if src_max else None
                        else:
                            lower_bound = wm - timedelta(minutes=lookback_minutes)

                    # 5) handle runaway watermark vs source reality
                    if src_max and lower_bound and lower_bound > src_max:
                        lower_bound = src_max - timedelta(minutes=lookback_minutes)

                    # 6) build WHERE and ORDER
                    preds = []
                    if lower_bound is not None:
                        preds.append(f"{_ch_ident(cursor_col)} >= {_ch_lit(lower_bound)}")
                    if preds:
                        where_sql = "WHERE " + " AND ".join(preds)

                    order_exprs = [f"{_ch_ident(cursor_col)} ASC"]
                    if pk_cols:
                        order_exprs += [f"{_ch_ident(c)} ASC" for c in pk_cols if c != cursor_col]
                    order_sql = ", ".join(order_exprs)
                else:
                    # no cursor: deterministic one-batch by PKs (or by all cols)
                    order_sql = ", ".join(_ch_ident(c) for c in (pk_cols or cols))

                sql = f"SELECT {select_cols} {base_from} "
                if where_sql:
                    sql += f"{where_sql} "
                sql += f"ORDER BY {order_sql} LIMIT {int(chunk_rows)}"

                logger.info("Single-batch SELECT: %s", sql)
                rows = conn_ch.execute(sql)

                # Upsert this one batch (still split into insert_page_size for PG)
                total = 0
                chunks_since_commit = 0
                for chunk in _iter_slices(rows, insert_page_size):
                    _upsert_rows_pg(
                        conn_pg, job["target_schema"], job["target_table"],
                        cols, chunk, pk_cols,
                        pg_type_by_col=pg_type_by_col,
                        version_col=cursor_col,          # keeps newest per-PK within the batch
                        page_size=insert_page_size,
                    )
                    total += len(chunk)
                    inserted_total += len(chunk)
                    chunks_since_commit += 1
                    if chunks_since_commit >= commit_every:
                        conn_pg.commit(); chunks_since_commit = 0
                conn_pg.commit()
                logger.info("Single-batch writer committed; rows %d", total)

            finally:
                try: conn_pg.close()
                except Exception: pass
                try: conn_ch.close()
                except Exception: pass


            logger.info("Extract+Upsert phase (single-batch) done; total rows %d", inserted_total)
            ok(proc)

            # 4) Update registry
            proc = f"{job['id']}_summarize"; pend(proc)
            reg = PostgresHook(postgres_conn_id=REGISTRY_CONN_ID)
            now = datetime.now(timezone.utc)
            next_run = _compute_next_run(job, now)
            reg.run(
                f"""
                UPDATE {REGISTRY_TABLE}
                   SET last_run_at = %s,
                       next_run_at = %s,
                       last_status = %s,
                       last_error  = %s
                 WHERE id = %s
                """,
                parameters=(now.isoformat(), next_run.isoformat(), 'success', None, job["id"])
            )
            logger.info("Job total elapsed %.2fs", time.time()-job_start)
            ok(proc)
            return inserted_total

        except Exception as e:
            logger.exception("JOB %s failed in phase %s", job.get("id"), proc)
            fail(proc, e)
            try:
                reg = PostgresHook(postgres_conn_id=REGISTRY_CONN_ID)
                now = datetime.now(timezone.utc)
                safe_next = now + timedelta(minutes=5)
                reg.run(
                    f"""
                    UPDATE {REGISTRY_TABLE}
                       SET last_run_at = %s,
                           next_run_at = %s,
                           last_status = 'failed',
                           last_error  = %s
                     WHERE id = %s
                    """,
                    parameters=(now.isoformat(), safe_next.isoformat(), str(e)[:2000], job["id"])
                )
            finally:
                pass
            raise

    due = fetch_due_jobs()
    gate = assert_has_due_jobs(due)
    results = run_one_job.expand(job=due)
    gate >> results
