# dags/ck2ck_dynamic_parallel.py
from __future__ import annotations

import time
import re
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from croniter import croniter

# ========= DAG meta =========
DAG_ID = "DWH_all_table"
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
}
DAG_SCHEDULE = "* * * * *"   # tick fast; per-job cadence handled via next_run_at
REGISTRY_CONN_ID = "airflow_logs_mitratel"        # Postgres where registry lives
REGISTRY_TABLE = "public.ck2ck_ingestion_registry"

# ========= Small helpers =========
def _wrap_src_for_from(src_sql: str) -> str:
    clean = _strip_trailing_format(_strip_trailing_semicolon(src_sql))
    return f"({clean}) AS _s"

def _force_nonnull_pk(src_schema: List[Tuple[str, str]], pk_cols_csv: str) -> List[Tuple[str, str]]:
    pkset = {c.strip() for c in (pk_cols_csv or "").split(",") if c.strip()}
    out: List[Tuple[str, str]] = []
    for name, typ in src_schema:
        if name in pkset:
            base = _strip_nullable(typ)  # buang Nullable(...)
            out.append((name, base))     # jadikan non-nullable
        else:
            out.append((name, typ))
    return out

def _insertable_cols(tgt_hook, db: str, tbl: str, src_cols: list[str]) -> list[str]:
    """
    Return the column names to put in INSERT (…) — intersection of src_cols and target columns,
    excluding MATERIALIZED/ALIAS columns (ClickHouse computes them).
    """
    http = tgt_hook.get_conn()
    rows = http.execute(
        "SELECT name, default_kind "
        "FROM system.columns WHERE database = %(db)s AND table = %(tbl)s "
        "ORDER BY position",
        params={"db": db, "tbl": tbl}
    )
    try:
        tgt_order = []
        mat_or_alias = set()
        for name, default_kind in rows:
            if default_kind in ("MATERIALIZED", "ALIAS"):
                mat_or_alias.add(name)
            tgt_order.append(name)
        wanted = [c for c in tgt_order if c in src_cols and c not in mat_or_alias]
        return wanted
    finally:
        try: http.disconnect()
        except Exception: pass

def _insertable_cols_for_table(tgt_hook: ClickHouseHook, db: str, tbl: str) -> list[str]:
    """
    Returns the column names to use in INSERT (…) for an existing CH table,
    excluding MATERIALIZED/ALIAS (they’re computed).
    Preserves table column order.
    """
    http = tgt_hook.get_conn()
    rows = http.execute(
        "SELECT name, default_kind "
        "FROM system.columns "
        "WHERE database = %(db)s AND table = %(tbl)s "
        "ORDER BY position",
        params={"db": db, "tbl": tbl},
    )
    try:
        cols = []
        for name, default_kind in rows:
            if default_kind not in ("MATERIALIZED", "ALIAS"):
                cols.append(name)
        return cols
    finally:
        try: http.disconnect()
        except Exception: pass

def _dt_to_iso(v: Any):
    if isinstance(v, datetime):
        return v.isoformat()
    return v

def _sanitize_job_row(d: dict) -> dict:
    for k in ("last_run_at", "next_run_at"):
        if k in d and isinstance(d[k], datetime):
            d[k] = _dt_to_iso(d[k])
    return d

def _is_due(row: Dict[str, Any], now: datetime) -> bool:
    if not row["enabled"]:
        return False
    nxt = row.get("next_run_at")
    if nxt and isinstance(nxt, str):
        try:
            nxt = datetime.fromisoformat(nxt)
        except Exception:
            nxt = None
    if nxt and now < nxt:
        return False
    return True

def _compute_next_run(row: Dict[str, Any], base: datetime) -> datetime:
    stype = (row.get("schedule_type") or "interval").lower()
    if stype == "interval":
        minutes = int(row.get("interval_minutes") or 15)
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
            (status, now, (str(error_message)[:2000] if error_message else None),
             process_name, dag_name, mark, job["log_kategori"]),
        )
    conn.commit()
    cursor.close(); conn.close()

def _get_ch_conn(conn_id: str):
    """Return (ClickHouseHook, http_conn, airflow Connection)"""
    hook = ClickHouseHook(clickhouse_conn_id=conn_id)
    airflow_conn = BaseHook.get_connection(conn_id)
    return hook, hook.get_conn(), airflow_conn  # http_conn executes SQL on server

def _esc_single_quotes(s: str) -> str:
    if s is None:
        return ""
    return str(s).replace("\\", "\\\\").replace("'", "\\'")

def _lint_source_sql(src_sql: str):
    """Fail fast for common mistakes (e.g., '.owner' without alias)."""
    offenders = set(m.group(0) for m in re.finditer(r'(?m)(?<![A-Za-z0-9_`])\.[A-Za-z_][A-Za-z0-9_]*', src_sql or ""))
    if offenders:
        sample = ", ".join(sorted(offenders))
        raise ValueError(
            f"Source SQL has column(s) starting with a dot (missing table/alias). Fix these: {sample}. "
            "Example: '.owner' → 't.owner'."
        )

def _strip_nullable(t: str) -> str:
    t = (t or "").strip()
    return t[9:-1] if t.startswith("Nullable(") and t.endswith(")") else t

def _is_int_type(t: str) -> bool:
    return _strip_nullable(t) in {"Int8","Int16","Int32","Int64","UInt8","UInt16","UInt32","UInt64"}

def _is_datetime_type(t: str) -> bool:
    base = _strip_nullable(t)
    return base in {"Date","DateTime"} or base.startswith("DateTime64(")

def _strip_trailing_semicolon(sql: str) -> str:
    return re.sub(r';\s*$', '', sql.strip(), flags=re.S)

def _strip_trailing_format(sql: str) -> str:
    return re.sub(r'\s+FORMAT\s+\w+\s*$', '', sql, flags=re.IGNORECASE)

def _describe_query(ch_hook, database: str, src_sql: str):
    client = ch_hook.get_conn()

    sql_clean = _strip_trailing_format(_strip_trailing_semicolon(src_sql))
    wrapped = f"SELECT * FROM ({sql_clean}) AS _q LIMIT 0"

    _, cols = client.execute(wrapped, with_column_types=True)

    # Normalisasi: jadikan list[(name, type)] meski driver/versi beda
    norm_cols = []
    for col in cols:
        if isinstance(col, (list, tuple)):
            name = col[0]
            ch_type = col[1] if len(col) > 1 else "String"
            norm_cols.append((name, ch_type))
        else:
            # fallback—abaikan jika bentuknya aneh
            continue

    return norm_cols

def _target_columns(tgt_hook: ClickHouseHook, db: str, table: str) -> Dict[str, str]:
    http = tgt_hook.get_conn()
    try:
        rows = http.execute(
            "SELECT name, type FROM system.columns "
            "WHERE database = '{db}' AND table = '{tbl}' ORDER BY position".format(
                db=_esc_single_quotes(db), tbl=_esc_single_quotes(table)
            )
        )
        return {name: typ for name, typ in rows}
    finally:
        try: http.disconnect()
        except Exception: pass

def _current_engine_and_order_by(tgt_hook: ClickHouseHook, db: str, table: str) -> Tuple[str, str]:
    http = tgt_hook.get_conn()
    try:
        r = http.execute(
            "SELECT engine, sorting_key FROM system.tables "
            "WHERE database = '{db}' AND name = '{tbl}' LIMIT 1".format(
                db=_esc_single_quotes(db), tbl=_esc_single_quotes(table)
            )
        )
        if not r:
            return "", ""
        return r[0][0], (r[0][1] or "")
    finally:
        try: http.disconnect()
        except Exception: pass

def _desired_order_by(pk_cols: str, fallback: str = "tuple()") -> str:
    parts = [c.strip() for c in (pk_cols or "").split(",") if c.strip()]
    return "({})".format(", ".join(["`{}`".format(p) for p in parts])) if parts else fallback

def _build_engine_and_helper(src_schema: List[Tuple[str, str]], version_col: Optional[str]) -> Tuple[str, Optional[str], Optional[str]]:
    """
    Decide ENGINE clause and (optionally) return helper version column DDL/name when src version is Nullable.
    Returns: (engine_clause, helper_col_name_or_None, helper_col_ddl_or_None)
    """
    if not version_col:
        return "ENGINE = ReplacingMergeTree()\n", None, None

    src_types = {c: t for c, t in src_schema}
    vt = src_types.get(version_col)
    if not vt or (not _is_int_type(vt) and not _is_datetime_type(vt)):
        return "ENGINE = ReplacingMergeTree()\n", None, None

    base = _strip_nullable(vt)
    if vt.startswith("Nullable("):
        helper = "__{}_v".format(version_col)
        if base.startswith("DateTime64("):
            prec = base[11:-1]
            ddl = "`{}` DateTime64({}) MATERIALIZED ifNull(`{}`, toDateTime64(0, {}))".format(helper, prec, version_col, prec)
        elif base == "DateTime":
            ddl = "`{}` DateTime MATERIALIZED ifNull(`{}`, toDateTime(0))".format(helper, version_col)
        elif base == "Date":
            ddl = "`{}` Date MATERIALIZED ifNull(`{}`, toDate('1970-01-01'))".format(helper, version_col)
        else:
            ddl = "`{}` {} MATERIALIZED ifNull(`{}`, 0)".format(helper, base, version_col)
        engine = "ENGINE = ReplacingMergeTree(`{}`)\n".format(helper)
        return engine, helper, ddl

    engine = "ENGINE = ReplacingMergeTree(`{}`)\n".format(version_col)
    return engine, None, None

def _create_target_if_missing(
    tgt_hook: ClickHouseHook, tgt_db: str, tgt_table: str,
    src_schema: List[Tuple[str, str]],
    order_by_expr: str,
    version_col: Optional[str],
    partition_by: Optional[str] = None,
    sample_by: Optional[str] = None
):
    http = tgt_hook.get_conn()
    try:
        engine, helper_name, helper_ddl = _build_engine_and_helper(src_schema, version_col)

        cols = ["`{}` {}".format(c, t) for c, t in src_schema]
        if helper_ddl:
            cols.append(helper_ddl)
        cols_ddl = ",\n  ".join(cols) if cols else "`__dummy` UInt8"

        extras = []
        if partition_by: extras.append("PARTITION BY {}".format(partition_by))
        if sample_by:    extras.append("SAMPLE BY {}".format(sample_by))
        extras_sql = ("\n" + "\n".join(extras)) if extras else ""

        http.execute("CREATE DATABASE IF NOT EXISTS `{}`".format(tgt_db))
        sql = (
            "CREATE TABLE IF NOT EXISTS `{db}`.`{tbl}` (\n"
            "  {cols}\n"
            ")\n"
            "{engine}"
            "ORDER BY {orderby}"
            "{extras}\n"
            "SETTINGS index_granularity = 8192"
        ).format(
            db=tgt_db, tbl=tgt_table, cols=cols_ddl,
            engine=engine, orderby=order_by_expr, extras=extras_sql
        )
        http.execute(sql)
    finally:
        try: http.disconnect()
        except Exception: pass

def _sync_columns(
    tgt_hook: ClickHouseHook, tgt_db: str, tgt_table: str,
    src_schema: List[Tuple[str, str]],
    allow_drop: bool,
    version_col: Optional[str]
):
    http = tgt_hook.get_conn()
    try:
        tgt_map = _target_columns(tgt_hook, tgt_db, tgt_table)
        src_names = [c for c, _ in src_schema]
        src_types = {c: t for c, t in src_schema}

        # Add missing query columns
        for c in src_names:
            if c not in tgt_map:
                http.execute("ALTER TABLE `{db}`.`{tbl}` ADD COLUMN `{col}` {typ}".format(
                    db=tgt_db, tbl=tgt_table, col=c, typ=src_types[c]
                ))

        # Ensure helper (if nullable version)
        helper_name = None
        helper_ddl = None
        if version_col:
            _, helper_name, helper_ddl = _build_engine_and_helper(src_schema, version_col)
            if helper_name and helper_ddl:
                if helper_name not in tgt_map:
                    http.execute("ALTER TABLE `{db}`.`{tbl}` ADD COLUMN {ddl}".format(
                        db=tgt_db, tbl=tgt_table, ddl=helper_ddl
                    ))
                else:
                    http.execute("ALTER TABLE `{db}`.`{tbl}` MODIFY COLUMN {ddl}".format(
                        db=tgt_db, tbl=tgt_table, ddl=helper_ddl
                    ))

        # Drop columns no longer in query (optionally keep helper)
        if allow_drop:
            keep = set(src_names)
            if helper_name:
                keep.add(helper_name)
            for c in list(tgt_map.keys()):
                if c not in keep:
                    http.execute("ALTER TABLE `{db}`.`{tbl}` DROP COLUMN `{col}`".format(
                        db=tgt_db, tbl=tgt_table, col=c
                    ))
    finally:
        try: http.disconnect()
        except Exception: pass

def _rebuild_if_needed(
    tgt_hook: ClickHouseHook, db: str, tbl: str,
    src_schema: List[Tuple[str, str]],
    order_by_wanted: str, version_col: Optional[str],
    partition_by: Optional[str] = None, sample_by: Optional[str] = None
):
    engine_cur, sorting_key = _current_engine_and_order_by(tgt_hook, db, tbl)
    engine_wanted, helper_name, _ = _build_engine_and_helper(src_schema, version_col)
    wanted_engine_prefix = "ReplacingMergeTree"
    ok_engine = engine_cur.startswith(wanted_engine_prefix) and (
        (helper_name and helper_name in engine_cur) or
        (not helper_name and (not version_col or (version_col in engine_cur or engine_cur == "ReplacingMergeTree")))
    )
    ok_sort   = sorting_key.replace(" ", "") == order_by_wanted.replace(" ", "")
    if ok_engine and ok_sort:
        return

    http = tgt_hook.get_conn()
    try:
        cols = http.execute(
            "SELECT name, type FROM system.columns "
            "WHERE database='{db}' AND table='{tbl}' ORDER BY position".format(
                db=_esc_single_quotes(db), tbl=_esc_single_quotes(tbl)
            )
        )
        cols_ddl = ",\n  ".join(["`{}` {}".format(c, t) for c, t in cols]) if cols else "`__dummy` UInt8"
        extras = []
        if partition_by: extras.append("PARTITION BY {}".format(partition_by))
        if sample_by:    extras.append("SAMPLE BY {}".format(sample_by))
        extras_sql = ("\n" + "\n".join(extras)) if extras else ""

        engine_clause = "{}ORDER BY {}".format(engine_wanted, order_by_wanted)

        rebuild_name = "{}__rebuild".format(tbl)
        http.execute("DROP TABLE IF EXISTS `{db}`.`{rebuild}` SYNC".format(db=db, rebuild=rebuild_name))
        sql = (
            "CREATE TABLE `{db}`.`{rebuild}` (\n"
            "  {cols}\n"
            ")\n"
            "{engine}"
            "{extras}"
        ).format(
            db=db, rebuild=rebuild_name, cols=cols_ddl,
            engine=engine_clause, extras=("\n"+extras_sql) if extras_sql else ""
        )
        http.execute(sql)

        # Copy with explicit column list (omit MATERIALIZED/ALIAS)
        insert_cols = _insertable_cols_for_table(tgt_hook, db, rebuild_name)
        cols_sql = ", ".join("`{}`".format(c) for c in insert_cols)
        http.execute(
            "INSERT INTO `{db}`.`{rebuild}` ({cols}) SELECT {cols} FROM `{db}`.`{tbl}`".format(
                db=db, rebuild=rebuild_name, tbl=tbl, cols=cols_sql
            )
        )
        bak = "{}__old_{}".format(tbl, int(time.time()))
        http.execute("RENAME TABLE `{db}`.`{tbl}` TO `{db}`.`{bak}`, `{db}`.`{rebuild}` TO `{db}`.`{tbl}`".format(
            db=db, tbl=tbl, bak=bak, rebuild=rebuild_name
        ))
        http.execute("DROP TABLE IF EXISTS `{db}`.`{bak}` SYNC".format(db=db, bak=bak))
    finally:
        try: http.disconnect()
        except Exception: pass

def _build_tuple(pk_cols: str) -> str:
    cols = [c.strip() for c in pk_cols.split(",") if c.strip()]
    return "tuple({})".format(", ".join(["`{}`".format(c) for c in cols]))

def _build_tuple_values(pk_cols: str, last_vals: Tuple[Any, ...]) -> str:
    parts = []
    for v in last_vals:
        if v is None:
            parts.append("NULL")
        elif isinstance(v, (int, float)):
            parts.append(str(v))
        else:
            parts.append("'{}'".format(_esc_single_quotes(v)))
    return "({})".format(", ".join(parts))

def _slice_where(pk_cols: str, slice_id: int, parallel_slices: int) -> str:
    pk_tuple = _build_tuple(pk_cols).replace("`", "")
    return "(cityHash64({}) % {}) = {}".format(pk_tuple, parallel_slices, slice_id)

def _source_conn_meta(source_conn) -> Dict[str, Any]:
    extra = source_conn.extra_dejson or {}
    return {
        "host": source_conn.host,
        "tcp_port": int(extra.get("native_port", 9000)),
        "user": source_conn.login or "",
        "password": source_conn.password or "",
    }

# ========= DAG =========
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule_interval=DAG_SCHEDULE,
    catchup=False,
    tags=["dwh","clickhouse","ck2ck","schema-sync","replacingmergetree","parallel"],
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
              source_ch_conn_id, target_ch_conn_id, pg_log_conn_id,
              src_database, src_sql,
              target_db, target_table,
              pk_cols, version_col,
              schedule_type, interval_minutes, cron_expr,
              parallel_slices, page_rows,
              allow_drop_columns, truncate_before_load,
              pre_sql, post_sql,
              log_table, log_type, log_kategori,
              last_run_at, next_run_at, last_status, last_error
            FROM {REGISTRY_TABLE}
            WHERE enabled = TRUE
              AND (next_run_at IS NULL OR next_run_at <= now())
            ORDER BY COALESCE(next_run_at, now()) ASC, id ASC
            LIMIT 20
        """)
        cols = [
            "id","enabled",
            "source_ch_conn_id","target_ch_conn_id","pg_log_conn_id",
            "src_database","src_sql",
            "target_db","target_table",
            "pk_cols","version_col",
            "schedule_type","interval_minutes","cron_expr",
            "parallel_slices","page_rows",
            "allow_drop_columns","truncate_before_load",
            "pre_sql","post_sql",
            "log_table","log_type","log_kategori",
            "last_run_at","next_run_at","last_status","last_error"
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
        mark = "{}_{}".format(job['id'], datetime.now().strftime('%Y%m%d%H%M%S%f'))
        inserted_total = 0

        def ok(proc): _log_status(job, proc, mark, "success")
        def pend(proc): _log_status(job, proc, mark, "pending")
        def fail(proc, e): _log_status(job, proc, mark, "failed", str(e))

        try:
            # 1) Ensure / sync target schema
            proc = "{}_ensure_and_sync_schema".format(job['id']); pend(proc)

            src_hook, _, _ = _get_ch_conn(job["source_ch_conn_id"])
            tgt_hook, _, _ = _get_ch_conn(job["target_ch_conn_id"])

            # infer schema from query
            src_schema = _describe_query(src_hook, job["src_database"], job["src_sql"])
            src_cols   = [name for name, _ in src_schema]

            src_schema_nn = _force_nonnull_pk(src_schema, job["pk_cols"])

            if not src_cols:
                raise RuntimeError("Source query returns no columns.")
            # validate PK exists in projection
            pk_tokens = [c.strip() for c in (job["pk_cols"] or "").split(",") if c.strip()]
            for pkc in pk_tokens:
                if pkc not in src_cols:
                    raise RuntimeError(f"Primary key column '{pkc}' is not present in source query projection.")

            order_by_expr = _desired_order_by(job["pk_cols"], "tuple()")

            _create_target_if_missing(
                tgt_hook, job["target_db"], job["target_table"],
                src_schema_nn, order_by_expr, job.get("version_col")
            )
            _sync_columns(
                tgt_hook, job["target_db"], job["target_table"],
                src_schema_nn, bool(job["allow_drop_columns"]), job.get("version_col")
            )
            _rebuild_if_needed(
                tgt_hook, job["target_db"], job["target_table"],
                src_schema_nn, order_by_expr, job.get("version_col")
            )
            ok(proc)

            # 2) Optional pre_sql (use short-lived client)
            proc = "{}_pre_sql".format(job['id']); pend(proc)
            if job.get("pre_sql"):
                c = ClickHouseHook(clickhouse_conn_id=job["target_ch_conn_id"]).get_conn()
                try:
                    c.execute("USE `{}`".format(job["target_db"]))
                    c.execute(job["pre_sql"])
                finally:
                    try: c.disconnect()
                    except Exception: pass
            ok(proc)

            # 3) Extract+Load (parallel slices, paged per slice)
            proc = "{}_extract_load_parallel".format(job['id']); pend(proc)

            # truncate if full-refresh
            if job["truncate_before_load"]:
                c = ClickHouseHook(clickhouse_conn_id=job["target_ch_conn_id"]).get_conn()
                try:
                    c.execute("TRUNCATE TABLE `{db}`.`{tbl}`".format(
                        db=job["target_db"], tbl=job["target_table"]
                    ))
                finally:
                    try: c.disconnect()
                    except Exception: pass

            parallel_slices = max(1, int(job["parallel_slices"] or 1))
            page_rows = max(1, int(job["page_rows"] or 50000))

            def run_slice(slice_id: int, src_hook: ClickHouseHook, tgt_hook: ClickHouseHook) -> int:
                def _scalar_literal(v: Any) -> str:
                    if v is None:
                        return "NULL"
                    if isinstance(v, (int, float)):
                        return str(v)
                    if isinstance(v, datetime):
                        return f"toDateTime('{v.strftime('%Y-%m-%d %H:%M:%S')}')"
                    return "'{}'".format(_esc_single_quotes(v))

                src_client = None
                tgt_client = None
                try:
                    src_client = src_hook.get_conn()
                    tgt_client = tgt_hook.get_conn()

                    rows_inserted = 0
                    last_key: Optional[Tuple[Any, ...]] = None

                    insert_cols = _insertable_cols(tgt_hook, job["target_db"], job["target_table"], src_cols)
                    if not insert_cols:
                        raise RuntimeError("No insertable columns resolved between source query and target table.")

                    insert_cols_sql = ", ".join(f"`{c}`" for c in insert_cols)
                    select_cols_sql = ", ".join(f"_s.`{c}`" for c in insert_cols)

                    pk_tokens_loc = [c.strip() for c in (job["pk_cols"] or "").split(",") if c.strip()]
                    if not pk_tokens_loc:
                        raise RuntimeError("pk_cols is empty; need at least one cursor column.")
                    pk_idx = [insert_cols.index(c) for c in pk_tokens_loc]

                    wrapped_src = _wrap_src_for_from(job["src_sql"])

                    single_key = len(pk_tokens_loc) == 1
                    if single_key:
                        pk = pk_tokens_loc[0]
                        # Use assumeNotNull for hash, order, and comparison
                        pk_nn = f"assumeNotNull(_s.`{pk}`)"
                        hash_expr = f"cityHash64({pk_nn})"
                        order_by_clause = f"ORDER BY ({pk_nn})"
                        null_guards = [f"isNotNull(_s.`{pk}`)"]

                        def page_predicate():
                            preds = [f"({hash_expr} % {parallel_slices}) = {slice_id}", *null_guards]
                            if last_key is not None:
                                preds.append(f"{pk_nn} > {_scalar_literal(last_key[0])}")
                            return " AND ".join(preds)

                    else:
                        # Build tuple of PKs and a non-nullable version for hash/compare
                        tuple_pk_raw = "tuple({})".format(", ".join(f"_s.`{c}`" for c in pk_tokens_loc))
                        tuple_pk_nn  = "tuple({})".format(", ".join(f"assumeNotNull(_s.`{c}`)" for c in pk_tokens_loc))
                        hash_expr = f"cityHash64({tuple_pk_nn})"
                        order_by_clause = "ORDER BY ({})".format(", ".join(f"assumeNotNull(_s.`{c}`)" for c in pk_tokens_loc))
                        null_guards = [f"isNotNull(_s.`{c}`)" for c in pk_tokens_loc]

                        def page_predicate():
                            preds = [f"({hash_expr} % {parallel_slices}) = {slice_id}", *null_guards]
                            if last_key is not None:
                                rhs = "(" + ", ".join(_scalar_literal(v) for v in last_key) + ")"
                                preds.append(f"({tuple_pk_nn} > {rhs})")
                            return " AND ".join(preds)

                    while True:
                        where_sql = page_predicate()
                        select_sql = (
                            f"SELECT {select_cols_sql} "
                            f"FROM {wrapped_src} "
                            f"WHERE {where_sql} {order_by_clause} "
                            f"LIMIT {page_rows}"
                        )

                        rows = src_client.execute(select_sql)
                        if not rows:
                            break

                        tgt_client.execute(
                            f"INSERT INTO `{job['target_db']}`.`{job['target_table']}` ({insert_cols_sql}) VALUES",
                            rows
                        )

                        rows_inserted += len(rows)
                        last_key = tuple(rows[-1][i] for i in pk_idx)
                        if len(rows) < page_rows:
                            break

                    return rows_inserted

                finally:
                    try:
                        if src_client is not None:
                            src_client.disconnect()
                    except Exception:
                        pass
                    try:
                        if tgt_client is not None:
                            tgt_client.disconnect()
                    except Exception:
                        pass

            total_rows = 0
            with ThreadPoolExecutor(max_workers=parallel_slices) as pool:
                futures = [pool.submit(run_slice, i, src_hook, tgt_hook) for i in range(parallel_slices)]
                for f in as_completed(futures):
                    total_rows += int(f.result() or 0)

            inserted_total = total_rows
            ok(proc)

            # 4) Optional post_sql (short-lived client)
            proc = "{}_post_sql".format(job['id']); pend(proc)
            if job.get("post_sql"):
                c = ClickHouseHook(clickhouse_conn_id=job["target_ch_conn_id"]).get_conn()
                try:
                    c.execute("USE `{}`".format(job["target_db"]))
                    c.execute(job["post_sql"])
                finally:
                    try: c.disconnect()
                    except Exception: pass
            ok(proc)

            # 5) Update next_run_at
            proc = "{}_finalize".format(job['id']); pend(proc)
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

    # --------- Wiring (single-level mapping) ---------
    due = fetch_due_jobs()
    gate = assert_has_due_jobs(due)
    results = run_one_job.expand(job=due)
    gate >> results
