from __future__ import annotations

import json, re, gzip, hashlib, time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Any, List, Tuple, Optional

import logging
import requests
import xml.etree.ElementTree as ET
import random

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import get_current_context

STRICT_SAP_STRINGS = True
LOGGER = logging.getLogger("DL_sap_all_table")
DEBUG = (Variable.get("SAP_CDC_DEBUG", default_var="1") == "1")

try:
    from croniter import croniter
    HAVE_CRON = True
except Exception:
    HAVE_CRON = False

# ===== DAG meta =====
DAG_ID = "DL_sap_all_table"
DEFAULT_SCHEDULE = "*/2 * * * *"
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

# ===== SAP OData headers / ns =====
HEADERS = {'Accept-Encoding': 'gzip', 'Prefer': 'odata.track-changes'}
OADATA_NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
    'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices',
}

# Registry/prefix live here (single bootstrap connection)
REGISTRY_CONN_ID = "airflow_logs_mitratel"
REGISTRY_TABLE   = "sap_cdc_registry"
REGISTRY_TABLE_2 = "sap_cdc_env_prefix"

# Log table (only table name/schema; the **connection** comes from job.log_conn_id)
LOG_TABLE    = "public.airflow_logs"
LOG_TYPE     = "sap cdc"
LOG_KATEGORI = "Data Lake"

# ---------- helpers (JSON-safe, hooks, SQL fetch) ----------
def _extract_pk_cols_from_entity_id(entity_id: str) -> List[str]:
    m = re.search(r"\((.+)\)", entity_id)
    if not m:
        return []
    inside = m.group(1).strip()
    parts = re.findall(r"(?:[^,'\"]+|'[^']*'|\"[^\"]*\")+", inside)
    cols = []
    for p in parts:
        kv = p.split("=", 1)
        if len(kv) == 2:
            cols.append(kv[0].strip())
    return cols

def _first_entry_pk_cols(xml_bytes: bytes) -> List[str]:
    try:
        root = ET.fromstring(xml_bytes)
        entry = root.find('atom:entry', OADATA_NS)
        if entry is None:
            return []
        id_el = entry.find('atom:id', OADATA_NS)
        if id_el is None or not (id_el.text or "").strip():
            title_el = entry.find('atom:title', OADATA_NS)
            cand = (title_el.text or "") if title_el is not None else ""
            return _extract_pk_cols_from_entity_id(cand)
        return _extract_pk_cols_from_entity_id(id_el.text or "")
    except Exception:
        return []

from decimal import Decimal
from urllib.parse import urljoin

XML_NS = "{http://www.w3.org/XML/1998/namespace}"

def _feed_service_root(xml_bytes: bytes, fallback_from_url: Optional[str] = None) -> Optional[str]:
    try:
        root = ET.fromstring(xml_bytes)
        base = root.attrib.get(XML_NS + "base") or root.attrib.get("xml:base")
        if base:
            return base if base.endswith("/") else base + "/"
    except Exception:
        pass
    if fallback_from_url:
        m = re.search(r"(https?://[^/]+/sap/opu/odata/[^/]+/[^/]+/)", fallback_from_url)
        if m:
            return m.group(1)
    return None

def _get_next_delta_url(pg: PostgresHook, job_code: str, env: str) -> Optional[str]:
    col = f"{env.lower()}_next_delta_url"
    rows = fetch_dicts(pg, f"SELECT {col} FROM {REGISTRY_TABLE} WHERE job_code=%s", (job_code,))
    if not rows: return None
    return rows[0].get(col)

def _set_next_delta_url(pg: PostgresHook, job_code: str, env: str, url: Optional[str]):
    col = f"{env.lower()}_next_delta_url"
    pg.run(f"UPDATE {REGISTRY_TABLE} SET {col}=%s, updated_at=now() WHERE job_code=%s", parameters=(url, job_code))

def _abs_delta_url(base_url: str, href: str) -> str:
    b = base_url if base_url.endswith('/') else base_url + '/'
    return urljoin(b, href.lstrip('/'))

def _json_safe(v):
    if isinstance(v, datetime):
        v = v.astimezone(timezone.utc).replace(tzinfo=None)
        return v.isoformat(sep=' ')
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (bytes, bytearray)):
        return v.decode('utf-8', errors='ignore')
    return v

def get_sql_hook(conn_id: str):
    conn = BaseHook.get_connection(conn_id)
    ct = (conn.conn_type or "").lower()
    if ct in ("postgres", "postgresql", "psql"):
        return PostgresHook(postgres_conn_id=conn_id)
    if ct in ("mysql", "mariadb"):
        return MySqlHook(mysql_conn_id=conn_id)
    return PostgresHook(postgres_conn_id=conn_id)

def fetch_dicts(hook, sql: str, params=None) -> list[dict]:
    conn = hook.get_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        return [{cols[i]: _json_safe(r[i]) for i in range(len(cols))} for r in rows]
    finally:
        try: cur.close()
        except Exception: pass
        conn.close()

# ---------- logging (uses job.log_conn_id dynamically) ----------
def _log_status(log_hook, process_name: str, mark: str, status: str,
                ck_db: str, ck_table: str, error_message: Optional[str] = None):
    dag_name = f"{ck_db}.{ck_table}"
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    conn = log_hook.get_conn(); cur = conn.cursor()
    try:
        if status == "pending":
            cur.execute(
                f"INSERT INTO {LOG_TABLE} (process_name, dag_name, type, status, start_time, mark, kategori) "
                f"VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (process_name, dag_name, LOG_TYPE, status, now, mark, LOG_KATEGORI)
            )
        else:
            cur.execute(
                f"UPDATE {LOG_TABLE} SET status=%s, end_time=%s, error_message=%s "
                f"WHERE process_name=%s AND status='pending' AND dag_name=%s AND mark=%s AND kategori=%s",
                (status, now, error_message, process_name, dag_name, mark, LOG_KATEGORI)
            )
        conn.commit()
    finally:
        try: cur.close()
        except Exception: pass
        conn.close()

def log_event(
    log_hook,
    job_code: str,
    env: str,
    step: str,
    mark: str,
    ck_db: str,
    ck_table: str,
    msg: str
):
    pname = f"{job_code}:{env}:{step}:event"
    _log_status(log_hook, pname, mark, "pending", ck_db, ck_table)
    _log_status(log_hook, pname, mark, "success", ck_db, ck_table, error_message=msg)

def log_debug(msg: str, **kv):
    if DEBUG:
        if kv:
            kv_s = " | " + " ".join([f"{k}={kv[k]}" for k in kv])
        else:
            kv_s = ""
        LOGGER.info("[DEBUG] %s%s", msg, kv_s)

@contextmanager
def phase_logger(log_hook, process_name: str, mark: str, ck_db: str, ck_table: str):
    _log_status(log_hook, process_name, mark, "pending", ck_db, ck_table)
    try:
        yield
        _log_status(log_hook, process_name, mark, "success", ck_db, ck_table)
    except Exception as e:
        _log_status(log_hook, process_name, mark, "error", ck_db, ck_table, error_message=str(e))
        raise

@contextmanager
def page_logger(log_hook, job_code: str, env: str, phase: str, mark: str, ck_db: str, ck_table: str, skip: int, top: int):
    pname = f"{job_code}:{env}:{phase}:page:skip={skip}:top={top}"
    _log_status(log_hook, pname, mark, "pending", ck_db, ck_table)
    try:
        def ok(pulled: int):
            _log_status(log_hook, pname, mark, "success", ck_db, ck_table,
                        error_message=f"pulled={pulled}; skip_start={skip}; top={top}")
        yield ok
    except Exception as e:
        _log_status(log_hook, pname, mark, "error", ck_db, ck_table, error_message=str(e))
        raise

# ---------- SAP helpers ----------
def _gzip_aware(resp: requests.Response) -> bytes:
    enc = (resp.headers.get('Content-Encoding') or "").lower()
    data = resp.content
    is_gzip_magic = data[:2] == b'\x1f\x8b'
    should_try_gzip = ('gzip' in enc) or is_gzip_magic
    if should_try_gzip:
        try:
            return gzip.decompress(data)
        except Exception:
            return data
    return data

def _auth(env: str) -> Tuple[str,str]:
    return Variable.get(f"SAP_{env}_AUTH_USER"), Variable.get(f"SAP_{env}_AUTH_PASS")

def _load_prefix_map(pg: PostgresHook) -> Dict[str,str]:
    rows = pg.get_records(f"SELECT env, base_url FROM {REGISTRY_TABLE_2} WHERE is_enabled = TRUE")
    return {r[0].upper(): r[1].rstrip('/') for r in rows}

def _compose_url(base: str, path: str, sap_client: str) -> str:
    p = path if path.startswith('/') else '/' + path
    sep = '&' if '?' in p else '?'
    return f"{base}{p}{sep}sap-client={sap_client}"

def _infer_ck_type(val: Optional[str]) -> str:
    if STRICT_SAP_STRINGS:
        return "Nullable(String)"
    if val is None or str(val).strip() == "" or str(val).upper() == "NULL":
        return "Nullable(String)"
    v = str(val).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            datetime.strptime(v[:19], fmt)
            return "Nullable(DateTime)" if 'T' in v else "Nullable(Date)"
        except Exception:
            pass
    if re.fullmatch(r"[-+]?\d+", v): return "Nullable(Int64)"
    if re.fullmatch(r"[-+]?\d+(\.\d+)?", v): return "Nullable(Float64)"
    return "Nullable(String)"

SAP_INTERNAL_COLS = {
    "_ingested_at","_batch_id","_env","_is_deleted","_op_seq","_pk_hash","_batch_row_count"
}

def _coerce_sap_values_to_string(row: Dict[str, Any]) -> Dict[str, Any]:
    if not STRICT_SAP_STRINGS:
        return row
    out = {}
    for k, v in row.items():
        if k in SAP_INTERNAL_COLS:
            out[k] = v
        else:
            out[k] = None if v is None else str(v)
    return out

def _props_from_entry(entry: ET.Element) -> Dict[str,Any]:
    props = {}
    root = entry.find('atom:content/m:properties', OADATA_NS)
    if root is None: return props
    for el in list(root):
        name = el.tag.split('}',1)[1] if '}' in el.tag else el.tag
        props[name] = el.text if el.text is not None else None
    return props

def _parse_feed(xml_bytes: bytes) -> Tuple[List[Dict[str,Any]], Optional[str], Optional[str], List[str]]:
    tree = ET.fromstring(xml_bytes)
    entries = [_props_from_entry(e) for e in tree.findall('atom:entry', OADATA_NS)]
    change_link, delta_token = None, None
    for e in tree.findall('atom:entry', OADATA_NS):
        link = e.find("atom:link[@rel='http://schemas.microsoft.com/ado/2007/08/dataservices/related/ChangesAfter']", OADATA_NS)
        if link is not None and link.get('href'):
            change_link = link.get('href')
            break
    for e in tree.findall('atom:entry', OADATA_NS):
        props = e.find('atom:content/m:properties', OADATA_NS)
        if props is None:
            continue
        tok = props.find('d:DeltaToken', OADATA_NS)
        if tok is not None and tok.text:
            delta_token = tok.text
            break
    pk_cols = _first_entry_pk_cols(xml_bytes)
    return entries, change_link, delta_token, pk_cols

def _schema_sync(ch: ClickHouseHook, db: str, table: str,
                 rows: List[Dict[str,Any]],
                 pk_cols: Optional[List[str]] = None):
    ck_exec(ch, f"CREATE DATABASE IF NOT EXISTS {db}")
    full = f"{db}.{table}"

    # desired types
    types: Dict[str,str] = {}
    for r in rows:
        for k, v in r.items():
            t = _infer_ck_type(v)
            if k not in types or types[k] == "Nullable(String)":
                types[k] = t

    # internal cols
    types.update({
        "_ingested_at":"DateTime","_batch_id":"String","_env":"LowCardinality(String)",
        "_is_deleted":"UInt8","_op_seq":"UInt64","_pk_hash":"UInt64","_batch_row_count":"UInt64"
    })

    # ORDER BY uses stable PK (FIX)
    if pk_cols:
        key_exprs = [f"ifNull(`{c}`, '')" for c in pk_cols]
        order_expr = f"({', '.join(key_exprs)})"
    else:
        order_expr = "(_pk_hash)"

    create_cols = ",\n  ".join([f"`{c}` {t}" for c,t in types.items()])
    ck_exec(ch, f"""
      CREATE TABLE IF NOT EXISTS {full} (
        {create_cols}
      )
      ENGINE = ReplacingMergeTree(_op_seq)
      ORDER BY {order_expr}
      PARTITION BY toYYYYMM(_ingested_at)
      SETTINGS index_granularity = 8192
    """)

    # describe existing
    existing_cols = set()
    existing_types = {}
    try:
        for name, ctype, *_ in ck_query(ch, f"DESCRIBE TABLE {full}"):
            existing_cols.add(name); existing_types[name] = ctype
    except Exception:
        pass

    # add missing
    for c, t in types.items():
        if c not in existing_cols:
            ck_exec(ch, f"ALTER TABLE {full} ADD COLUMN IF NOT EXISTS `{c}` {t}")

    # keep SAP props as strings
    if STRICT_SAP_STRINGS and existing_types:
        for c, ctype in existing_types.items():
            if c in SAP_INTERNAL_COLS:
                continue
            cl = ctype.lower()
            if cl.startswith("nullable(string)") or cl == "string":
                continue
            try:
                ck_exec(ch, f"ALTER TABLE {full} MODIFY COLUMN `{c}` Nullable(String)")
            except Exception:
                pass

def _pk_hash(row: Dict[str,Any], pk_cols: Optional[List[str]] = None) -> int:
    keys = (pk_cols if pk_cols else sorted(row.keys()))
    s = "|".join([f"{k}={row.get(k,'')}" for k in keys])
    return int.from_bytes(hashlib.sha1(s.encode('utf-8')).digest()[:8], 'big', signed=False)

def _insert_rows(ch, db, table, env, rows, batch_id, is_delete=0, batch_row_count=0, pk_cols=None):
    if not rows: return
    full = f"{db}.{table}"
    now_dt = datetime.utcnow()
    seq = int(time.time()*1000)

    data = []
    for i, r in enumerate(rows):
        d = dict(r)
        d.update({
            "_ingested_at": now_dt,
            "_batch_id": batch_id,
            "_env": env,
            "_is_deleted": is_delete,
            "_op_seq": seq + i,
            "_pk_hash": _pk_hash(r, pk_cols=pk_cols),  # FIX: stable PK
            "_batch_row_count": batch_row_count or len(rows),
        })
        d = _coerce_sap_values_to_string(d)
        data.append(d)
    cols = sorted(list(data[0].keys()))
    sql = f"INSERT INTO {full} ({','.join(f'`{c}`' for c in cols)}) VALUES"
    ck_insert(ch, sql, [{c: row.get(c) for c in cols} for row in data])

def _get_initial_page(session: requests.Session, url_base: str, skip: int, top: int, auth: Tuple[str,str]) -> bytes:
    sep = '&' if '?' in url_base else '?'
    url = f"{url_base}{sep}$skip={skip}&$top={top}"
    resp = session.get(url, auth=auth, headers=HEADERS, timeout=120)
    if DEBUG:
        LOGGER.info("[DEBUG] GET %s | status=%s | ce=%s | ct=%s | len=%s | head=%r",
                    url, resp.status_code,
                    resp.headers.get('Content-Encoding'),
                    resp.headers.get('Content-Type'),
                    len(resp.content),
                    resp.content[:20])
    resp.raise_for_status()
    return _gzip_aware(resp)

def _get_url(session: requests.Session, url: str, auth: Tuple[str,str]) -> bytes:
    resp = session.get(url, auth=auth, headers=HEADERS, timeout=120)
    if DEBUG:
        LOGGER.info("[DEBUG] GET %s | status=%s | ce=%s | ct=%s | len=%s | head=%r",
                    url, resp.status_code,
                    resp.headers.get('Content-Encoding'),
                    resp.headers.get('Content-Type'),
                    len(resp.content),
                    resp.content[:20])
    resp.raise_for_status()
    return _gzip_aware(resp)

def _compute_next_run(schedule_type: str, interval_minutes: int, cron_expr: Optional[str],
                      after: datetime) -> datetime:
    if schedule_type == "interval":
        return after + timedelta(minutes=max(1, int(interval_minutes or 1)))
    if HAVE_CRON and cron_expr:
        return croniter(cron_expr, after).get_next(datetime)
    return after + timedelta(minutes=5)

def _update_env_runtime(pg: PostgresHook, job_code: str, env: str,
                        status: str, error: Optional[str],
                        last_run: datetime, next_run: datetime,
                        next_skip: Optional[int] = None,
                        initial_done: Optional[bool] = None):
    s_col = f"{env.lower()}_last_status"
    e_col = f"{env.lower()}_last_error"
    lr_col = f"{env.lower()}_last_run"
    nr_col = f"{env.lower()}_next_run"
    ns_col = f"{env.lower()}_next_skip"
    id_col = f"{env.lower()}_initial_done"

    sets = [f"{s_col}=%s", f"{e_col}=%s", f"{lr_col}=%s", f"{nr_col}=%s"]
    params: List[Any] = [status, (error or None), last_run, next_run]

    if next_skip is not None:
        sets.append(f"{ns_col}=%s"); params.append(int(next_skip))
    if initial_done is not None:
        sets.append(f"{id_col}=%s"); params.append(bool(initial_done))

    params.append(job_code)
    sql = f"UPDATE {REGISTRY_TABLE} SET {', '.join(sets)}, updated_at=now() WHERE job_code=%s"
    pg.run(sql, parameters=tuple(params))

# ---------- ClickHouse compatibility helpers ----------
def ck_exec(ch: ClickHouseHook, sql: str, params=None):
    if hasattr(ch, "execute"):
        try:
            return ch.execute(sql, params) if params is not None else ch.execute(sql)
        except TypeError:
            try:
                return ch.execute(sql)
            except Exception:
                pass
    if hasattr(ch, "run"):
        try:
            return ch.run(sql, parameters=params) if params is not None else ch.run(sql)
        except TypeError:
            try:
                return ch.run(sql)
            except Exception:
                pass
    conn = ch.get_conn()
    try:
        return conn.execute(sql, params) if params is not None else conn.execute(sql)
    except TypeError:
        return conn.execute(sql)

def ck_insert(ch: ClickHouseHook, sql: str, data):
    if hasattr(ch, "execute"):
        try:
            return ch.execute(sql, data)
        except TypeError:
            return ch.execute(sql)
    if hasattr(ch, "run"):
        try:
            return ch.run(sql, parameters=data)
        except TypeError:
            return ch.run(sql)
    conn = ch.get_conn()
    try:
        return conn.execute(sql, data)
    except TypeError:
        return conn.execute(sql)

def ck_query(ch: ClickHouseHook, sql: str):
    if hasattr(ch, "get_pandas_df"):
        df = ch.get_pandas_df(sql)
        return [tuple(row) for row in df.itertuples(index=False, name=None)]
    conn = ch.get_conn()
    return conn.execute(sql)

def ck_table_columns(ch: ClickHouseHook, full_table: str) -> list[str]:
    rows = ck_query(ch, f"DESCRIBE TABLE {full_table}")
    return [r[0] for r in rows]

# ---------- state helpers (persist PK cols) ----------
def _load_state(pg: PostgresHook, job_code: str) -> Dict[str, Any]:
    rows = fetch_dicts(pg, f"SELECT state_json FROM {REGISTRY_TABLE} WHERE job_code=%s", (job_code,))
    if not rows: return {}
    sj = rows[0].get("state_json")
    if not sj: return {}
    if isinstance(sj, dict): return sj
    try:
        return json.loads(sj)
    except Exception:
        return {}

def _save_state(pg: PostgresHook, job_code: str, state: Dict[str, Any]):
    pg.run(f"UPDATE {REGISTRY_TABLE} SET state_json=%s, updated_at=now() WHERE job_code=%s",
           parameters=(json.dumps(state), job_code))

# ===== DAG =====
with DAG(
    dag_id=DAG_ID,
    schedule=DEFAULT_SCHEDULE,
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    tags=["SAP","CDC","ClickHouse"],
) as dag:
    
    @task()
    def load_registry() -> list[dict]:
        LOGGER.info("Loading registry rows from %s.%s", REGISTRY_CONN_ID, REGISTRY_TABLE)
        hook = get_sql_hook(REGISTRY_CONN_ID)
        try:
            items = fetch_dicts(hook, f"SELECT * FROM {REGISTRY_TABLE} WHERE is_enabled = TRUE")
            LOGGER.info("Registry read: %d rows (is_enabled=TRUE)", len(items))
        except Exception as e:
            LOGGER.warning("Registry read with is_enabled failed (%s). Falling back to full select.", str(e))
            items = fetch_dicts(hook, f"SELECT * FROM {REGISTRY_TABLE}")
            before = len(items)
            items = [r for r in items if any(r.get(k) for k in ("sim_enabled","qas_enabled","prod_enabled"))]
            LOGGER.info("Registry fallback: %d rows total, %d rows after env-enabled filter", before, len(items))
        return items

    @task()
    def process_job(job: dict):
        ctx = get_current_context()
        dag_conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
        job_code = job["job_code"]

        random_value = random.randint(1000, 9999)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        RANDOM_VALUE = f"{random_value}_{timestamp}"

        # Read core settings
        method   = job["method"]
        stype    = job.get("schedule_type") or "interval"
        interval_minutes = int(job.get("interval_minutes") or 5)
        cron_expr = job.get("cron_expr")
        initial_path = job["initial_path"].strip()
        delta_path   = (job.get("delta_path") or initial_path).strip()
        top          = int(job.get("initial_top") or 5000)
        force_initial = bool(job.get("trigger_force_initial") or dag_conf.get("force_initial"))
        run_now       = bool(job.get("trigger_run_now") or dag_conf.get("run_now"))

        # One ClickHouse / one log DB from registry row
        ck_conn_id  = job.get("ck_conn_id")  or "clickhouse_default"
        log_conn_id = job.get("log_conn_id") or REGISTRY_CONN_ID
        ch = ClickHouseHook(clickhouse_conn_id=ck_conn_id)
        try:
            ck_exec(ch, "SELECT 1")
            LOGGER.info("ClickHouse connectivity OK via %s", ck_conn_id)
        except Exception:
            LOGGER.exception("ClickHouse connectivity FAILED via %s", ck_conn_id)
            raise
        log_hook = get_sql_hook(log_conn_id)
        pg = PostgresHook(postgres_conn_id=REGISTRY_CONN_ID)

        LOGGER.info("Job %s | method=%s schedule_type=%s interval=%s cron=%s ck_conn=%s log_conn=%s",
                    job_code, method, stype, interval_minutes, cron_expr, ck_conn_id, log_conn_id)

        prefix_map = _load_prefix_map(pg)
        log_event(log_hook, job_code, "ALL", "setup", RANDOM_VALUE, job.get("sim_db") or "", job.get("sim_table") or "", 
                f"prefixes={prefix_map}")

        # Load persisted state for PK cols (FIX)
        state = _load_state(pg, job_code)
        persisted_pk_cols: List[str] = state.get("pk_cols") or []

        envs = [
            ("SIM", bool(job.get("sim_enabled")), job.get("sim_db"), job.get("sim_table"), job.get("sim_client"), int(job.get("sim_next_skip") or 0), bool(job.get("sim_initial_done"))),
            ("QAS", bool(job.get("qas_enabled")), job.get("qas_db"), job.get("qas_table"), job.get("qas_client"), int(job.get("qas_next_skip") or 0), bool(job.get("qas_initial_done"))),
            ("PROD", bool(job.get("prod_enabled")), job.get("prod_db"), job.get("prod_table"), job.get("prod_client"), int(job.get("prod_next_skip") or 0), bool(job.get("prod_initial_done"))),
        ]

        for env, enabled, db, table, client, next_skip, initial_done in envs:
            if not enabled:
                LOGGER.info("Job %s env %s skipped (disabled)", job_code, env)
                continue

            # Start with persisted PK cols if available (FIX)
            pk_cols_current: List[str] = list(persisted_pk_cols)

            now = datetime.now()
            lr = job.get(f"{env.lower()}_last_run")
            nr = job.get(f"{env.lower()}_next_run")
            if not run_now:
                base_time = datetime.fromisoformat(lr) if isinstance(lr, str) and lr else now
                due = datetime.fromisoformat(nr) if isinstance(nr, str) and nr else _compute_next_run(stype, interval_minutes, cron_expr, base_time)
                if now < due:
                    log_event(log_hook, job_code, env, "schedule_skip",
                            RANDOM_VALUE, ck_db=db, ck_table=table,
                            msg=f"now={now} next_run={due}")
                    continue
            else:
                due = now
                LOGGER.info("Job %s env %s run_now=True (ignoring schedule)", job_code, env)

            base_url = prefix_map.get(env)
            if not base_url:
                msg = f"Missing base_url for {env}"
                LOGGER.error("Job %s env %s %s", job_code, env, msg)
                _update_env_runtime(pg, job_code, env, "error", msg, now, _compute_next_run(stype, interval_minutes, cron_expr, now))
                continue

            initial_url = _compose_url(base_url, initial_path, client)
            delta_url   = _compose_url(base_url, delta_path, client)
            auth = _auth(env)

            batch_id = f"{job_code}__{env}__{int(time.time())}"
            session = requests.Session()

            log_event(
                log_hook, job_code, env, "setup",
                mark=batch_id, ck_db=db, ck_table=table,
                msg=f"base_url={base_url} initial_url={initial_url} delta_url={delta_url} "
                    f"top={top} force_initial={force_initial} initial_done={initial_done}"
            )
            log_debug("HTTP auth resolved", env=env, user=auth[0])

            def maybe_persist_pk(pk_cols: List[str]):
                """Persist discovered PK cols once (FIX)"""
                nonlocal state, persisted_pk_cols
                if pk_cols and pk_cols != persisted_pk_cols:
                    state["pk_cols"] = pk_cols
                    _save_state(pg, job_code, state)
                    persisted_pk_cols = list(pk_cols)
                    LOGGER.info("Job %s persisted pk_cols=%s", job_code, pk_cols)

            with phase_logger(log_hook, f"{job_code}:{env}:run", batch_id, db, table):
                try:
                    if method == "weekly_refresh":
                        with phase_logger(log_hook, f"{job_code}:{env}:truncate", batch_id, db, table):
                            ck_exec(ch, f"CREATE DATABASE IF NOT EXISTS {db}")
                            ck_exec(ch, f"TRUNCATE TABLE IF EXISTS {db}.{table}")
                            LOGGER.info("Job %s env %s truncated %s.%s", job_code, env, db, table)
                        next_skip = 0
                        initial_done = False

                        with phase_logger(log_hook, f"{job_code}:{env}:initial", batch_id, db, table):
                            while True:
                                with page_logger(log_hook, job_code, env, "initial", batch_id, db, table, skip=next_skip, top=top) as page_ok:
                                    try:
                                        xml_bytes = _get_initial_page(session, initial_url, next_skip, top, auth)
                                    except Exception:
                                        LOGGER.exception("Job %s env %s initial HTTP error at skip=%s top=%s", job_code, env, next_skip, top)
                                        raise
                                    rows, _, _, pk_cols = _parse_feed(xml_bytes)
                                    if pk_cols and not pk_cols_current:
                                        pk_cols_current = pk_cols
                                        maybe_persist_pk(pk_cols_current)  # FIX
                                        LOGGER.info("Job %s env %s detected PK columns: %s", job_code, env, pk_cols_current)
                                    if DEBUG:
                                        snippet = xml_bytes[:200].decode('utf-8', errors='ignore')
                                        log_debug("Initial XML snippet", env=env, skip=next_skip, top=top, snippet=snippet)

                                    if next_skip == 0 and rows:
                                        _schema_sync(ch, db, table, rows, pk_cols=pk_cols_current or None)  # FIX
                                        LOGGER.info("Job %s env %s schema synced with %d columns", job_code, env, len(rows[0]))

                                    batch = len(rows)
                                    LOGGER.info("Job %s env %s initial page pulled=%d (skip=%d top=%d)", job_code, env, batch, next_skip, top)
                                    if batch == 0:
                                        page_ok(0); initial_done = True; break

                                    _insert_rows(ch, db, table, env, rows, batch_id, batch_row_count=batch, pk_cols=pk_cols_current or None)  # FIX
                                    LOGGER.info("Job %s env %s inserted %d rows into %s.%s", job_code, env, batch, db, table)

                                    page_ok(batch)
                                    next_skip += batch
                                    if batch < top:
                                        initial_done = True; break

                        last_run = now
                        next_run = _compute_next_run(stype, interval_minutes, cron_expr, last_run)
                        _update_env_runtime(pg, job_code, env, "success", None, last_run, next_run,
                                            next_skip=next_skip, initial_done=initial_done)
                        _set_next_delta_url(pg, job_code, env, None)
                        LOGGER.info("Job %s env %s completed weekly_refresh | next_skip=%d initial_done=%s next_run=%s",
                                    job_code, env, next_skip, initial_done, next_run)

                    else:
                        # continuous_cdc
                        if force_initial:
                            with phase_logger(log_hook, f"{job_code}:{env}:truncate", batch_id, db, table):
                                ck_exec(ch, f"CREATE DATABASE IF NOT EXISTS {db}")
                                ck_exec(ch, f"TRUNCATE TABLE IF EXISTS {db}.{table}")
                                LOGGER.info("Job %s env %s forced truncate %s.%s", job_code, env, db, table)
                            next_skip = 0
                            initial_done = False
                            _set_next_delta_url(pg, job_code, env, None)
                            state.pop(f"{env.lower()}_next_delta_url", None)
                            _save_state(pg, job_code, state)
                            LOGGER.info("Job %s env %s cleared delta pointer", job_code, env)

                        if not initial_done:
                            with phase_logger(log_hook, f"{job_code}:{env}:initial", batch_id, db, table):
                                while True:
                                    with page_logger(log_hook, job_code, env, "initial", batch_id, db, table, skip=next_skip, top=top) as page_ok:
                                        try:
                                            xml_bytes = _get_initial_page(session, initial_url, next_skip, top, auth)
                                        except Exception:
                                            LOGGER.exception("Job %s env %s initial HTTP error at skip=%s top=%s", job_code, env, next_skip, top)
                                            raise
                                        rows, _, _, pk_cols = _parse_feed(xml_bytes)
                                        if pk_cols and not pk_cols_current:
                                            pk_cols_current = pk_cols
                                            maybe_persist_pk(pk_cols_current)  # FIX
                                            LOGGER.info("Job %s env %s detected PK columns: %s", job_code, env, pk_cols_current)
                                        if DEBUG:
                                            snippet = xml_bytes[:200].decode('utf-8', errors='ignore')
                                            log_debug("Initial XML snippet", env=env, skip=next_skip, top=top, snippet=snippet)

                                        if next_skip == 0 and rows:
                                            _schema_sync(ch, db, table, rows, pk_cols=pk_cols_current or None)  # FIX
                                            LOGGER.info("Job %s env %s schema synced with %d columns", job_code, env, len(rows[0]))

                                        batch = len(rows)
                                        LOGGER.info("Job %s env %s initial page pulled=%d (skip=%d top=%d)", job_code, env, batch, next_skip, top)
                                        if batch == 0:
                                            page_ok(0); initial_done = True; break
                                        _insert_rows(ch, db, table, env, rows, batch_id, batch_row_count=batch, pk_cols=pk_cols_current or None)  # FIX
                                        LOGGER.info("Job %s env %s inserted %d rows into %s.%s", job_code, env, batch, db, table)
                                        page_ok(batch)
                                        next_skip += batch
                                        if batch < top:
                                            initial_done = True; break

                        with phase_logger(log_hook, f"{job_code}:{env}:delta", batch_id, db, table):
                            # DB-stored pointer takes precedence
                            next_delta_url = _get_next_delta_url(pg, job_code, env)

                            service_root: Optional[str] = None

                            if not next_delta_url:
                                xml_bytes = _get_url(session, delta_url, auth)
                                rows0, change_link, delta_token, pk_cols_next = _parse_feed(xml_bytes)
                                if pk_cols_next and not pk_cols_current:
                                    pk_cols_current = pk_cols_next
                                    maybe_persist_pk(pk_cols_current)  # FIX
                                    LOGGER.info("Job %s env %s detected PK columns (delta seed): %s", job_code, env, pk_cols_current)

                                service_root = _feed_service_root(xml_bytes, fallback_from_url=delta_url) or (base_url.rstrip("/") + "/")
                                if change_link:
                                    next_delta_url = change_link if change_link.startswith("http") else _abs_delta_url(service_root, change_link)
                                elif delta_token:
                                    sep = '&' if '?' in delta_url else '?'
                                    next_delta_url = f"{delta_url}{sep}$deltatoken={delta_token}"

                                _set_next_delta_url(pg, job_code, env, next_delta_url)
                                LOGGER.info("Job %s env %s seeded delta url: %s (service_root=%s)", job_code, env, next_delta_url, service_root)

                            if next_delta_url:
                                xml_bytes = _get_url(session, next_delta_url, auth)
                                rows, change_link_next, delta_token_next, pk_cols_next = _parse_feed(xml_bytes)
                                if pk_cols_next and not pk_cols_current:
                                    pk_cols_current = pk_cols_next
                                    maybe_persist_pk(pk_cols_current)  # FIX
                                    LOGGER.info("Job %s env %s detected PK columns (delta): %s", job_code, env, pk_cols_current)

                                if rows:
                                    _schema_sync(ch, db, table, rows, pk_cols=pk_cols_current or None)  # FIX
                                    _insert_rows(ch, db, table, env, rows, batch_id, batch_row_count=len(rows), pk_cols=pk_cols_current or None)  # FIX
                                    LOGGER.info("Job %s env %s inserted delta rows=%d into %s.%s", job_code, env, len(rows), db, table)

                                service_root_next = _feed_service_root(xml_bytes, fallback_from_url=delta_url) or (base_url.rstrip("/") + "/")

                                if change_link_next:
                                    next_delta_url = change_link_next if change_link_next.startswith("http") else _abs_delta_url(service_root_next, change_link_next)
                                elif delta_token_next:
                                    sep = '&' if '?' in delta_url else '?'
                                    next_delta_url = f"{delta_url}{sep}$deltatoken={delta_token_next}"

                                _set_next_delta_url(pg, job_code, env, next_delta_url)
                                LOGGER.info("Job %s env %s advanced delta pointer → %s", job_code, env, next_delta_url)

                        last_run = now
                        next_run = _compute_next_run(stype, interval_minutes, cron_expr, last_run)
                        _update_env_runtime(pg, job_code, env, "success", None, last_run, next_run,
                                            next_skip=next_skip, initial_done=initial_done)
                        LOGGER.info("Job %s env %s completed continuous_cdc | next_skip=%d initial_done=%s next_run=%s",
                                    job_code, env, next_skip, initial_done, next_run)

                except Exception as e:
                    LOGGER.exception("Job %s env %s failed", job_code, env)
                    last_run = now
                    next_run = _compute_next_run(stype, interval_minutes, cron_expr, last_run)
                    _update_env_runtime(pg, job_code, env, "error", str(e), last_run, next_run,
                                        next_skip=next_skip, initial_done=initial_done)
                    # phase_logger already wrote the 'error' row; continue other envs

        # reset one-shot triggers if set
        if job.get("trigger_force_initial") or job.get("trigger_run_now"):
            PostgresHook(postgres_conn_id=REGISTRY_CONN_ID).run(
                f"UPDATE {REGISTRY_TABLE} SET trigger_force_initial=FALSE, trigger_run_now=FALSE, updated_at=now() WHERE job_code=%s",
                parameters=(job_code,)
            )
        return True

    jobs = load_registry()
    process_job.expand(job=jobs)
