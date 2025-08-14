from airflow import DAG
import random
import logging
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import requests
import os
import glob
from lxml import etree
from requests.auth import HTTPBasicAuth
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
import urllib.parse
from airflow.models import Variable

# Constants
BASE_URL = 'http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH?sap-client=300'
DELTA_DISCOVERY_URL = 'http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/DeltaLinksOfFactsOfZCDCAFIH?sap-client=300'
NEXT_URL = 'http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/'
HEADERS = {
    'Accept-Encoding': 'gzip',
    'Prefer': 'odata.track-changes,odata.maxpagesize=100000'
}
USERNAME = Variable.get("sap_user_prod")
PASSWORD = Variable.get("sap_pass_prod")
DELTA_LINK_PATH = '/tmp/sap_afih_delta_link.txt'
SKIP_TOKEN_PATH = '/tmp/sap_afih_skip_token.txt'
XML_DIR = '/tmp/sap_afih'
DAG_ID = "DL_sap_afih"
DAG_INTERVAL = "*/3 * * * *"
CLICKHOUSE_CONN_ID = "clickhouse_mitratel"
CLICKHOUSE_DATABASE = "sap"
CLICKHOUSE_TABLE = "afih"
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "delta and skip compress"
LOG_KATEGORI = "Data Lake" 
TAGS = ["dl", "sap", "afih"]
INSERT_QUERY = """
                    INSERT INTO `sap`.`afih`
                    (`AUFNR`, `ARTPR`,`PRIOK`,`EQUNR`,`BAUTL`,`ILOAN`,`ILOAI`,`ANLZU`,`IWERK`,`INGPR`,`APGRP`,
                    `PMOBJTY`,`GEWRK`,`KUNUM`,`ANING`,`GAUZT`,`GAUEH`,`ANLBD`,`ANLVD`,`ANLBZ`,`ANLVZ`,
                    `INSPK`,`DATAN`,`WARPL`,`ABNUM`,`WAPOS`,`LAUFN`,`REVNR`,`ADDAT`,`ADUHR`,`IPHAS`,
                    `ILART`,`QMNUM`,`HISDA`,`AKKNZ`,`PLKNZ`,`SERIALNR`,`SERMAT`,`DEVICEID`,`SCREENTY`,
                    `ADPSP`,`RSUPG`,`CHANGEDDATETIME`,`MAINTORDPERSONRESPONSIBLE`,`MAINTORDOVRLPROCPHASE`,
                    `MAINTORDOVRLPROCSUBPHASE`,`DUMMYAFIHINCLEEWPS`,`OBJNR`,`MEQUI`,`MHIOADDATE`,`MHIOADTIME`,
                    `USERMODE`,`UII`,`DATAMODELVERSION`,`SERVICEDOCTYPE`,`SERVICEDOCID`,`SERVICEDOCITEMID`,
                    `ISBILLABLE`,`FLDLOGSDELIVISHELDONSHORE`,`ORCOD`,`PRMAN`,`PRVAL`,`TWTDE`,`MSTCK`,
                    `LACDDATE`,`OLDLACDDATE`,`ODQ_CHANGEMODE`,`ODQ_ENTITYCNTR`)
                    VALUES
                    (%(AUFNR)s, %(ARTPR)s, %(PRIOK)s, %(EQUNR)s, %(BAUTL)s, %(ILOAN)s, %(ILOAI)s, 
                    %(ANLZU)s, %(IWERK)s, %(INGPR)s, %(APGRP)s, %(PMOBJTY)s, %(GEWRK)s, %(KUNUM)s, 
                    %(ANING)s, %(GAUZT)s, %(GAUEH)s, %(ANLBD)s, %(ANLVD)s, %(ANLBZ)s, %(ANLVZ)s, 
                    %(INSPK)s, %(DATAN)s, %(WARPL)s, %(ABNUM)s, %(WAPOS)s, %(LAUFN)s, %(REVNR)s, 
                    %(ADDAT)s, %(ADUHR)s, %(IPHAS)s, %(ILART)s, %(QMNUM)s, %(HISDA)s, %(AKKNZ)s, 
                    %(PLKNZ)s, %(SERIALNR)s, %(SERMAT)s, %(DEVICEID)s, %(SCREENTY)s, %(ADPSP)s, 
                    %(RSUPG)s, %(CHANGEDDATETIME)s, %(MAINTORDPERSONRESPONSIBLE)s, 
                    %(MAINTORDOVRLPROCPHASE)s, %(MAINTORDOVRLPROCSUBPHASE)s, %(DUMMYAFIHINCLEEWPS)s, 
                    %(OBJNR)s, %(MEQUI)s, %(MHIOADDATE)s, %(MHIOADTIME)s, %(USERMODE)s, %(UII)s, 
                    %(DATAMODELVERSION)s, %(SERVICEDOCTYPE)s, %(SERVICEDOCID)s, %(SERVICEDOCITEMID)s, 
                    %(ISBILLABLE)s, %(FLDLOGSDELIVISHELDONSHORE)s, %(ORCOD)s, %(PRMAN)s, %(PRVAL)s, 
                    %(TWTDE)s, %(MSTCK)s, %(LACDDATE)s, %(OLDLACDDATE)s, %(ODQ_CHANGEMODE)s, 
                    %(ODQ_ENTITYCNTR)s)
                """
INSERT_QUERY_2 = """
                    INSERT INTO `sap`.`afih`
                    (`AUFNR`, `ARTPR`,`PRIOK`,`EQUNR`,`BAUTL`,`ILOAN`,`ILOAI`,`ANLZU`,`IWERK`,`INGPR`,`APGRP`,
                    `PMOBJTY`,`GEWRK`,`KUNUM`,`ANING`,`GAUZT`,`GAUEH`,`ANLBD`,`ANLVD`,`ANLBZ`,`ANLVZ`,
                    `INSPK`,`DATAN`,`WARPL`,`ABNUM`,`WAPOS`,`LAUFN`,`REVNR`,`ADDAT`,`ADUHR`,`IPHAS`,
                    `ILART`,`QMNUM`,`HISDA`,`AKKNZ`,`PLKNZ`,`SERIALNR`,`SERMAT`,`DEVICEID`,`SCREENTY`,
                    `ADPSP`,`RSUPG`,`CHANGEDDATETIME`,`MAINTORDPERSONRESPONSIBLE`,`MAINTORDOVRLPROCPHASE`,
                    `MAINTORDOVRLPROCSUBPHASE`,`DUMMYAFIHINCLEEWPS`,`OBJNR`,`MEQUI`,`MHIOADDATE`,`MHIOADTIME`,
                    `USERMODE`,`UII`,`DATAMODELVERSION`,`SERVICEDOCTYPE`,`SERVICEDOCID`,`SERVICEDOCITEMID`,
                    `ISBILLABLE`,`FLDLOGSDELIVISHELDONSHORE`,`ORCOD`,`PRMAN`,`PRVAL`,`TWTDE`,`MSTCK`,
                    `LACDDATE`,`OLDLACDDATE`,`ODQ_CHANGEMODE`,`ODQ_ENTITYCNTR`)
                    VALUES
                """

os.makedirs(XML_DIR, exist_ok=True)

def log_status(process_name, mark, status, error_message=None):
    """Insert or update the log table."""
    pg_hook = PostgresHook(postgres_conn_id=LOG_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    dag_name = f"{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}"
    
    if status == "pending":
        cursor.execute(
            f"""
            INSERT INTO {LOG_TABLE} (process_name, dag_name, type, status, start_time, mark, kategori)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (process_name, dag_name, LOG_TYPE, status, now, mark, LOG_KATEGORI)
        )
    else:
        cursor.execute(
            f"""
            UPDATE {LOG_TABLE} SET status = %s, end_time = %s, error_message = %s
            WHERE process_name = %s AND status = 'pending' AND dag_name = %s AND mark = %s AND kategori = %s
            """, (status, now, error_message, process_name, dag_name, mark, LOG_KATEGORI)
        )
    
    conn.commit()
    cursor.close()
    conn.close()

def choose_fetch_mode(**kwargs):
    random_value = random.randint(1000, 9999)  # Angka acak 4 digit
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")  # Format YYYYMMDDHHMMSS
    RANDOM_VALUE = f"{random_value}_{timestamp}"
    process_name = "choose_fetch_mode"
    log_status(process_name, RANDOM_VALUE, "pending")

    try:
        kwargs['ti'].xcom_push(key="random_value", value=RANDOM_VALUE)
        log_status(process_name, RANDOM_VALUE, "success")
    except Exception as e:
        log_status(process_name, RANDOM_VALUE, "failed", str(e))
        raise
    if os.path.exists(SKIP_TOKEN_PATH):
        return 'fetch_sap_cdc_initial'
    elif os.path.exists(DELTA_LINK_PATH):
        return 'fetch_sap_delta'
    else:
        return 'fetch_sap_cdc_initial'

def fetch_sap_cdc_initial(**kwargs):
    random_value = kwargs['ti'].xcom_pull(task_ids="branch_initial_or_delta", key="random_value")
    process_name = "fetch_sap_cdc_initial"
    log_status(process_name, random_value, "pending")

    try:
        # pattern = os.path.join(XML_DIR, 'batch*.xml')
        # for file_path in glob.glob(pattern):
        #     os.remove(file_path)

        # url = BASE_URL + '&InitialLoad=true'
        # idx = 0
        # while url:
        #     response = requests.get(url, headers=HEADERS, auth=HTTPBasicAuth(USERNAME, PASSWORD))
        #     response.raise_for_status()

        #     file_path = os.path.join(XML_DIR, f'batch_{idx}.xml')
        #     with open(file_path, 'wb') as f:
        #         f.write(response.content)

        #     root = etree.fromstring(response.content)
        #     ns = {'atom': 'http://www.w3.org/2005/Atom'}
        #     next_link = root.find("atom:link[@rel='next']", namespaces=ns)
        #     url = NEXT_URL + next_link.attrib['href'] if next_link is not None else None
        #     idx += 1
        # log_status(process_name, random_value, "success")
        pattern = os.path.join(XML_DIR, 'batch*.xml')
        for file_path in glob.glob(pattern):
            os.remove(file_path)

        if os.path.exists(SKIP_TOKEN_PATH):
            with open(SKIP_TOKEN_PATH, "r") as f:
                skiptoken = f.read().strip()
            next_url = NEXT_URL + skiptoken
        else:
            next_url = BASE_URL + '&InitialLoad=true'

        response = requests.get(next_url, headers=HEADERS, auth=HTTPBasicAuth(USERNAME, PASSWORD))
        if response.status_code != 200:
            raise Exception(f"Initial fetch failed: {response.text}")

        file_path = os.path.join(XML_DIR, 'batch_0.xml')
        with open(file_path, 'wb') as f:
            f.write(response.content)

        root = etree.fromstring(response.content)
        ns = {'atom': 'http://www.w3.org/2005/Atom'}
        next_link_el = root.find("atom:link[@rel='next']", namespaces=ns)
        if next_link_el is not None:
            next_href = next_link_el.attrib['href']
            parsed = urllib.parse.urlparse(next_href)
            qs = urllib.parse.parse_qs(parsed.query)
            skiptoken = qs.get('$skiptoken', [None])[0]
            if skiptoken:
                # Rebuild the skiptoken URL part
                skiptoken_url = f"FactsOfZCDCAFIH?sap-client=300&$skiptoken={skiptoken}"
                with open(SKIP_TOKEN_PATH, "w") as f:
                    f.write(skiptoken_url)
            else:
                # fallback: write the original next_href if $skiptoken not found
                with open(SKIP_TOKEN_PATH, "w") as f:
                    f.write(next_href)
        else:
            # Jika tidak ada lagi halaman selanjutnya, hapus skiptoken (reset)
            if os.path.exists(SKIP_TOKEN_PATH):
                os.remove(SKIP_TOKEN_PATH)
            # if os.path.exists(START_TOKEN_PATH):
                # os.remove(START_TOKEN_PATH)
        log_status(process_name, random_value, "success")
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise

def store_initial_delta_link(**kwargs):
    random_value = kwargs['ti'].xcom_pull(task_ids="branch_initial_or_delta", key="random_value")
    process_name = "store_initial_delta_link"
    log_status(process_name, random_value, "pending")

    if os.path.exists(DELTA_LINK_PATH):
        log_status(process_name, random_value, "success")
    else:
        try:
            response = requests.get(DELTA_DISCOVERY_URL, headers=HEADERS, auth=HTTPBasicAuth(USERNAME, PASSWORD))
            response.raise_for_status()

            root = etree.fromstring(response.content)

            # Use namespaces for accurate XML parsing
            ns = {
                'atom': 'http://www.w3.org/2005/Atom',
                'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
                'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices'
            }

            changes_after_href = None
            delta_token = None

            for entry in root.findall('atom:entry', namespaces=ns):
                # Find link containing "ChangesAfter"
                for link in entry.findall('atom:link', namespaces=ns):
                    href = link.attrib.get('href', '')
                    if 'ChangesAfter' in href:
                        changes_after_href = href
                        break

                # Get the DeltaToken
                props = entry.find('.//m:properties', namespaces=ns)
                if props is not None:
                    token_elem = props.find('d:DeltaToken', namespaces=ns)
                    if token_elem is not None:
                        delta_token = token_elem.text

                if changes_after_href and delta_token:
                    break  # Found everything we need

            if (changes_after_href and delta_token):
                # log_status(process_name, random_value, "failed", "Missing 'ChangesAfter' link or DeltaToken in delta discovery response.")
                # raise Exception("Missing 'ChangesAfter' link or DeltaToken in delta discovery response.")

                # Write both parts to file
                with open(DELTA_LINK_PATH, 'w') as f:
                    f.write(f"{changes_after_href}?sap-client=300&!deltatoken='{delta_token}'")

            log_status(process_name, random_value, "success")
        except Exception as e:
            log_status(process_name, random_value, "failed", str(e))
            raise
        
def fetch_sap_delta(**kwargs):
    random_value = kwargs['ti'].xcom_pull(task_ids="branch_initial_or_delta", key="random_value")
    process_name = "fetch_sap_delta"
    log_status(process_name, random_value, "pending")

    try:
        pattern = os.path.join(XML_DIR, 'delta*.xml')
        for file_path in glob.glob(pattern):
            os.remove(file_path)
            
        with open(DELTA_LINK_PATH, 'r') as f:
            url = NEXT_URL + f.read()

        idx = 0
        while url:
            response = requests.get(url, headers=HEADERS, auth=HTTPBasicAuth(USERNAME, PASSWORD))
            response.raise_for_status()

            file_path = os.path.join(XML_DIR, f'delta_{idx}.xml')
            with open(file_path, 'wb') as f:
                f.write(response.content)

            root = etree.fromstring(response.content)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            next_link = root.find("atom:link[@rel='delta']", namespaces=ns)
            if next_link is not None:
                with open(DELTA_LINK_PATH, 'w') as f:
                    u = next_link.attrib['href']
                    if "?!deltatoken=" in u:
                        u = u.replace("?!deltatoken=", "&!deltatoken=", 1)
                    f.write(u)
                break

            next_link = root.find("atom:link[@rel='next']", namespaces=ns)
            url = NEXT_URL + next_link.attrib['href'] if next_link is not None else None
            idx += 1
        log_status(process_name, random_value, "success")
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise

def parse_xml_init():
    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
        'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices'
    }
    records = []
    for filename in os.listdir(XML_DIR):
        if filename.startswith("batch") and filename.endswith(".xml"):
            with open(os.path.join(XML_DIR, filename), 'rb') as f:
                root = etree.parse(f).getroot()
                for entry in root.findall('atom:entry', namespaces=ns):
                    content = entry.find('atom:content', namespaces=ns)
                    properties = content.find('m:properties', namespaces=ns)
                    record = {elem.tag.split('}')[1]: elem.text for elem in properties}
                    record['ODQ_CHANGEMODE'] = record.get('ODQ_CHANGEMODE', 'I')
                    records.append(record)
    return records

def parse_xml():
    ns = {
        'atom': 'http://www.w3.org/2005/Atom',
        'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
        'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices'
    }
    records = []
    for filename in os.listdir(XML_DIR):
        if filename.startswith("delta") and filename.endswith(".xml"):
            with open(os.path.join(XML_DIR, filename), 'rb') as f:
                root = etree.parse(f).getroot()
                for entry in root.findall('atom:entry', namespaces=ns):
                    content = entry.find('atom:content', namespaces=ns)
                    properties = content.find('m:properties', namespaces=ns)
                    record = {elem.tag.split('}')[1]: elem.text for elem in properties}
                    record['ODQ_CHANGEMODE'] = record.get('ODQ_CHANGEMODE', 'I')
                    records.append(record)
    return records

def load_to_clickhouse_init(**kwargs):
    random_value = kwargs['ti'].xcom_pull(task_ids="branch_initial_or_delta", key="random_value")
    process_name = "load_to_clickhouse_init"
    log_status(process_name, random_value, "pending")

    try:
        client = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        records = parse_xml_init()

        # logging.warning(INSERT_QUERY, records[0])
        # for r in records:
        #     if r['ODQ_CHANGEMODE'] == 'D':
        #         aufnr = r['AUFNR']
        #         DELETE_QUERY =  f"ALTER TABLE `sap`.`afko` DELETE  WHERE AUFNR = '{aufnr}'"
        #         client.execute(DELETE_QUERY)
        #     else:
        #         client.execute(INSERT_QUERY, r)
        client.execute(INSERT_QUERY_2, records)
        
        log_status(process_name, random_value, "success")
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise
    
def load_to_clickhouse(**kwargs):
    random_value = kwargs['ti'].xcom_pull(task_ids="branch_initial_or_delta", key="random_value")
    process_name = "load_to_clickhouse"
    log_status(process_name, random_value, "pending")

    try:
        client = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        records = parse_xml()

        # logging.warning(INSERT_QUERY, records[0])
        for r in records:
            if r['ODQ_CHANGEMODE'] == 'D':
                aufnr = r['AUFNR']
                DELETE_QUERY =  f"ALTER TABLE `sap`.`afih` DELETE  WHERE AUFNR = '{aufnr}'"
                client.execute(DELETE_QUERY)
            else:
                client.execute(INSERT_QUERY, r)
        log_status(process_name, random_value, "success")
    except Exception as e:
        log_status(process_name, random_value, "failed", str(e))
        raise

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule_interval= DAG_INTERVAL,
    catchup=False, tags=TAGS,
    default_args={
        'owner': 'airflow', 
        # 'retries': 1, 
        # 'retry_delay': timedelta(minutes=5)
    }
) as dag:

    branch = BranchPythonOperator(
        task_id='branch_initial_or_delta',
        python_callable=choose_fetch_mode
    )

    initial_task = PythonOperator(
        task_id='fetch_sap_cdc_initial',
        python_callable=fetch_sap_cdc_initial
    )

    store_delta_link = PythonOperator(
        task_id='store_initial_delta_link',
        python_callable=store_initial_delta_link
    )

    fetch_delta_task = PythonOperator(
        task_id='fetch_sap_delta',
        python_callable=fetch_sap_delta
    )

    parse_task = PythonOperator(
        task_id='parse_and_load',
        python_callable=load_to_clickhouse
    )

    parse_task_init = PythonOperator(
        task_id='parse_and_load_init',
        python_callable=load_to_clickhouse_init
    )

    branch >> initial_task >> store_delta_link >> parse_task_init
    branch >> fetch_delta_task >> parse_task
