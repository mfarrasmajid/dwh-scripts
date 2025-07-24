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
from airflow.operators.bash import BashOperator
import urllib.parse
from airflow.models import Variable

# Constants
BASE_URL = 'http://dmterpsim.mitratel.co.id:8041/sap/opu/odata/sap/ZCDC_AFVC_2_SRV/FactsOfZCDCAFVC?sap-client=300'
DELTA_DISCOVERY_URL = 'http://dmterpsim.mitratel.co.id:8041/sap/opu/odata/sap/ZCDC_AFVC_2_SRV/DeltaLinksOfFactsOfZCDCAFVC?sap-client=300'
NEXT_URL = 'http://dmterpsim.mitratel.co.id:8041/sap/opu/odata/sap/ZCDC_AFVC_2_SRV/'
HEADERS = {
    'Accept-Encoding': 'gzip',
    'Prefer': 'odata.track-changes,odata.maxpagesize=5000'
}
USERNAME = Variable.get("sap_user_sim")
PASSWORD = Variable.get("sap_pass_sim")
DELTA_LINK_PATH = '/tmp/sapsim_afvc_delta_link.txt'
SKIP_TOKEN_PATH = '/tmp/sapsim_afvc_skip_token.txt'
XML_DIR = '/tmp/sapsim_afvc'
DAG_ID = "DL_sapsim_afvc"
DAG_INTERVAL = "2-59/5 17-22 * * *"
CLICKHOUSE_CONN_ID = "clickhouse_mitratel"
CLICKHOUSE_DATABASE = "sapsim"
CLICKHOUSE_TABLE = "afvc"
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "delta and skip compress"
LOG_KATEGORI = "Data Lake"
INSERT_QUERY = """
                    INSERT INTO `sapsim`.`afvc`
                    (`AUFPL`,`APLZL`,`PLNFL`,`PLNKN`,`PLNAL`,`PLNTY`,`VINTV`,`PLNNR`,`ZAEHL`,`VORNR`,
                    `STEUS`,`ARBID`,`PDEST`,`WERKS`,`KTSCH`,`LTXA1`,`LTXA2`,`TXTSP`,`VPLTY`,`VPLNR`,
                    `VPLAL`,`VPLFL`,`VGWTS`,`LAR01`,`LAR02`,`LAR03`,`LAR04`,`LAR05`,`LAR06`,`ZERMA`,
                    `ZGDAT`,`ZCODE`,`ZULNR`,`LOANZ`,`LOART`,`RSANZ`,`QUALF`,`ANZMA`,`RFGRP`,`RFSCH`,
                    `RASCH`,`AUFAK`,`LOGRP`,`UEMUS`,`UEKAN`,`FLIES`,`SPMUS`,`SPLIM`,`ABLIPKZ`,`RSTRA`,
                    `SUMNR`,`SORTL`,`LIFNR`,`PREIS`,`PEINH`,`SAKTO`,`WAERS`,`INFNR`,`ESOKZ`,`EKORG`,
                    `EKGRP`,`KZLGF`,`KZWRTF`,`MATKL`,`DDEHN`,`ANZZL`,`PRZNT`,`MLSTN`,`PPRIO`,`BUKRS`,
                    `ANFKO`,`ANFKOKRS`,`INDET`,`LARNT`,`PRKST`,`APLFL`,`RUECK`,`RMZHL`,`PROJN`,`OBJNR`,
                    `SPANZ`,`BEDID`,`BEDZL`,`BANFN`,`BNFPO`,`LEK01`,`LEK02`,`LEK03`,`LEK04`,`LEK05`,
                    `LEK06`,`SELKZ`,`KALID`,`FRSP`,`STDKN`,`ANLZU`,`ISTRU`,`ISTTY`,`ISTNR`,`ISTKN`,
                    `ISTPO`,`IUPOZ`,`EBORT`,`VERTL`,`LEKNW`,`NPRIO`,`PVZKN`,`PHFLG`,`PHSEQ`,`KNOBJ`,
                    `ERFSICHT`,`QPPKTABS`,`OTYPE`,`OBJEKTID`,`QLKAPAR`,`RSTUF`,`NPTXTKY`,`SUBSYS`,
                    `PSPNR`,`PACKNO`,`TXJCD`,`SCOPE`,`GSBER`,`PRCTR`,`NODISP`,`QKZPRZEIT`,`QKZZTMG1`,
                    `QKZPRMENG`,`QKZPRFREI`,`KZFEAT`,`QKZTLSBEST`,`AENNR`,`CUOBJARB`,`EVGEW`,`ARBII`,
                    `WERKI`,`CYSEQNRV`,`KAPTPUFFR`,`EBELN`,`EBELP`,`WEMPF`,`ABLAD`,`CLASF`,`FRUNV`,
                    `ZSCHL`,`KALSM`,`SCHEDEND`,`NETZKONT`,`OWAER`,`AFNAM`,`BEDNR`,`KZFIX`,`PERNR`,
                    `FRDLB`,`QPART`,`LOEKZ`,`WKURS`,`PRODACT`,`FPLNR`,`OBJTYPE`,`CHPROC`,`KLVAR`,
                    `KALNR`,`FORDN`,`FORDP`,`MATPRKST`,`PRZ01`,`RFPNT`,`FUNCAREA`,`TECHS`,`ADPSP`,
                    `RFIPPNT`,`MESOPERID`,`MESSTEPID`,`OANINSTIDSETUP`,`OANINSTIDPRODUCE`,
                    `OANINSTIDTEARDOWN`,`TLVERSN`,`VERSNTRACKGUID`,`DUMMYAFVCINCLEEWPS`,`CUGUID`,
                    `OBJNR_1`,`AFVCSTATUS`,`ADRNR`,`ADRN2`,`KUNN2`,`FLDLOGSDELIVISHELDONSHORE`,
                    `MILLOCAUFNRMO`,`WTYIND`,`TPLNR`,`EQUNR`,`MAINTOPEXECUTIONPHASECODE`,`CPDUPDAT`,
                    `ZZSPMKNO`,`ODQ_CHANGEMODE`,`ODQ_ENTITYCNTR`)
                    VALUES
                    (%(AUFPL)s, %(APLZL)s, %(PLNFL)s, %(PLNKN)s, %(PLNAL)s, %(PLNTY)s, %(VINTV)s, %(PLNNR)s, 
                    %(ZAEHL)s, %(VORNR)s, %(STEUS)s, %(ARBID)s, %(PDEST)s, %(WERKS)s, %(KTSCH)s, %(LTXA1)s, 
                    %(LTXA2)s, %(TXTSP)s, %(VPLTY)s, %(VPLNR)s, %(VPLAL)s, %(VPLFL)s, %(VGWTS)s, %(LAR01)s, 
                    %(LAR02)s, %(LAR03)s, %(LAR04)s, %(LAR05)s, %(LAR06)s, %(ZERMA)s, %(ZGDAT)s, %(ZCODE)s, 
                    %(ZULNR)s, %(LOANZ)s, %(LOART)s, %(RSANZ)s, %(QUALF)s, %(ANZMA)s, %(RFGRP)s, %(RFSCH)s, 
                    %(RASCH)s, %(AUFAK)s, %(LOGRP)s, %(UEMUS)s, %(UEKAN)s, %(FLIES)s, %(SPMUS)s, %(SPLIM)s, 
                    %(ABLIPKZ)s, %(RSTRA)s, %(SUMNR)s, %(SORTL)s, %(LIFNR)s, %(PREIS)s, %(PEINH)s, %(SAKTO)s, 
                    %(WAERS)s, %(INFNR)s, %(ESOKZ)s, %(EKORG)s, %(EKGRP)s, %(KZLGF)s, %(KZWRTF)s, %(MATKL)s, 
                    %(DDEHN)s, %(ANZZL)s, %(PRZNT)s, %(MLSTN)s, %(PPRIO)s, %(BUKRS)s, %(ANFKO)s, %(ANFKOKRS)s,
                    %(INDET)s, %(LARNT)s, %(PRKST)s, %(APLFL)s, %(RUECK)s, %(RMZHL)s, %(PROJN)s, %(OBJNR)s, 
                    %(SPANZ)s, %(BEDID)s, %(BEDZL)s, %(BANFN)s, %(BNFPO)s, %(LEK01)s, %(LEK02)s, %(LEK03)s, 
                    %(LEK04)s, %(LEK05)s, %(LEK06)s, %(SELKZ)s, %(KALID)s, %(FRSP)s, %(STDKN)s, %(ANLZU)s, 
                    %(ISTRU)s, %(ISTTY)s, %(ISTNR)s, %(ISTKN)s, %(ISTPO)s, %(IUPOZ)s, %(EBORT)s, %(VERTL)s, 
                    %(LEKNW)s, %(NPRIO)s, %(PVZKN)s, %(PHFLG)s, %(PHSEQ)s, %(KNOBJ)s, %(ERFSICHT)s, 
                    %(QPPKTABS)s, %(OTYPE)s, %(OBJEKTID)s, %(QLKAPAR)s, %(RSTUF)s, %(NPTXTKY)s, %(SUBSYS)s, 
                    %(PSPNR)s, %(PACKNO)s, %(TXJCD)s, %(SCOPE)s, %(GSBER)s, %(PRCTR)s, %(NODISP)s, 
                    %(QKZPRZEIT)s, %(QKZZTMG1)s, %(QKZPRMENG)s, %(QKZPRFREI)s, %(KZFEAT)s, %(QKZTLSBEST)s, 
                    %(AENNR)s, %(CUOBJARB)s, %(EVGEW)s, %(ARBII)s, %(WERKI)s, %(CYSEQNRV)s, %(KAPTPUFFR)s, 
                    %(EBELN)s, %(EBELP)s, %(WEMPF)s, %(ABLAD)s, %(CLASF)s, %(FRUNV)s, %(ZSCHL)s, %(KALSM)s, 
                    %(SCHEDEND)s, %(NETZKONT)s, %(OWAER)s, %(AFNAM)s, %(BEDNR)s, %(KZFIX)s, %(PERNR)s, 
                    %(FRDLB)s, %(QPART)s, %(LOEKZ)s, %(WKURS)s, %(PRODACT)s, %(FPLNR)s, %(OBJTYPE)s, 
                    %(CHPROC)s, %(KLVAR)s, %(KALNR)s, %(FORDN)s, %(FORDP)s, %(MATPRKST)s, %(PRZ01)s, 
                    %(RFPNT)s, %(FUNCAREA)s, %(TECHS)s, %(ADPSP)s, %(RFIPPNT)s, %(MESOPERID)s, %(MESSTEPID)s, 
                    %(OANINSTIDSETUP)s, %(OANINSTIDPRODUCE)s, %(OANINSTIDTEARDOWN)s, %(TLVERSN)s, 
                    %(VERSNTRACKGUID)s, %(DUMMYAFVCINCLEEWPS)s, %(CUGUID)s, %(OBJNR_1)s, %(AFVCSTATUS)s, 
                    %(ADRNR)s, %(ADRN2)s, %(KUNN2)s, %(FLDLOGSDELIVISHELDONSHORE)s, %(MILLOCAUFNRMO)s, 
                    %(WTYIND)s, %(TPLNR)s, %(EQUNR)s, %(MAINTOPEXECUTIONPHASECODE)s, %(CPDUPDAT)s, 
                    %(ZZSPMKNO)s, %(ODQ_CHANGEMODE)s, %(ODQ_ENTITYCNTR)s)
                """
INSERT_QUERY_2 = """
                    INSERT INTO `sapsim`.`afvc`
                    (`AUFPL`,`APLZL`,`PLNFL`,`PLNKN`,`PLNAL`,`PLNTY`,`VINTV`,`PLNNR`,`ZAEHL`,`VORNR`,
                    `STEUS`,`ARBID`,`PDEST`,`WERKS`,`KTSCH`,`LTXA1`,`LTXA2`,`TXTSP`,`VPLTY`,`VPLNR`,
                    `VPLAL`,`VPLFL`,`VGWTS`,`LAR01`,`LAR02`,`LAR03`,`LAR04`,`LAR05`,`LAR06`,`ZERMA`,
                    `ZGDAT`,`ZCODE`,`ZULNR`,`LOANZ`,`LOART`,`RSANZ`,`QUALF`,`ANZMA`,`RFGRP`,`RFSCH`,
                    `RASCH`,`AUFAK`,`LOGRP`,`UEMUS`,`UEKAN`,`FLIES`,`SPMUS`,`SPLIM`,`ABLIPKZ`,`RSTRA`,
                    `SUMNR`,`SORTL`,`LIFNR`,`PREIS`,`PEINH`,`SAKTO`,`WAERS`,`INFNR`,`ESOKZ`,`EKORG`,
                    `EKGRP`,`KZLGF`,`KZWRTF`,`MATKL`,`DDEHN`,`ANZZL`,`PRZNT`,`MLSTN`,`PPRIO`,`BUKRS`,
                    `ANFKO`,`ANFKOKRS`,`INDET`,`LARNT`,`PRKST`,`APLFL`,`RUECK`,`RMZHL`,`PROJN`,`OBJNR`,
                    `SPANZ`,`BEDID`,`BEDZL`,`BANFN`,`BNFPO`,`LEK01`,`LEK02`,`LEK03`,`LEK04`,`LEK05`,
                    `LEK06`,`SELKZ`,`KALID`,`FRSP`,`STDKN`,`ANLZU`,`ISTRU`,`ISTTY`,`ISTNR`,`ISTKN`,
                    `ISTPO`,`IUPOZ`,`EBORT`,`VERTL`,`LEKNW`,`NPRIO`,`PVZKN`,`PHFLG`,`PHSEQ`,`KNOBJ`,
                    `ERFSICHT`,`QPPKTABS`,`OTYPE`,`OBJEKTID`,`QLKAPAR`,`RSTUF`,`NPTXTKY`,`SUBSYS`,
                    `PSPNR`,`PACKNO`,`TXJCD`,`SCOPE`,`GSBER`,`PRCTR`,`NODISP`,`QKZPRZEIT`,`QKZZTMG1`,
                    `QKZPRMENG`,`QKZPRFREI`,`KZFEAT`,`QKZTLSBEST`,`AENNR`,`CUOBJARB`,`EVGEW`,`ARBII`,
                    `WERKI`,`CYSEQNRV`,`KAPTPUFFR`,`EBELN`,`EBELP`,`WEMPF`,`ABLAD`,`CLASF`,`FRUNV`,
                    `ZSCHL`,`KALSM`,`SCHEDEND`,`NETZKONT`,`OWAER`,`AFNAM`,`BEDNR`,`KZFIX`,`PERNR`,
                    `FRDLB`,`QPART`,`LOEKZ`,`WKURS`,`PRODACT`,`FPLNR`,`OBJTYPE`,`CHPROC`,`KLVAR`,
                    `KALNR`,`FORDN`,`FORDP`,`MATPRKST`,`PRZ01`,`RFPNT`,`FUNCAREA`,`TECHS`,`ADPSP`,
                    `RFIPPNT`,`MESOPERID`,`MESSTEPID`,`OANINSTIDSETUP`,`OANINSTIDPRODUCE`,
                    `OANINSTIDTEARDOWN`,`TLVERSN`,`VERSNTRACKGUID`,`DUMMYAFVCINCLEEWPS`,`CUGUID`,
                    `OBJNR_1`,`AFVCSTATUS`,`ADRNR`,`ADRN2`,`KUNN2`,`FLDLOGSDELIVISHELDONSHORE`,
                    `MILLOCAUFNRMO`,`WTYIND`,`TPLNR`,`EQUNR`,`MAINTOPEXECUTIONPHASECODE`,`CPDUPDAT`,
                    `ZZSPMKNO`,`ODQ_CHANGEMODE`,`ODQ_ENTITYCNTR`)
                    VALUES
                """

os.makedirs(XML_DIR, exist_ok=True)

def sanitize_record(record):
    for key in ['AUFPL', 'APLZL']:
        record[key] = record.get(key) or ''
    return record

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
                skiptoken_url = f"FactsOfZCDCAFVC?sap-client=300&$skiptoken={skiptoken}"
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
                    sanitize_record(record)
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
                    sanitize_record(record)
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
        #         objnr = r['OBJNR']
        #         kstar = r['KSTAR']
        #         abper = r['ABPER']
        #         abgjr = r['ABGJR']
        #         DELETE_QUERY =  f"ALTER TABLE `sap`.`fmzuob` DELETE WHERE OBJNR = '{objnr}' AND KSTAR = '{kstar}' AND ABPER = '{abper}' AND ABGJR = '{abgjr}'"
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
                aufpl = r['AUFPL']
                aplzl = r['APLZL']
                DELETE_QUERY =  f"ALTER TABLE `sapsim`.`afvc` DELETE WHERE AUFPL = '{aufpl}' AND APLZL = '{aplzl}'"
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
    catchup=False,
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
