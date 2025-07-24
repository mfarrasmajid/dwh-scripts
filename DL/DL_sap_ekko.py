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
BASE_URL = 'http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_EKKO_2_SRV/FactsOfZCDCEKKO?sap-client=300'
DELTA_DISCOVERY_URL = 'http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_EKKO_2_SRV/DeltaLinksOfFactsOfZCDCEKKO?sap-client=300'
NEXT_URL = 'http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_EKKO_2_SRV/'
HEADERS = {
    'Accept-Encoding': 'gzip',
    'Prefer': 'odata.track-changes,odata.maxpagesize=50000'
}
USERNAME = Variable.get("sap_user_prod")
PASSWORD = Variable.get("sap_pass_prod")
DELTA_LINK_PATH = '/tmp/sap_ekko_delta_link.txt'
SKIP_TOKEN_PATH = '/tmp/sap_ekko_skip_token.txt'
XML_DIR = '/tmp/sap_ekko'
DAG_ID = "DL_sap_ekko"
DAG_INTERVAL = "*/3 0-10,13-23 * * *"
CLICKHOUSE_CONN_ID = "clickhouse_mitratel"
CLICKHOUSE_DATABASE = "sap"
CLICKHOUSE_TABLE = "ekko"
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "delta and skip"
LOG_KATEGORI = "Data Lake"
INSERT_QUERY = """
                    INSERT INTO `sap`.`ekko`
                    (`EBELN`,`BUKRS`,`BSTYP`,`BSART`,`BSAKZ`,`LOEKZ`,`STATU`,`AEDAT`,`ERNAM`,
                    `LASTCHANGEDATETIME`,`PINCR`,`LPONR`,`LIFNR`,`SPRAS`,`ZTERM`,`ZBD1T`,`ZBD2T`,
                    `ZBD3T`,`ZBD1P`,`ZBD2P`,`EKORG`,`EKGRP`,`WAERS`,`WKURS`,`KUFIX`,`BEDAT`,`KDATB`,
                    `KDATE`,`BWBDT`,`ANGDT`,`BNDDT`,`GWLDT`,`AUSNR`,`ANGNR`,`IHRAN`,`IHREZ`,`VERKF`,
                    `TELF1`,`LLIEF`,`KUNNR`,`ACTIVEID`,`KONNR`,`ABGRU`,`AUTLF`,`WEAKT`,`RESWK`,`LBLIF`,
                    `INCO1`,`INCO2`,`KTWRT`,`DISTRIBUTIONTYPE`,`SUBMI`,`KNUMV`,`KALSM`,`STAFO`,`LIFRE`,
                    `EXNUM`,`UNSEZ`,`LOGSY`,`UPINC`,`STAKO`,`FRGGR`,`FRGSX`,`FRGKE`,`FRGZU`,`FRGRL`,
                    `LANDS`,`LPHIS`,`ADRNR`,`STCEGL`,`STCEG`,`ABSGR`,`ADDNR`,`KORNR`,`MEMORY`,`PROCSTAT`,
                    `PROCESSINDICATOR`,`RLWRT`,`CRSTAT`,`REVNO`,`SCMPROC`,`REASONCODE`,`MEMORYTYPE`,
                    `RETTP`,`RETPC`,`DPTYP`,`DPPCT`,`DPAMT`,`DPDAT`,`MSRID`,`HIERARCHYEXISTS`,`GROUPINGID`,
                    `PARENTID`,`THRESHOLDEXISTS`,`LEGALCONTRACT`,`DESCRIPTION`,`RELEASEDATE`,`VSART`,
                    `HANDOVERLOC`,`SHIPCOND`,`INCOV`,`INCO2L`,`INCO3L`,`GRWCU`,`INTRAREL`,`INTRAEXCL`,
                    `QTNERLSTSUBMSNDATE`,`FOLLOWONDOCCAT`,`FOLLOWONDOCTYPE`,`DUMMYEKKOINCLEEWPS`,
                    `EXTERNALSYSTEM`,`EXTERNALREFERENCEID`,`EXTREVTMSTMP`,`ISEOPBLOCKED`,`ISAGED`,
                    `FORCEID`,`FORCECNT`,`RELOCID`,`RELOCSEQID`,`SOURCELOGSYS`,`FSHTRANSACTION`,
                    `FSHITEMGROUP`,`FSHVASLASTITEM`,`FSHOSSTGCHANGE`,`TMSREFUUID`,`ZZGRPMT`,`ZZVOLUM`,
                    `ZZPOUNT`,`ZZPOURN`,`ZZCTRNUM`,`ZZNOVOL`,`ZZHEADERID`,`ZZBYMHD`,`ZAPCGK`,`APCGKEXTEND`,
                    `ZBASDATE`,`ZADATTYP`,`ZSTARTDAT`,`ZDEV`,`ZINDANX`,`ZLIMITDAT`,`NUMERATOR`,`HASHCALBDAT`,
                    `HASHCAL`,`NEGATIVE`,`HASHCALEXISTS`,`KNOWNINDEX`,`POSTAT`,`VZSKZ`,`FSHSNSTSTATUS`,
                    `PROCE`,`CONC`,`CONT`,`COMP`,`OUTR`,`DESP`,`DESPDAT`,`DESPCARGO`,`PARE`,`PAREDAT`,
                    `PARECARGO`,`PFMCONTRACT`,`POHFTYPE`,`EQEINDT`,`EQWERKS`,`FIXPO`,`EKGRPALLOW`,
                    `WERKSALLOW`,`CONTRACTALLOW`,`PSTYPALLOW`,`FIXPOALLOW`,`KEYIDALLOW`,`AURELALLOW`,
                    `DELPERALLOW`,`EINDTALLOW`,`LTSNRALLOW`,`OTBLEVEL`,`OTBCONDTYPE`,`KEYID`,`OTBVALUE`,
                    `OTBCURR`,`OTBRESVALUE`,`OTBSPECVALUE`,`SPRRSNPROFILE`,`BUDGTYPE`,`OTBSTATUS`,
                    `OTBREASON`,`CHECKTYPE`,`CONOTBREQ`,`CONPREBOOKLEV`,`CONDISTRLEV`,`ODQ_CHANGEMODE`,
                    `ODQ_ENTITYCNTR`)
                    VALUES
                    (%(EBELN)s, %(BUKRS)s, %(BSTYP)s, %(BSART)s, %(BSAKZ)s, %(LOEKZ)s, %(STATU)s, %(AEDAT)s, 
                    %(ERNAM)s, %(LASTCHANGEDATETIME)s, %(PINCR)s, %(LPONR)s, %(LIFNR)s, %(SPRAS)s, %(ZTERM)s, 
                    %(ZBD1T)s, %(ZBD2T)s, %(ZBD3T)s, %(ZBD1P)s, %(ZBD2P)s, %(EKORG)s, %(EKGRP)s, %(WAERS)s, 
                    %(WKURS)s, %(KUFIX)s, %(BEDAT)s, %(KDATB)s, %(KDATE)s, %(BWBDT)s, %(ANGDT)s, %(BNDDT)s, 
                    %(GWLDT)s, %(AUSNR)s, %(ANGNR)s, %(IHRAN)s, %(IHREZ)s, %(VERKF)s, %(TELF1)s, %(LLIEF)s, 
                    %(KUNNR)s, %(ACTIVEID)s, %(KONNR)s, %(ABGRU)s, %(AUTLF)s, %(WEAKT)s, %(RESWK)s, %(LBLIF)s, 
                    %(INCO1)s, %(INCO2)s, %(KTWRT)s, %(DISTRIBUTIONTYPE)s, %(SUBMI)s, %(KNUMV)s, %(KALSM)s, 
                    %(STAFO)s, %(LIFRE)s, %(EXNUM)s, %(UNSEZ)s, %(LOGSY)s, %(UPINC)s, %(STAKO)s, %(FRGGR)s, 
                    %(FRGSX)s, %(FRGKE)s, %(FRGZU)s, %(FRGRL)s, %(LANDS)s, %(LPHIS)s, %(ADRNR)s, %(STCEGL)s, 
                    %(STCEG)s, %(ABSGR)s, %(ADDNR)s, %(KORNR)s, %(MEMORY)s, %(PROCSTAT)s, %(PROCESSINDICATOR)s, 
                    %(RLWRT)s, %(CRSTAT)s, %(REVNO)s, %(SCMPROC)s, %(REASONCODE)s, %(MEMORYTYPE)s, %(RETTP)s, 
                    %(RETPC)s, %(DPTYP)s, %(DPPCT)s, %(DPAMT)s, %(DPDAT)s, %(MSRID)s, %(HIERARCHYEXISTS)s, 
                    %(GROUPINGID)s, %(PARENTID)s, %(THRESHOLDEXISTS)s, %(LEGALCONTRACT)s, %(DESCRIPTION)s, 
                    %(RELEASEDATE)s, %(VSART)s, %(HANDOVERLOC)s, %(SHIPCOND)s, %(INCOV)s, %(INCO2L)s, 
                    %(INCO3L)s, %(GRWCU)s, %(INTRAREL)s, %(INTRAEXCL)s, %(QTNERLSTSUBMSNDATE)s, 
                    %(FOLLOWONDOCCAT)s, %(FOLLOWONDOCTYPE)s, %(DUMMYEKKOINCLEEWPS)s, %(EXTERNALSYSTEM)s, 
                    %(EXTERNALREFERENCEID)s, %(EXTREVTMSTMP)s, %(ISEOPBLOCKED)s, %(ISAGED)s, %(FORCEID)s, 
                    %(FORCECNT)s, %(RELOCID)s, %(RELOCSEQID)s, %(SOURCELOGSYS)s, %(FSHTRANSACTION)s, 
                    %(FSHITEMGROUP)s, %(FSHVASLASTITEM)s, %(FSHOSSTGCHANGE)s, %(TMSREFUUID)s, %(ZZGRPMT)s, 
                    %(ZZVOLUM)s, %(ZZPOUNT)s, %(ZZPOURN)s, %(ZZCTRNUM)s, %(ZZNOVOL)s, %(ZZHEADERID)s, 
                    %(ZZBYMHD)s, %(ZAPCGK)s, %(APCGKEXTEND)s, %(ZBASDATE)s, %(ZADATTYP)s, %(ZSTARTDAT)s, 
                    %(ZDEV)s, %(ZINDANX)s, %(ZLIMITDAT)s, %(NUMERATOR)s, %(HASHCALBDAT)s, %(HASHCAL)s, 
                    %(NEGATIVE)s, %(HASHCALEXISTS)s, %(KNOWNINDEX)s, %(POSTAT)s, %(VZSKZ)s, %(FSHSNSTSTATUS)s, 
                    %(PROCE)s, %(CONC)s, %(CONT)s, %(COMP)s, %(OUTR)s, %(DESP)s, %(DESPDAT)s, %(DESPCARGO)s, 
                    %(PARE)s, %(PAREDAT)s, %(PARECARGO)s, %(PFMCONTRACT)s, %(POHFTYPE)s, %(EQEINDT)s, 
                    %(EQWERKS)s, %(FIXPO)s, %(EKGRPALLOW)s, %(WERKSALLOW)s, %(CONTRACTALLOW)s, %(PSTYPALLOW)s, 
                    %(FIXPOALLOW)s, %(KEYIDALLOW)s, %(AURELALLOW)s, %(DELPERALLOW)s, %(EINDTALLOW)s, 
                    %(LTSNRALLOW)s, %(OTBLEVEL)s, %(OTBCONDTYPE)s, %(KEYID)s, %(OTBVALUE)s, %(OTBCURR)s, 
                    %(OTBRESVALUE)s, %(OTBSPECVALUE)s, %(SPRRSNPROFILE)s, %(BUDGTYPE)s, %(OTBSTATUS)s, 
                    %(OTBREASON)s, %(CHECKTYPE)s, %(CONOTBREQ)s, %(CONPREBOOKLEV)s, %(CONDISTRLEV)s, 
                    %(ODQ_CHANGEMODE)s, %(ODQ_ENTITYCNTR)s)
                """
INSERT_QUERY_2 = """
                    INSERT INTO `sap`.`ekko`
                    (`EBELN`,`BUKRS`,`BSTYP`,`BSART`,`BSAKZ`,`LOEKZ`,`STATU`,`AEDAT`,`ERNAM`,
                    `LASTCHANGEDATETIME`,`PINCR`,`LPONR`,`LIFNR`,`SPRAS`,`ZTERM`,`ZBD1T`,`ZBD2T`,
                    `ZBD3T`,`ZBD1P`,`ZBD2P`,`EKORG`,`EKGRP`,`WAERS`,`WKURS`,`KUFIX`,`BEDAT`,`KDATB`,
                    `KDATE`,`BWBDT`,`ANGDT`,`BNDDT`,`GWLDT`,`AUSNR`,`ANGNR`,`IHRAN`,`IHREZ`,`VERKF`,
                    `TELF1`,`LLIEF`,`KUNNR`,`ACTIVEID`,`KONNR`,`ABGRU`,`AUTLF`,`WEAKT`,`RESWK`,`LBLIF`,
                    `INCO1`,`INCO2`,`KTWRT`,`DISTRIBUTIONTYPE`,`SUBMI`,`KNUMV`,`KALSM`,`STAFO`,`LIFRE`,
                    `EXNUM`,`UNSEZ`,`LOGSY`,`UPINC`,`STAKO`,`FRGGR`,`FRGSX`,`FRGKE`,`FRGZU`,`FRGRL`,
                    `LANDS`,`LPHIS`,`ADRNR`,`STCEGL`,`STCEG`,`ABSGR`,`ADDNR`,`KORNR`,`MEMORY`,`PROCSTAT`,
                    `PROCESSINDICATOR`,`RLWRT`,`CRSTAT`,`REVNO`,`SCMPROC`,`REASONCODE`,`MEMORYTYPE`,
                    `RETTP`,`RETPC`,`DPTYP`,`DPPCT`,`DPAMT`,`DPDAT`,`MSRID`,`HIERARCHYEXISTS`,`GROUPINGID`,
                    `PARENTID`,`THRESHOLDEXISTS`,`LEGALCONTRACT`,`DESCRIPTION`,`RELEASEDATE`,`VSART`,
                    `HANDOVERLOC`,`SHIPCOND`,`INCOV`,`INCO2L`,`INCO3L`,`GRWCU`,`INTRAREL`,`INTRAEXCL`,
                    `QTNERLSTSUBMSNDATE`,`FOLLOWONDOCCAT`,`FOLLOWONDOCTYPE`,`DUMMYEKKOINCLEEWPS`,
                    `EXTERNALSYSTEM`,`EXTERNALREFERENCEID`,`EXTREVTMSTMP`,`ISEOPBLOCKED`,`ISAGED`,
                    `FORCEID`,`FORCECNT`,`RELOCID`,`RELOCSEQID`,`SOURCELOGSYS`,`FSHTRANSACTION`,
                    `FSHITEMGROUP`,`FSHVASLASTITEM`,`FSHOSSTGCHANGE`,`TMSREFUUID`,`ZZGRPMT`,`ZZVOLUM`,
                    `ZZPOUNT`,`ZZPOURN`,`ZZCTRNUM`,`ZZNOVOL`,`ZZHEADERID`,`ZZBYMHD`,`ZAPCGK`,`APCGKEXTEND`,
                    `ZBASDATE`,`ZADATTYP`,`ZSTARTDAT`,`ZDEV`,`ZINDANX`,`ZLIMITDAT`,`NUMERATOR`,`HASHCALBDAT`,
                    `HASHCAL`,`NEGATIVE`,`HASHCALEXISTS`,`KNOWNINDEX`,`POSTAT`,`VZSKZ`,`FSHSNSTSTATUS`,
                    `PROCE`,`CONC`,`CONT`,`COMP`,`OUTR`,`DESP`,`DESPDAT`,`DESPCARGO`,`PARE`,`PAREDAT`,
                    `PARECARGO`,`PFMCONTRACT`,`POHFTYPE`,`EQEINDT`,`EQWERKS`,`FIXPO`,`EKGRPALLOW`,
                    `WERKSALLOW`,`CONTRACTALLOW`,`PSTYPALLOW`,`FIXPOALLOW`,`KEYIDALLOW`,`AURELALLOW`,
                    `DELPERALLOW`,`EINDTALLOW`,`LTSNRALLOW`,`OTBLEVEL`,`OTBCONDTYPE`,`KEYID`,`OTBVALUE`,
                    `OTBCURR`,`OTBRESVALUE`,`OTBSPECVALUE`,`SPRRSNPROFILE`,`BUDGTYPE`,`OTBSTATUS`,
                    `OTBREASON`,`CHECKTYPE`,`CONOTBREQ`,`CONPREBOOKLEV`,`CONDISTRLEV`,`ODQ_CHANGEMODE`,
                    `ODQ_ENTITYCNTR`)
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
                skiptoken_url = f"FactsOfZCDCEKKO?sap-client=300&$skiptoken={skiptoken}"
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
                ebeln = r['EBELN']
                DELETE_QUERY =  f"ALTER TABLE `sap`.`ekko` DELETE  WHERE EBELN = '{ebeln}'"
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
