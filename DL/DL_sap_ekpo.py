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
BASE_URL = 'http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_EKPO_1_SRV/FactsOfZCDCEKPO?sap-client=300'
DELTA_DISCOVERY_URL = 'http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_EKPO_1_SRV/DeltaLinksOfFactsOfZCDCEKPO?sap-client=300'
NEXT_URL = 'http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_EKPO_1_SRV/'
HEADERS = {
    'Accept-Encoding': 'gzip',
    'Prefer': 'odata.track-changes,odata.maxpagesize=20000'
}
USERNAME = Variable.get("sap_user_prod")
PASSWORD = Variable.get("sap_pass_prod")
DELTA_LINK_PATH = '/tmp/sap_ekpo_delta_link.txt'
SKIP_TOKEN_PATH = '/tmp/sap_ekpo_skip_token.txt'
XML_DIR = '/tmp/sap_ekpo'
DAG_ID = "DL_sap_ekpo"
DAG_INTERVAL = "1-59/3 * * * *"
CLICKHOUSE_CONN_ID = "clickhouse_mitratel"
CLICKHOUSE_DATABASE = "sap"
CLICKHOUSE_TABLE = "ekpo"
LOG_CONN_ID = "airflow_logs_mitratel"
LOG_TABLE = "airflow_logs"
LOG_TYPE = "delta and skip compress"
LOG_KATEGORI = "Data Lake"
INSERT_QUERY = """
                    INSERT INTO `sap`.`ekpo`
                    (`EBELN`,`EBELP`,`UNIQUEID`,`LOEKZ`,`STATU`,`AEDAT`,`TXZ01`,`MATNR`,`EMATN`,`BUKRS`,`WERKS`,
                    `LGORT`,`BEDNR`,`MATKL`,`INFNR`,`IDNLF`,`KTMNG`,`MENGE`,`MEINS`,`BPRME`,`BPUMZ`,`BPUMN`,
                    `UMREZ`,`UMREN`,`NETPR`,`PEINH`,`NETWR`,`BRTWR`,`AGDAT`,`WEBAZ`,`MWSKZ`,`TXDATFROM`,`TXDAT`,
                    `TAXCOUNTRY`,`BONUS`,`INSMK`,`SPINF`,`PRSDR`,`SCHPR`,`MAHNZ`,`MAHN1`,`MAHN2`,`MAHN3`,
                    `UEBTO`,`UEBTK`,`UNTTO`,`BWTAR`,`BWTTY`,`ABSKZ`,`AGMEM`,`ELIKZ`,`EREKZ`,`PSTYP`,`KNTTP`,
                    `KZVBR`,`VRTKZ`,`TWRKZ`,`WEPOS`,`WEUNB`,`REPOS`,`WEBRE`,`KZABS`,`LABNR`,`KONNR`,`KTPNR`,
                    `ABDAT`,`ABFTZ`,`ETFZ1`,`ETFZ2`,`KZSTU`,`NOTKZ`,`LMEIN`,`EVERS`,`ZWERT`,`NAVNW`,`ABMNG`,
                    `PRDAT`,`BSTYP`,`EFFWR`,`XOBLR`,`KUNNR`,`ADRNR`,`EKKOL`,`SKTOF`,`STAFO`,`PLIFZ`,`NTGEW`,
                    `GEWEI`,`TXJCD`,`ETDRK`,`SOBKZ`,`ARSNR`,`ARSPS`,`INSNC`,`SSQSS`,`ZGTYP`,`EAN11`,`BSTAE`,
                    `REVLV`,`GEBER`,`FISTL`,`FIPOS`,`KOGSBER`,`KOPARGB`,`KOPRCTR`,`KOPPRCTR`,`MEPRF`,`BRGEW`,
                    `VOLUM`,`VOLEH`,`INCO1`,`INCO2`,`VORAB`,`KOLIF`,`LTSNR`,`PACKNO`,`FPLNR`,`GNETWR`,`STAPO`,
                    `UEBPO`,`LEWED`,`EMLIF`,`LBLKZ`,`SATNR`,`ATTYP`,`VSART`,`HANDOVERLOC`,`KANBA`,`ADRN2`,
                    `CUOBJ`,`XERSY`,`EILDT`,`DRDAT`,`DRUHR`,`DRUNR`,`AKTNR`,`ABELN`,`ABELP`,`ANZPU`,`PUNEI`,
                    `SAISO`,`SAISJ`,`EBON2`,`EBON3`,`EBONF`,`MLMAA`,`MHDRZ`,`ANFNR`,`ANFPS`,`KZKFG`,`USEQU`,
                    `UMSOK`,`BANFN`,`BNFPO`,`MTART`,`UPTYP`,`UPVOR`,`KZWI1`,`KZWI2`,`KZWI3`,`KZWI4`,`KZWI5`,
                    `KZWI6`,`SIKGR`,`MFZHI`,`FFZHI`,`RETPO`,`AUREL`,`BSGRU`,`LFRET`,`MFRGR`,`NRFHG`,`J1BNBM`,
                    `J1BMATUSE`,`J1BMATORG`,`J1BOWNPRO`,`J1BINDUST`,`ABUEB`,`NLABD`,`NFABD`,`KZBWS`,`BONBA`,
                    `FABKZ`,`LOADINGPOINT`,`J1AINDXP`,`J1AIDATEP`,`MPROF`,`EGLKZ`,`KZTLF`,`KZFME`,`RDPRF`,
                    `TECHS`,`CHGSRV`,`CHGFPLNR`,`MFRPN`,`MFRNR`,`EMNFR`,`NOVET`,`AFNAM`,`TZONRC`,`IPRKZ`,
                    `LEBRE`,`BERID`,`XCONDITIONS`,`APOMS`,`CCOMP`,`GRANTNBR`,`FKBER`,`STATUS`,`RESLO`,`KBLNR`,
                    `KBLPOS`,`PSPSPPNR`,`KOSTL`,`SAKTO`,`WEORA`,`SRVBASCOM`,`PRIOURG`,`PRIOREQ`,`EMPST`,
                    `DIFFINVOICE`,`TRMRISKRELEVANT`,`CREATIONDATE`,`CREATIONTIME`,`SPEABGRU`,`SPECRMSO`,
                    `SPECRMSOITEM`,`SPECRMREFSO`,`SPECRMREFITEM`,`SPECRMFKREL`,`SPECHNGSYS`,`SPEINSMKSRC`,
                    `SPECQCTRLTYPE`,`SPECQNOCQ`,`REASONCODE`,`CQUSAR`,`ANZSN`,`SPEEWMDTC`,`EXLIN`,`EXSNR`,
                    `EHTYP`,`RETPC`,`DPTYP`,`DPPCT`,`DPAMT`,`DPDAT`,`FLSRSTO`,`EXTRFXNUMBER`,`EXTRFXITEM`,
                    `EXTRFXSYSTEM`,`SRMCONTRACTID`,`SRMCONTRACTITM`,`BLKREASONID`,`BLKREASONTXT`,`ITCONS`,
                    `FIXMG`,`WABWE`,`CMPLDLVITM`,`INCO2L`,`INCO3L`,`STAWN`,`ISVCO`,`GRWRT`,`SERVICEPERFORMER`,
                    `PRODUCTTYPE`,`GRBYSES`,`REQUESTFORQUOTATION`,`REQUESTFORQUOTATIONITEM`,
                    `EXTMATERIALFORPURG`,`TARGETVALUE`,`EXTERNALREFERENCEID`,`TCAUTDET`,`MANUALTCREASON`,
                    `FISCALINCENTIVE`,`TAXSUBJECTST`,`FISCALINCENTIVEID`,`SFTXJCD`,`DUMMYEKPOINCLEEWPS`,
                    `EXPECTEDVALUE`,`LIMITAMOUNT`,`CONTRACTFORLIMIT`,`ENHDATE1`,`ENHDATE2`,`ENHPERCENT`,
                    `ENHNUMC1`,`DATAAGING`,`CUPIT`,`CIGIT`,`NEGENITEM`,`NEDEPFREE`,`NESTRUCCAT`,`ADVCODE`,
                    `BUDGETPD`,`EXCPE`,`FMFGUSKEY`,`IUIDRELEVANT`,`MRPIND`,`SGTSCAT`,`SGTRCAT`,`TMSREFUUID`,
                    `TMSSRCLOCKEY`,`TMSDESLOCKEY`,`WRFCHARSTC1`,`WRFCHARSTC2`,`WRFCHARSTC3`,`ZZTEST`,
                    `ZZHEADERID`,`ZZSPMK`,`ZZBYMHD`,`ZZSAKNR`,`REFSITE`,`ZAPCGK`,`APCGKEXTEND`,`ZBASDATE`,
                    `ZADATTYP`,`ZSTARTDAT`,`ZDEV`,`ZINDANX`,`ZLIMITDAT`,`NUMERATOR`,`HASHCALBDAT`,`HASHCAL`,
                    `NEGATIVE`,`HASHCALEXISTS`,`KNOWNINDEX`,`GPOSE`,`ANGPN`,`ADMOI`,`ADPRI`,`LPRIO`,`ADACN`,
                    `AFPNR`,`BSARK`,`AUDAT`,`ANGNR`,`PNSTAT`,`ADDNS`,`ASSIGNMENTPRIORITY`,`ARUNGROUPPRIO`,
                    `ARUNORDERPRIO`,`SERRU`,`SERNP`,`DISUBSOBKZ`,`DISUBPSPNR`,`DISUBKUNNR`,`DISUBVBELN`,
                    `DISUBPOSNR`,`DISUBOWNER`,`FSHSEASONYEAR`,`FSHSEASON`,`FSHCOLLECTION`,`FSHTHEME`,
                    `FSHATPDATE`,`FSHVASREL`,`FSHVASPRNTID`,`FSHTRANSACTION`,`FSHITEMGROUP`,`FSHITEM`,
                    `FSHSS`,`FSHGRIDCONDREC`,`FSHPSMPFMSPLIT`,`CNFMQTY`,`FSHPQRUEPOS`,`RFMDIVERSION`,
                    `RFMSCCINDICATOR`,`STPAC`,`LGBZO`,`LGBZOB`,`ADDRNUM`,`CONSNUM`,`BORGRMISS`,`DEPID`,
                    `BELNR`,`KBLPOSCAB`,`KBLNRCOMP`,`KBLPOSCOMP`,`WBSELEMENT`,`RFMPSSTRULE`,`RFMPSSTGROUP`,
                    `RFMREFDOC`,`RFMREFITEM`,`RFMREFACTION`,`RFMREFSLITEM`,`REFITEM`,`SOURCEID`,`SOURCEKEY`,
                    `PUTBACK`,`POLID`,`CONSORDER`,`ODQ_CHANGEMODE`,`ODQ_ENTITYCNTR`)
                    VALUES
                    (%(EBELN)s, %(EBELP)s, %(UNIQUEID)s, %(LOEKZ)s, %(STATU)s, %(AEDAT)s, %(TXZ01)s, 
                    %(MATNR)s, %(EMATN)s, %(BUKRS)s, %(WERKS)s, %(LGORT)s, %(BEDNR)s, %(MATKL)s, %(INFNR)s, 
                    %(IDNLF)s, %(KTMNG)s, %(MENGE)s, %(MEINS)s, %(BPRME)s, %(BPUMZ)s, %(BPUMN)s, %(UMREZ)s, 
                    %(UMREN)s, %(NETPR)s, %(PEINH)s, %(NETWR)s, %(BRTWR)s, %(AGDAT)s, %(WEBAZ)s, %(MWSKZ)s, 
                    %(TXDATFROM)s, %(TXDAT)s, %(TAXCOUNTRY)s, %(BONUS)s, %(INSMK)s, %(SPINF)s, %(PRSDR)s, 
                    %(SCHPR)s, %(MAHNZ)s, %(MAHN1)s, %(MAHN2)s, %(MAHN3)s, %(UEBTO)s, %(UEBTK)s, %(UNTTO)s, 
                    %(BWTAR)s, %(BWTTY)s, %(ABSKZ)s, %(AGMEM)s, %(ELIKZ)s, %(EREKZ)s, %(PSTYP)s, %(KNTTP)s, 
                    %(KZVBR)s, %(VRTKZ)s, %(TWRKZ)s, %(WEPOS)s, %(WEUNB)s, %(REPOS)s, %(WEBRE)s, %(KZABS)s, 
                    %(LABNR)s, %(KONNR)s, %(KTPNR)s, %(ABDAT)s, %(ABFTZ)s, %(ETFZ1)s, %(ETFZ2)s, %(KZSTU)s, 
                    %(NOTKZ)s, %(LMEIN)s, %(EVERS)s, %(ZWERT)s, %(NAVNW)s, %(ABMNG)s, %(PRDAT)s, %(BSTYP)s, 
                    %(EFFWR)s, %(XOBLR)s, %(KUNNR)s, %(ADRNR)s, %(EKKOL)s, %(SKTOF)s, %(STAFO)s, %(PLIFZ)s, 
                    %(NTGEW)s, %(GEWEI)s, %(TXJCD)s, %(ETDRK)s, %(SOBKZ)s, %(ARSNR)s, %(ARSPS)s, %(INSNC)s, 
                    %(SSQSS)s, %(ZGTYP)s, %(EAN11)s, %(BSTAE)s, %(REVLV)s, %(GEBER)s, %(FISTL)s, %(FIPOS)s, 
                    %(KOGSBER)s, %(KOPARGB)s, %(KOPRCTR)s, %(KOPPRCTR)s, %(MEPRF)s, %(BRGEW)s, %(VOLUM)s, 
                    %(VOLEH)s, %(INCO1)s, %(INCO2)s, %(VORAB)s, %(KOLIF)s, %(LTSNR)s, %(PACKNO)s, %(FPLNR)s, 
                    %(GNETWR)s, %(STAPO)s, %(UEBPO)s, %(LEWED)s, %(EMLIF)s, %(LBLKZ)s, %(SATNR)s, %(ATTYP)s, 
                    %(VSART)s, %(HANDOVERLOC)s, %(KANBA)s, %(ADRN2)s, %(CUOBJ)s, %(XERSY)s, %(EILDT)s, 
                    %(DRDAT)s, %(DRUHR)s, %(DRUNR)s, %(AKTNR)s, %(ABELN)s, %(ABELP)s, %(ANZPU)s, %(PUNEI)s, 
                    %(SAISO)s, %(SAISJ)s, %(EBON2)s, %(EBON3)s, %(EBONF)s, %(MLMAA)s, %(MHDRZ)s, %(ANFNR)s, 
                    %(ANFPS)s, %(KZKFG)s, %(USEQU)s, %(UMSOK)s, %(BANFN)s, %(BNFPO)s, %(MTART)s, %(UPTYP)s, 
                    %(UPVOR)s, %(KZWI1)s, %(KZWI2)s, %(KZWI3)s, %(KZWI4)s, %(KZWI5)s, %(KZWI6)s, %(SIKGR)s, 
                    %(MFZHI)s, %(FFZHI)s, %(RETPO)s, %(AUREL)s, %(BSGRU)s, %(LFRET)s, %(MFRGR)s, %(NRFHG)s, 
                    %(J1BNBM)s, %(J1BMATUSE)s, %(J1BMATORG)s, %(J1BOWNPRO)s, %(J1BINDUST)s, %(ABUEB)s, 
                    %(NLABD)s, %(NFABD)s, %(KZBWS)s, %(BONBA)s, %(FABKZ)s, %(LOADINGPOINT)s, %(J1AINDXP)s, 
                    %(J1AIDATEP)s, %(MPROF)s, %(EGLKZ)s, %(KZTLF)s, %(KZFME)s, %(RDPRF)s, %(TECHS)s, 
                    %(CHGSRV)s, %(CHGFPLNR)s, %(MFRPN)s, %(MFRNR)s, %(EMNFR)s, %(NOVET)s, %(AFNAM)s, 
                    %(TZONRC)s, %(IPRKZ)s, %(LEBRE)s, %(BERID)s, %(XCONDITIONS)s, %(APOMS)s, %(CCOMP)s, 
                    %(GRANTNBR)s, %(FKBER)s, %(STATUS)s, %(RESLO)s, %(KBLNR)s, %(KBLPOS)s, %(PSPSPPNR)s, 
                    %(KOSTL)s, %(SAKTO)s, %(WEORA)s, %(SRVBASCOM)s, %(PRIOURG)s, %(PRIOREQ)s, %(EMPST)s, 
                    %(DIFFINVOICE)s, %(TRMRISKRELEVANT)s, %(CREATIONDATE)s, %(CREATIONTIME)s, %(SPEABGRU)s, 
                    %(SPECRMSO)s, %(SPECRMSOITEM)s, %(SPECRMREFSO)s, %(SPECRMREFITEM)s, %(SPECRMFKREL)s, 
                    %(SPECHNGSYS)s, %(SPEINSMKSRC)s, %(SPECQCTRLTYPE)s, %(SPECQNOCQ)s, %(REASONCODE)s, 
                    %(CQUSAR)s, %(ANZSN)s, %(SPEEWMDTC)s, %(EXLIN)s, %(EXSNR)s, %(EHTYP)s, %(RETPC)s, 
                    %(DPTYP)s, %(DPPCT)s, %(DPAMT)s, %(DPDAT)s, %(FLSRSTO)s, %(EXTRFXNUMBER)s, %(EXTRFXITEM)s, 
                    %(EXTRFXSYSTEM)s, %(SRMCONTRACTID)s, %(SRMCONTRACTITM)s, %(BLKREASONID)s, %(BLKREASONTXT)s, 
                    %(ITCONS)s, %(FIXMG)s, %(WABWE)s, %(CMPLDLVITM)s, %(INCO2L)s, %(INCO3L)s, %(STAWN)s, 
                    %(ISVCO)s, %(GRWRT)s, %(SERVICEPERFORMER)s, %(PRODUCTTYPE)s, %(GRBYSES)s, 
                    %(REQUESTFORQUOTATION)s, %(REQUESTFORQUOTATIONITEM)s, %(EXTMATERIALFORPURG)s, 
                    %(TARGETVALUE)s, %(EXTERNALREFERENCEID)s, %(TCAUTDET)s, %(MANUALTCREASON)s, 
                    %(FISCALINCENTIVE)s, %(TAXSUBJECTST)s, %(FISCALINCENTIVEID)s, %(SFTXJCD)s, 
                    %(DUMMYEKPOINCLEEWPS)s, %(EXPECTEDVALUE)s, %(LIMITAMOUNT)s, %(CONTRACTFORLIMIT)s, 
                    %(ENHDATE1)s, %(ENHDATE2)s, %(ENHPERCENT)s, %(ENHNUMC1)s, %(DATAAGING)s, %(CUPIT)s, 
                    %(CIGIT)s, %(NEGENITEM)s, %(NEDEPFREE)s, %(NESTRUCCAT)s, %(ADVCODE)s, %(BUDGETPD)s, 
                    %(EXCPE)s, %(FMFGUSKEY)s, %(IUIDRELEVANT)s, %(MRPIND)s, %(SGTSCAT)s, %(SGTRCAT)s, 
                    %(TMSREFUUID)s, %(TMSSRCLOCKEY)s, %(TMSDESLOCKEY)s, %(WRFCHARSTC1)s, %(WRFCHARSTC2)s, 
                    %(WRFCHARSTC3)s, %(ZZTEST)s, %(ZZHEADERID)s, %(ZZSPMK)s, %(ZZBYMHD)s, %(ZZSAKNR)s, 
                    %(REFSITE)s, %(ZAPCGK)s, %(APCGKEXTEND)s, %(ZBASDATE)s, %(ZADATTYP)s, %(ZSTARTDAT)s, 
                    %(ZDEV)s, %(ZINDANX)s, %(ZLIMITDAT)s, %(NUMERATOR)s, %(HASHCALBDAT)s, %(HASHCAL)s, 
                    %(NEGATIVE)s, %(HASHCALEXISTS)s, %(KNOWNINDEX)s, %(GPOSE)s, %(ANGPN)s, %(ADMOI)s, 
                    %(ADPRI)s, %(LPRIO)s, %(ADACN)s, %(AFPNR)s, %(BSARK)s, %(AUDAT)s, %(ANGNR)s, %(PNSTAT)s, 
                    %(ADDNS)s, %(ASSIGNMENTPRIORITY)s, %(ARUNGROUPPRIO)s, %(ARUNORDERPRIO)s, %(SERRU)s, 
                    %(SERNP)s, %(DISUBSOBKZ)s, %(DISUBPSPNR)s, %(DISUBKUNNR)s, %(DISUBVBELN)s, %(DISUBPOSNR)s, 
                    %(DISUBOWNER)s, %(FSHSEASONYEAR)s, %(FSHSEASON)s, %(FSHCOLLECTION)s, %(FSHTHEME)s, 
                    %(FSHATPDATE)s, %(FSHVASREL)s, %(FSHVASPRNTID)s, %(FSHTRANSACTION)s, %(FSHITEMGROUP)s, 
                    %(FSHITEM)s, %(FSHSS)s, %(FSHGRIDCONDREC)s, %(FSHPSMPFMSPLIT)s, %(CNFMQTY)s, 
                    %(FSHPQRUEPOS)s, %(RFMDIVERSION)s, %(RFMSCCINDICATOR)s, %(STPAC)s, %(LGBZO)s, 
                    %(LGBZOB)s, %(ADDRNUM)s, %(CONSNUM)s, %(BORGRMISS)s, %(DEPID)s, %(BELNR)s, %(KBLPOSCAB)s, 
                    %(KBLNRCOMP)s, %(KBLPOSCOMP)s, %(WBSELEMENT)s, %(RFMPSSTRULE)s, %(RFMPSSTGROUP)s, 
                    %(RFMREFDOC)s, %(RFMREFITEM)s, %(RFMREFACTION)s, %(RFMREFSLITEM)s, %(REFITEM)s, 
                    %(SOURCEID)s, %(SOURCEKEY)s, %(PUTBACK)s, %(POLID)s, %(CONSORDER)s, %(ODQ_CHANGEMODE)s, 
                    %(ODQ_ENTITYCNTR)s)
                """
INSERT_QUERY_2 = """
                    INSERT INTO `sap`.`ekpo`
                    (`EBELN`,`EBELP`,`UNIQUEID`,`LOEKZ`,`STATU`,`AEDAT`,`TXZ01`,`MATNR`,`EMATN`,`BUKRS`,`WERKS`,
                    `LGORT`,`BEDNR`,`MATKL`,`INFNR`,`IDNLF`,`KTMNG`,`MENGE`,`MEINS`,`BPRME`,`BPUMZ`,`BPUMN`,
                    `UMREZ`,`UMREN`,`NETPR`,`PEINH`,`NETWR`,`BRTWR`,`AGDAT`,`WEBAZ`,`MWSKZ`,`TXDATFROM`,`TXDAT`,
                    `TAXCOUNTRY`,`BONUS`,`INSMK`,`SPINF`,`PRSDR`,`SCHPR`,`MAHNZ`,`MAHN1`,`MAHN2`,`MAHN3`,
                    `UEBTO`,`UEBTK`,`UNTTO`,`BWTAR`,`BWTTY`,`ABSKZ`,`AGMEM`,`ELIKZ`,`EREKZ`,`PSTYP`,`KNTTP`,
                    `KZVBR`,`VRTKZ`,`TWRKZ`,`WEPOS`,`WEUNB`,`REPOS`,`WEBRE`,`KZABS`,`LABNR`,`KONNR`,`KTPNR`,
                    `ABDAT`,`ABFTZ`,`ETFZ1`,`ETFZ2`,`KZSTU`,`NOTKZ`,`LMEIN`,`EVERS`,`ZWERT`,`NAVNW`,`ABMNG`,
                    `PRDAT`,`BSTYP`,`EFFWR`,`XOBLR`,`KUNNR`,`ADRNR`,`EKKOL`,`SKTOF`,`STAFO`,`PLIFZ`,`NTGEW`,
                    `GEWEI`,`TXJCD`,`ETDRK`,`SOBKZ`,`ARSNR`,`ARSPS`,`INSNC`,`SSQSS`,`ZGTYP`,`EAN11`,`BSTAE`,
                    `REVLV`,`GEBER`,`FISTL`,`FIPOS`,`KOGSBER`,`KOPARGB`,`KOPRCTR`,`KOPPRCTR`,`MEPRF`,`BRGEW`,
                    `VOLUM`,`VOLEH`,`INCO1`,`INCO2`,`VORAB`,`KOLIF`,`LTSNR`,`PACKNO`,`FPLNR`,`GNETWR`,`STAPO`,
                    `UEBPO`,`LEWED`,`EMLIF`,`LBLKZ`,`SATNR`,`ATTYP`,`VSART`,`HANDOVERLOC`,`KANBA`,`ADRN2`,
                    `CUOBJ`,`XERSY`,`EILDT`,`DRDAT`,`DRUHR`,`DRUNR`,`AKTNR`,`ABELN`,`ABELP`,`ANZPU`,`PUNEI`,
                    `SAISO`,`SAISJ`,`EBON2`,`EBON3`,`EBONF`,`MLMAA`,`MHDRZ`,`ANFNR`,`ANFPS`,`KZKFG`,`USEQU`,
                    `UMSOK`,`BANFN`,`BNFPO`,`MTART`,`UPTYP`,`UPVOR`,`KZWI1`,`KZWI2`,`KZWI3`,`KZWI4`,`KZWI5`,
                    `KZWI6`,`SIKGR`,`MFZHI`,`FFZHI`,`RETPO`,`AUREL`,`BSGRU`,`LFRET`,`MFRGR`,`NRFHG`,`J1BNBM`,
                    `J1BMATUSE`,`J1BMATORG`,`J1BOWNPRO`,`J1BINDUST`,`ABUEB`,`NLABD`,`NFABD`,`KZBWS`,`BONBA`,
                    `FABKZ`,`LOADINGPOINT`,`J1AINDXP`,`J1AIDATEP`,`MPROF`,`EGLKZ`,`KZTLF`,`KZFME`,`RDPRF`,
                    `TECHS`,`CHGSRV`,`CHGFPLNR`,`MFRPN`,`MFRNR`,`EMNFR`,`NOVET`,`AFNAM`,`TZONRC`,`IPRKZ`,
                    `LEBRE`,`BERID`,`XCONDITIONS`,`APOMS`,`CCOMP`,`GRANTNBR`,`FKBER`,`STATUS`,`RESLO`,`KBLNR`,
                    `KBLPOS`,`PSPSPPNR`,`KOSTL`,`SAKTO`,`WEORA`,`SRVBASCOM`,`PRIOURG`,`PRIOREQ`,`EMPST`,
                    `DIFFINVOICE`,`TRMRISKRELEVANT`,`CREATIONDATE`,`CREATIONTIME`,`SPEABGRU`,`SPECRMSO`,
                    `SPECRMSOITEM`,`SPECRMREFSO`,`SPECRMREFITEM`,`SPECRMFKREL`,`SPECHNGSYS`,`SPEINSMKSRC`,
                    `SPECQCTRLTYPE`,`SPECQNOCQ`,`REASONCODE`,`CQUSAR`,`ANZSN`,`SPEEWMDTC`,`EXLIN`,`EXSNR`,
                    `EHTYP`,`RETPC`,`DPTYP`,`DPPCT`,`DPAMT`,`DPDAT`,`FLSRSTO`,`EXTRFXNUMBER`,`EXTRFXITEM`,
                    `EXTRFXSYSTEM`,`SRMCONTRACTID`,`SRMCONTRACTITM`,`BLKREASONID`,`BLKREASONTXT`,`ITCONS`,
                    `FIXMG`,`WABWE`,`CMPLDLVITM`,`INCO2L`,`INCO3L`,`STAWN`,`ISVCO`,`GRWRT`,`SERVICEPERFORMER`,
                    `PRODUCTTYPE`,`GRBYSES`,`REQUESTFORQUOTATION`,`REQUESTFORQUOTATIONITEM`,
                    `EXTMATERIALFORPURG`,`TARGETVALUE`,`EXTERNALREFERENCEID`,`TCAUTDET`,`MANUALTCREASON`,
                    `FISCALINCENTIVE`,`TAXSUBJECTST`,`FISCALINCENTIVEID`,`SFTXJCD`,`DUMMYEKPOINCLEEWPS`,
                    `EXPECTEDVALUE`,`LIMITAMOUNT`,`CONTRACTFORLIMIT`,`ENHDATE1`,`ENHDATE2`,`ENHPERCENT`,
                    `ENHNUMC1`,`DATAAGING`,`CUPIT`,`CIGIT`,`NEGENITEM`,`NEDEPFREE`,`NESTRUCCAT`,`ADVCODE`,
                    `BUDGETPD`,`EXCPE`,`FMFGUSKEY`,`IUIDRELEVANT`,`MRPIND`,`SGTSCAT`,`SGTRCAT`,`TMSREFUUID`,
                    `TMSSRCLOCKEY`,`TMSDESLOCKEY`,`WRFCHARSTC1`,`WRFCHARSTC2`,`WRFCHARSTC3`,`ZZTEST`,
                    `ZZHEADERID`,`ZZSPMK`,`ZZBYMHD`,`ZZSAKNR`,`REFSITE`,`ZAPCGK`,`APCGKEXTEND`,`ZBASDATE`,
                    `ZADATTYP`,`ZSTARTDAT`,`ZDEV`,`ZINDANX`,`ZLIMITDAT`,`NUMERATOR`,`HASHCALBDAT`,`HASHCAL`,
                    `NEGATIVE`,`HASHCALEXISTS`,`KNOWNINDEX`,`GPOSE`,`ANGPN`,`ADMOI`,`ADPRI`,`LPRIO`,`ADACN`,
                    `AFPNR`,`BSARK`,`AUDAT`,`ANGNR`,`PNSTAT`,`ADDNS`,`ASSIGNMENTPRIORITY`,`ARUNGROUPPRIO`,
                    `ARUNORDERPRIO`,`SERRU`,`SERNP`,`DISUBSOBKZ`,`DISUBPSPNR`,`DISUBKUNNR`,`DISUBVBELN`,
                    `DISUBPOSNR`,`DISUBOWNER`,`FSHSEASONYEAR`,`FSHSEASON`,`FSHCOLLECTION`,`FSHTHEME`,
                    `FSHATPDATE`,`FSHVASREL`,`FSHVASPRNTID`,`FSHTRANSACTION`,`FSHITEMGROUP`,`FSHITEM`,
                    `FSHSS`,`FSHGRIDCONDREC`,`FSHPSMPFMSPLIT`,`CNFMQTY`,`FSHPQRUEPOS`,`RFMDIVERSION`,
                    `RFMSCCINDICATOR`,`STPAC`,`LGBZO`,`LGBZOB`,`ADDRNUM`,`CONSNUM`,`BORGRMISS`,`DEPID`,
                    `BELNR`,`KBLPOSCAB`,`KBLNRCOMP`,`KBLPOSCOMP`,`WBSELEMENT`,`RFMPSSTRULE`,`RFMPSSTGROUP`,
                    `RFMREFDOC`,`RFMREFITEM`,`RFMREFACTION`,`RFMREFSLITEM`,`REFITEM`,`SOURCEID`,`SOURCEKEY`,
                    `PUTBACK`,`POLID`,`CONSORDER`,`ODQ_CHANGEMODE`,`ODQ_ENTITYCNTR`)
                    VALUES
                """

os.makedirs(XML_DIR, exist_ok=True)

def sanitize_record(record):
    for key in ['EBELN', 'EBELP', 'UNIQUEID']:
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
                skiptoken_url = f"FactsOfZCDCEKPO?sap-client=300&$skiptoken={skiptoken}"
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
                ebeln = r['EBELN']
                ebelp = r['EBELP']
                uniqueid = r['UNIQUEID']
                DELETE_QUERY =  f"ALTER TABLE `sap`.`ekpo` DELETE WHERE EBELN = '{ebeln}' AND EBELP = '{ebelp}' AND UNIQUEID = '{uniqueid}'"
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
