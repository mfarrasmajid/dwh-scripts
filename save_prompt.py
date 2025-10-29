Now build me ingestion table and airflow pipeline for pulling data from SAP CDC to Clickhouse using AirFlow DAG which has requirement like below:

- SAP CDC first method have two mode of pulling data, first it pulls initial data from Link Initial, with skips and tops until there is no data, second it pulls delta from Link Delta, with it first get the initial delta URL, and then it pulls the delta data infinitely
- First the AirFlow pull initial data from Link Initial and initial delta link from Link Delta, then after link initial has no value anymore, it pulls the link delta instead
- Also there is second method, which is scheduled on some datetime of the week, it pulls initial data with Link Initial until there is none data, then stops, then after the scheduled datetime of the week it truncate the table, and then pull initial again until there is none left, then stops
- Give me selection for first method or second method
- Give me enable/disable for the table
- Give me trigger for running Initial from the start, even after it has been run for some time for initial / it has been running delta
- Give me trigger for running current process now, not waiting for schedule
- Give me column for data row count partition, how may rows pulled in one try
- There is 3 environment, SIM, QAS, and PROD, give me option on which target database/table for each environment, give me also the enable/disable for each environment and each of its last status/last error
- Link Initial is like this: http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH?sap-client=300
- Link Delta is like this:
http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_AFIH_1_SRV/FactsOfZCDCAFIH?sap-client=300
- Use header for http request like this:
HEADERS = {
    'Accept-Encoding': 'gzip',
    'Prefer': 'odata.track-changes'
}
- Auto create/auto alter database/table on each process, each environment, based on the column given in the SAP CDC result
- The Link Intial result is like this:
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xml:base="http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/">
    <id>http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/FactsOfZCDCZDMTMMTB020</id>
    <title type="text">FactsOfZCDCZDMTMMTB020</title>
    <updated>2025-10-27T07:50:51Z</updated>
    <author>
        <name/>
    </author>
    <link href="FactsOfZCDCZDMTMMTB020" rel="self" title="FactsOfZCDCZDMTMMTB020"/>
    <entry>
        <id>http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/FactsOfZCDCZDMTMMTB020(ZSPMKNO='0020%2FSPMK%2FDKA-a1000000%2FVIII%2F2024',ZSPMKITEM='%201')</id>
        <title type="text">FactsOfZCDCZDMTMMTB020(ZSPMKNO='0020%2FSPMK%2FDKA-a1000000%2FVIII%2F2024',ZSPMKITEM='%201')</title>
        <updated>2025-10-27T07:50:51Z</updated>
        <category term="ZCDC_ZDMT_MMTB020_2_SRV.FactsOfZCDCZDMTMMTB020" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme"/>
        <link href="FactsOfZCDCZDMTMMTB020(ZSPMKNO='0020%2FSPMK%2FDKA-a1000000%2FVIII%2F2024',ZSPMKITEM='%201')" rel="self" title="FactsOfZCDCZDMTMMTB020"/>
        <content type="application/xml">
            <m:properties xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices">
                <d:ZSPMKNO>0020/SPMK/DKA-a1000000/VIII/2024</d:ZSPMKNO>
                <d:ZSPMKITEM> 1</d:ZSPMKITEM>
                <d:MARK></d:MARK>
                <d:POST1>24TS08C0001</d:POST1>
                <d:PSPID>CM-2408-TS0001</d:PSPID>
                <d:NOWO>WO/TSEL/08/2024/065</d:NOWO>
                <d:SURVEY>0.00</d:SURVEY>
                <d:SITAC>0.00</d:SITAC>
                <d:IMB>0.00</d:IMB>
                <d:CME>16140491.00</d:CME>
                <d:PANEL>11000000.00</d:PANEL>
                <d:PLN>23686961.00</d:PLN>
                <d:MANAKONSTRUKSI>0.00</d:MANAKONSTRUKSI>
                <d:RELOPERANGKAT>0.00</d:RELOPERANGKAT>
                <d:MATPERKUATAN>0.00</d:MATPERKUATAN>
                <d:JASAPERKUATAN>0.00</d:JASAPERKUATAN>
                <d:DELFLAG></d:DELFLAG>
                <d:DISTANCE>0.00</d:DISTANCE>
                <d:MATERIALFO>0.00</d:MATERIALFO>
                <d:JASAFO>0.00</d:JASAFO>
                <d:ADDMATERIALFO>0.00</d:ADDMATERIALFO>
                <d:ADDJASA>0.00</d:ADDJASA>
                <d:PERIJINANFO>0.00</d:PERIJINANFO>
                <d:MANFEEFO>0.00</d:MANFEEFO>
                <d:WARRANTYFO>0.00</d:WARRANTYFO>
                <d:ODQ_CHANGEMODE></d:ODQ_CHANGEMODE>
                <d:ODQ_ENTITYCNTR>0</d:ODQ_ENTITYCNTR>
            </m:properties>
        </content>
    </entry>
</feed>
- The Link Delta result is like this:
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xml:base="http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/">
    <id>http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/DeltaLinksOfFactsOfZCDCZDMTMMTB020</id>
    <title type="text">DeltaLinksOfFactsOfZCDCZDMTMMTB020</title>
    <updated>2025-10-27T07:51:40Z</updated>
    <author>
        <name/>
    </author>
    <link href="DeltaLinksOfFactsOfZCDCZDMTMMTB020" rel="self" title="DeltaLinksOfFactsOfZCDCZDMTMMTB020"/>
    <entry>
        <id>http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_ZDMT_MMTB020_2_SRV/DeltaLinksOfFactsOfZCDCZDMTMMTB020('D20251027075026_000085000')</id>
        <title type="text">DeltaLinksOfFactsOfZCDCZDMTMMTB020('D20251027075026_000085000')</title>
        <updated>2025-10-27T07:51:40Z</updated>
        <category term="ZCDC_ZDMT_MMTB020_2_SRV.DeltaLinksOfFactsOfZCDCZDMTMMTB020" scheme="http://schemas.microsoft.com/ado/2007/08/dataservices/scheme"/>
        <link href="DeltaLinksOfFactsOfZCDCZDMTMMTB020('D20251027075026_000085000')" rel="edit" title="DeltaLinksOfFactsOfZCDCZDMTMMTB020"/>
        <link href="DeltaLinksOfFactsOfZCDCZDMTMMTB020('D20251027075026_000085000')/ChangesAfter" rel="http://schemas.microsoft.com/ado/2007/08/dataservices/related/ChangesAfter" type="application/atom+xml;type=feed" title="ChangesAfter"/>
        <content type="application/xml">
            <m:properties xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices">
                <d:DeltaToken>D20251027075026_000085000</d:DeltaToken>
                <d:CreatedAt>2025-10-27T07:50:26</d:CreatedAt>
                <d:IsInitialLoad>true</d:IsInitialLoad>
            </m:properties>
        </content>
    </entry>
</feed>
- Use basic auth on SAP CDC with AirFlow Variable which is different username/password for SIM/QAS/PROD
- Use _log_status like previous dags
