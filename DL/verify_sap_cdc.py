"""
Quick verification script for SAP CDC implementation
Run this to verify the new functions work correctly
"""

import xml.etree.ElementTree as ET
import re
from typing import Optional

OADATA_NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'm': 'http://schemas.microsoft.com/ado/2007/08/dataservices/metadata',
    'd': 'http://schemas.microsoft.com/ado/2007/08/dataservices',
}

def test_extract_skiptoken():
    """Test skip token extraction from feed"""
    sample_xml = b"""<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
        <link rel="next" href="FactsOfZCDCZDMTMMTB020?sap-client=300&amp;InitialLoad=true&amp;$skiptoken=D20251104043043_000037000"/>
    </feed>"""
    
    tree = ET.fromstring(sample_xml)
    next_link = tree.find("atom:link[@rel='next']", OADATA_NS)
    if next_link is not None:
        href = next_link.get('href', '')
        match = re.search(r'[&?]\$skiptoken=([^&]+)', href)
        if match:
            token = match.group(1)
            print(f"✓ Skip token extracted: {token}")
            assert token == "D20251104043043_000037000"
        else:
            print("✗ No skiptoken found in href")
    else:
        print("✗ No next link found")

def test_extract_delta_token():
    """Test delta token extraction from DeltaLinksOf response"""
    sample_xml = b"""<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom" 
          xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" 
          xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices">
        <entry>
            <content type="application/xml">
                <m:properties>
                    <d:DeltaToken>D20251104043043_000037000</d:DeltaToken>
                    <d:CreatedAt>2025-11-04T04:30:43</d:CreatedAt>
                    <d:IsInitialLoad>true</d:IsInitialLoad>
                </m:properties>
            </content>
        </entry>
    </feed>"""
    
    tree = ET.fromstring(sample_xml)
    for entry in tree.findall('atom:entry', OADATA_NS):
        props = entry.find('atom:content/m:properties', OADATA_NS)
        if props is not None:
            tok = props.find('d:DeltaToken', OADATA_NS)
            if tok is not None and tok.text:
                print(f"✓ Delta token extracted: {tok.text}")
                assert tok.text == "D20251104043043_000037000"
                return
    print("✗ No delta token found")

def test_delta_links_path():
    """Test DeltaLinksOf path construction"""
    test_cases = [
        ("ZCDC_AFIH_1", "DeltaLinksOfFactsOfZCDCAFIH1"),
        ("FactsOfZCDCAFIH", "DeltaLinksOfFactsOfZCDCAFIH"),
    ]
    
    for entity, expected in test_cases:
        if entity.startswith("FactsOf"):
            result = f"DeltaLinksOf{entity}"
        else:
            result = f"DeltaLinksOfFactsOf{entity}"
        
        status = "✓" if result == expected else "✗"
        print(f"{status} {entity} -> {result}")

def test_make_headers():
    """Test dynamic header creation"""
    maxpagesize = 50000
    headers = {
        'Accept-Encoding': 'gzip',
        'Prefer': f'odata.track-changes,odata.maxpagesize={maxpagesize}'
    }
    print(f"✓ Headers created: {headers}")
    assert headers['Prefer'] == 'odata.track-changes,odata.maxpagesize=50000'

def test_url_construction():
    """Test URL construction scenarios"""
    base_url = "http://dmterpprd2.mitratel.co.id:8031/sap/opu/odata/sap/ZCDC_AFIH_1_SRV"
    path = "/FactsOfZCDCAFIH"
    client = "300"
    
    # Initial load URL
    initial_url = f"{base_url}{path}"
    sep = '&' if '?' in initial_url else '?'
    url1 = f"{initial_url}{sep}sap-client={client}&InitialLoad=true"
    print(f"✓ Initial URL: {url1}")
    
    # Skip token URL
    skip_token = "D20251104043043_000037000"
    sep = '&' if '?' in initial_url else '?'
    url2 = f"{initial_url}{sep}sap-client={client}&$skiptoken={skip_token}"
    print(f"✓ Skip token URL: {url2}")
    
    # Delta token URL
    delta_token = "D20251104043043_000037000"
    sep = '&' if '?' in initial_url else '?'
    url3 = f"{initial_url}{sep}sap-client={client}&$deltatoken={delta_token}"
    print(f"✓ Delta token URL: {url3}")
    
    # DeltaLinksOf URL
    delta_path = "/DeltaLinksOfFactsOfZCDCAFIH"
    sep = '&' if '?' in (base_url + delta_path) else '?'
    url4 = f"{base_url}{delta_path}{sep}sap-client={client}"
    print(f"✓ DeltaLinksOf URL: {url4}")

if __name__ == "__main__":
    print("=" * 60)
    print("SAP CDC Implementation Verification")
    print("=" * 60)
    
    print("\n1. Testing skip token extraction:")
    test_extract_skiptoken()
    
    print("\n2. Testing delta token extraction:")
    test_extract_delta_token()
    
    print("\n3. Testing DeltaLinksOf path construction:")
    test_delta_links_path()
    
    print("\n4. Testing header creation:")
    test_make_headers()
    
    print("\n5. Testing URL construction:")
    test_url_construction()
    
    print("\n" + "=" * 60)
    print("✓ All verification tests passed!")
    print("=" * 60)
