import pandas as pd
import requests
from bs4 import BeautifulSoup
import csv
import os

input_csv = "filtered_form_index.csv"
output_csv = "13F_positions.csv"
base_url = "https://www.sec.gov/Archives/"
target_cusip_prefixes = {"225447", "977852"}
headers = {"User-Agent": "MyApp/2.0 (myemail@example.com)"}

# Headers for 13F output
fields_13f = [
    "doc_type", "company_name", "filing_date", "nameofissuer",
    "titleofclass", "cusip", "value", "sshPrnamt", "sshprnamttype",
    "investmentDiscretion", "putCall"
]

def extract_13f_positions(content, company_name, filing_date):
    positions = []
    soup = BeautifulSoup(content, "html.parser")
    info_tables = soup.find_all("infotable")
    for table in info_tables:
        cusip = table.find("cusip").text.strip() if table.find("cusip") else None
        if cusip and any(cusip.startswith(prefix) for prefix in target_cusip_prefixes):
            positions.append({
                "doc_type": "13F-HR",
                "company_name": company_name,
                "filing_date": filing_date,
                "nameofissuer": table.find("nameofissuer").text.strip() if table.find("nameofissuer") else "N/A",
                "titleofclass": table.find("titleofclass").text.strip() if table.find("titleofclass") else "N/A",
                "cusip": cusip,
                "value": table.find("value").text.strip() if table.find("value") else "N/A",
                "sshPrnamt": table.find("shrsorprnamt").find("sshprnamt").text.strip() if table.find("shrsorprnamt") else "N/A",
                "sshprnamttype": table.find("shrsorprnamt").find("sshprnamttype").text.strip() if table.find("shrsorprnamt") else "N/A",
                "investmentDiscretion": table.find("investmentdiscretion").text.strip() if table.find("investmentdiscretion") else "N/A",
                "putCall": table.find("putcall").text.strip() if table.find("putcall") else "N/A"
            })
    return positions

def process_13f_file(file_url, company_name, filing_date):
    try:
        response = requests.get(file_url, headers=headers)
        response.raise_for_status()
        return extract_13f_positions(response.content.decode("utf-8"), company_name, filing_date)
    except Exception as e:
        print(f"Error processing 13F-HR file {file_url}: {e}")
        return []

def write_positions_to_csv(positions):
    if not positions:
        return
    write_header = not os.path.exists(output_csv)
    with open(output_csv, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields_13f)
        if write_header:
            writer.writeheader()
        writer.writerows(positions)

# Process 13F-HR filings
df = pd.read_csv(input_csv)
for _, row in df.iterrows():
    if row["Form Type"] == "13F-HR":
        file_url = f"{base_url}{row['File Name']}"
        positions = process_13f_file(file_url, row["Company Name"], row["Date Filed"])
        write_positions_to_csv(positions)
