import requests
import csv
import os
from lxml import etree
import gzip
import time
import pandas as pd


# File paths and constants
input_csv = "filtered_form_index.csv"
output_csv = "13F_positions.csv"
base_url = "https://www.sec.gov/Archives/"
headers = {"User-Agent": "MyApp/2.0 (myemail@example.com)"}

fields_13f = [
    "doc_type", "company_name", "filing_date", "nameofissuer",
    "titleofclass", "cusip", "value", "sshPrnamt", "sshprnamttype",
    "investmentDiscretion", "putCall","soleVotingAuthority",
    "sharedVotingAuthority", "noneVotingAuthority"
]

target_cusip_prefixes = {"225447", "977852"}

# Namespace and tags
NAMESPACE = "http://www.sec.gov/edgar/document/thirteenf/informationtable"
EDGAR_SUBMISSION_TAG = f"{{{NAMESPACE}}}edgarSubmission"
INFORMATION_TABLE_TAG = f"{{{NAMESPACE}}}informationTable"
INFOTABLE_TAG = f"{{{NAMESPACE}}}infoTable"
NAMEOFISSUER_TAG = f"{{{NAMESPACE}}}nameOfIssuer"
TITLEOFCLASS_TAG = f"{{{NAMESPACE}}}titleOfClass"
CUSIP_TAG = f"{{{NAMESPACE}}}cusip"
VALUE_TAG = f"{{{NAMESPACE}}}value"
SHRSORPRNAMT_TAG = f"{{{NAMESPACE}}}shrsOrPrnAmt"
SSHPRNAMT_TAG = f"{{{NAMESPACE}}}sshPrnamt"
SSHPRNAMTTYPE_TAG = f"{{{NAMESPACE}}}sshPrnamTtype"
INVESTMENTDISCRETION_TAG = f"{{{NAMESPACE}}}investmentDiscretion"
PUTCALL_TAG = f"{{{NAMESPACE}}}putCall"
SOLE_VOTING_TAG = f"{{{NAMESPACE}}}Sole"
SHARED_VOTING_TAG = f"{{{NAMESPACE}}}Shared"
NONE_VOTING_TAG = f"{{{NAMESPACE}}}None"

# Strip SEC Header and Start from <edgarSubmission>
def strip_header_and_stream(file_stream):
    found_start = False
    for line in file_stream:
        # Detect the opening <edgarSubmission> tag
        if not found_start and b"<informationTable" in line:
            found_start = True
            yield line
        if found_start:
            yield line  # Yield all lines after the root tag

# Extract 13F positions from the XML stream
def extract_13f_positions(file_stream, company_name, filing_date):
    positions = []
    try:
        context = etree.iterparse(file_stream, events=("start", "end"), recover=True)
        count = 0

        for event, elem in context:
    
            if event == "end" and elem.tag == INFORMATION_TABLE_TAG:
                # print("Found closing </informationTable>")
                break  # Stop after the closing tag

            # Process infotable tags within edgarSubmission
            if event == "end" and elem.tag == INFOTABLE_TAG:
                count += 1
                # Extract and validate CUSIP
                cusip = elem.findtext(CUSIP_TAG, default="N/A").strip()
                if cusip and any(cusip.startswith(prefix) for prefix in target_cusip_prefixes):
                    positions.append({
                        "doc_type": "13F-HR",
                        "company_name": company_name,
                        "filing_date": filing_date,
                        "nameofissuer": elem.findtext(NAMEOFISSUER_TAG, "N/A").strip(),
                        "titleofclass": elem.findtext(TITLEOFCLASS_TAG, "N/A").strip(),
                        "cusip": cusip.strip(),
                        "value": elem.findtext(VALUE_TAG, "N/A").strip(),
                        "sshPrnamt": elem.findtext(f"{SHRSORPRNAMT_TAG}/{SSHPRNAMT_TAG}", "N/A").strip(),
                        "sshprnamttype": elem.findtext(f"{SHRSORPRNAMT_TAG}/{SSHPRNAMTTYPE_TAG}", "N/A").strip(),
                        "investmentDiscretion": elem.findtext(INVESTMENTDISCRETION_TAG, "N/A").strip(),
                        "putCall": elem.findtext(PUTCALL_TAG, "N/A").strip(),
                        "soleVotingAuthority": elem.findtext(SOLE_VOTING_TAG, "N/A").strip(),
                        "sharedVotingAuthority": elem.findtext(SHARED_VOTING_TAG, "N/A").strip(),
                        "noneVotingAuthority": elem.findtext(NONE_VOTING_TAG, "N/A").strip(),
                    })
                elem.clear()  # Free memory for processed elements
        print(f"Total <infotable> tags processed: {count}")
        return positions
    except Exception as e:
        print(f"Error parsing positions: {e}")
        return []

# Process a single 13F-HR file
def process_13f_file(file_url, company_name, filing_date):
    try:
        response = requests.get(file_url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()

        # Handle gzip compression
        if response.headers.get("Content-Encoding") == "gzip":
            file_stream = gzip.GzipFile(fileobj=response.raw)
        else:
            file_stream = response.raw

        # Clean and parse the file stream
        cleaned_stream = StreamWrapper(strip_header_and_stream(file_stream))
        return extract_13f_positions(cleaned_stream, company_name, filing_date)
    except Exception as e:
        print(f"Error processing 13F-HR file {file_url}: {e}")
        return []

# Wrap the cleaned stream for compatibility with lxml
class StreamWrapper:
    def __init__(self, generator):
        self.generator = generator
        self.buffer = b""

    def read(self, size=-1):
        while len(self.buffer) < size or size == -1:
            try:
                self.buffer += next(self.generator)
            except StopIteration:
                break

        if size == -1:  # Read all
            result, self.buffer = self.buffer, b""
        else:
            result, self.buffer = self.buffer[:size], self.buffer[size:]
        return result

# Write extracted positions to CSV
def write_positions_to_csv(positions):
    if not positions:
        return
    write_header = not os.path.exists(output_csv)
    with open(output_csv, "a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields_13f)
        if write_header:
            writer.writeheader()
        writer.writerows(positions)

# Iterate over filings and process each file
df = pd.read_csv(input_csv)
for _, row in df.iterrows():
    if row["Form Type"] == "13F-HR":
        file_url = f"{base_url}{row['File Name']}"
        print(f"Processing file: {file_url}")
        positions = process_13f_file(file_url, row["Company Name"], row["Date Filed"])
        write_positions_to_csv(positions)
        time.sleep(.2)
