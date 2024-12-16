import pandas as pd
import requests
from lxml import etree
import csv
import os
import time
import gc
import time
import gzip
from io import BytesIO


headers = {
    "User-Agent": "MyApp/2.0 (myemail@example.com)"
}

# File for logging skipped files
skipped_files_csv = "skipped_files.csv"

# Log skipped files to CSV
def log_skipped_file(file_url, reason, size_mb=None):
    write_header = not os.path.exists(skipped_files_csv)  # Check if file exists
    with open(skipped_files_csv, mode="a", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=["File URL", "Reason", "Size (MB)"])
        if write_header:
            writer.writeheader()  # Write the header if the file is new
        writer.writerow({"File URL": file_url, "Reason": reason, "Size (MB)": size_mb})

# Define input/output paths and CUSIP prefixes
input_csv = "NPORT_filtered_form_index.csv"  # Input CSV
output_csv = "NPORT_filtered_positions.csv"  # Output CSV
base_url = "https://www.sec.gov/Archives/"  # Base URL
target_cusip_prefixes = {"225447", "977852"}  # Target prefixes

NAMESPACE = "http://www.sec.gov/edgar/nport"
EDGAR_SUBMISSION_TAG = f"{{{NAMESPACE}}}edgarSubmission"  # Formats the namespace-bound tag
INVSTORSEC_TAG = f"{{{NAMESPACE}}}invstOrSec"


# Static headers for NPORT-P filings
nport_headers = [
    "doc_type", "company_name", "filing_date", "name", "lei", "title", "cusip",
    "identifiers_otherDesc", "identifiers_value", "balance", "units", "curCd",
    "valUSD", "pctVal", "payoffProfile", "assetCat", "issuerConditional_desc",
    "issuerConditional_issuerCat", "invCountry", "isRestrictedSec", "fairValLevel",
    "derivCat", "counterpartyName", "counterpartyLei", "putOrCall", "writtenOrPur",
    "descRefInstrmnt_issuerName", "descRefInstrmnt_issueTitle",
    "descRefInstrmnt_otherDesc", "descRefInstrmnt_otherValue", "shareNo",
    "exercisePrice", "exercisePriceCurCd", "expDt", "delta", "unrealizedAppr",
    "isCashCollateral", "isNonCashCollateral", "isLoanByFund"
]

# Incremental parsing using lxml
def extract_nport_positions(file_stream, company_name, filing_date):
    # Define tags with namespaces
    CUSIP_TAG = f"{{{NAMESPACE}}}cusip"
    NAME_TAG = f"{{{NAMESPACE}}}name"
    LEI_TAG = f"{{{NAMESPACE}}}lei"
    TITLE_TAG = f"{{{NAMESPACE}}}title"
    BALANCE_TAG = f"{{{NAMESPACE}}}balance"
    UNITS_TAG = f"{{{NAMESPACE}}}units"
    CURCD_TAG = f"{{{NAMESPACE}}}curCd"
    VALUSD_TAG = f"{{{NAMESPACE}}}valUSD"
    PCTVAL_TAG = f"{{{NAMESPACE}}}pctVal"
    PAYOFF_PROFILE_TAG = f"{{{NAMESPACE}}}payoffProfile"
    ASSET_CAT_TAG = f"{{{NAMESPACE}}}assetCat"
    FAIRVAL_LEVEL_TAG = f"{{{NAMESPACE}}}fairValLevel"
    IDENTIFIERS_TAG = f"{{{NAMESPACE}}}identifiers"
    ISSUER_CONDITIONAL_TAG = f"{{{NAMESPACE}}}issuerConditional"
    DERIVATIVE_INFO_TAG = f"{{{NAMESPACE}}}derivativeInfo"
    SECURITY_LENDING_TAG = f"{{{NAMESPACE}}}securityLending"

    positions = []
    try:
        root_closed = False
        context = etree.iterparse(file_stream, events=("start", "end"))
        count = 0
        for event, elem in context:
            # if root_closed:
            #     break
            if event == "end" and elem.tag == INVSTORSEC_TAG:
                count += 1
                cusip = elem.findtext(CUSIP_TAG)
                if cusip and any(cusip.startswith(prefix) for prefix in target_cusip_prefixes):
                    position = {key: "N/A" for key in nport_headers}  # Default values
                    position.update({
                        "doc_type": "NPORT-P",
                        "company_name": company_name,
                        "filing_date": filing_date,
                        "name": elem.findtext(NAME_TAG, "N/A"),
                        "lei": elem.findtext(LEI_TAG, "N/A"),
                        "title": elem.findtext(TITLE_TAG, "N/A"),
                        "cusip": cusip.strip(),
                        "balance": elem.findtext(BALANCE_TAG, "N/A"),
                        "units": elem.findtext(UNITS_TAG, "N/A"),
                        "curCd": elem.findtext(CURCD_TAG, "N/A"),
                        "valUSD": elem.findtext(VALUSD_TAG, "N/A"),
                        "pctVal": elem.findtext(PCTVAL_TAG, "N/A"),
                        "payoffProfile": elem.findtext(PAYOFF_PROFILE_TAG, "N/A"),
                        "assetCat": elem.findtext(ASSET_CAT_TAG, "N/A"),
                        "fairValLevel": elem.findtext(FAIRVAL_LEVEL_TAG, "N/A"),
                    })

                    # Handle <identifiers>
                    identifiers = elem.find(IDENTIFIERS_TAG)
                    if identifiers:
                        other = identifiers.find(f"{{{NAMESPACE}}}other")
                        if other is not None:
                            position["identifiers_otherDesc"] = other.get("otherdesc", "N/A")
                            position["identifiers_value"] = other.get("value", "N/A")

                    # Handle <issuerConditional>
                    issuer_conditional = elem.find(ISSUER_CONDITIONAL_TAG)
                    if issuer_conditional is not None:
                        position["issuerConditional_desc"] = issuer_conditional.get("desc", "N/A")
                        position["issuerConditional_issuerCat"] = issuer_conditional.get("issuercat", "N/A")

                    # Handle <derivativeInfo>
                    derivative_info = elem.find(DERIVATIVE_INFO_TAG)
                    if derivative_info:
                        deriv = derivative_info.find(f"{{{NAMESPACE}}}optionSwaptionWarrantDeriv")
                        if deriv:
                            position["derivCat"] = deriv.get("derivcat", "N/A")
                            counterparties = deriv.find(f"{{{NAMESPACE}}}counterparties")
                            if counterparties:
                                position["counterpartyName"] = counterparties.findtext(
                                    f"{{{NAMESPACE}}}counterpartyName", "N/A").strip()
                                position["counterpartyLei"] = counterparties.findtext(
                                    f"{{{NAMESPACE}}}counterpartyLei", "N/A").strip()
                            position["putOrCall"] = deriv.findtext(f"{{{NAMESPACE}}}putOrCall", "N/A").strip()
                            position["writtenOrPur"] = deriv.findtext(f"{{{NAMESPACE}}}writtenOrPur", "N/A").strip()
                            position["shareNo"] = deriv.findtext(f"{{{NAMESPACE}}}shareno", "N/A").strip()
                            position["exercisePrice"] = deriv.findtext(f"{{{NAMESPACE}}}exerciseprice", "N/A").strip()
                            position["exercisePriceCurCd"] = deriv.findtext(f"{{{NAMESPACE}}}exercisepricecurcd", "N/A").strip()
                            position["expDt"] = deriv.findtext(f"{{{NAMESPACE}}}expdt", "N/A").strip()
                            position["delta"] = deriv.findtext(f"{{{NAMESPACE}}}delta", "N/A").strip()
                            position["unrealizedAppr"] = deriv.findtext(f"{{{NAMESPACE}}}unrealizedappr", "N/A").strip()

                    # Handle <securityLending>
                    security_lending = elem.find(SECURITY_LENDING_TAG)
                    if security_lending:
                        position["isCashCollateral"] = security_lending.findtext(
                            f"{{{NAMESPACE}}}isCashCollateral", "N/A").strip()
                        position["isNonCashCollateral"] = security_lending.findtext(
                            f"{{{NAMESPACE}}}isNonCashCollateral", "N/A").strip()
                        position["isLoanByFund"] = security_lending.findtext(
                            f"{{{NAMESPACE}}}isLoanByFund", "N/A").strip()

                    positions.append(position)

                elem.clear()  
                # Clear memory for processed elements
            elif event == "end" and elem.tag == EDGAR_SUBMISSION_TAG:
                # print(f"Root element <edgarSubmission> closed at position.")
                # root_closed = True
                break
        print(f"Total <invstorsec> tags processed: {count}")
        return positions
    except Exception as e:
        print(f"Error parsing positions: {e}")
        return []

# Process a single file

def strip_header_and_stream(file_stream):
    xml_started = False
    for line in file_stream:
        # Skip lines until the start of the XML declaration
        if not xml_started:
            if b"<edgarSubmission" in line:
                xml_started = True
                # print(line)
                yield line  # Include the XML declaration line
        else:
            # print(line)
            yield line  # Pass through subsequent lines


class StreamWrapper:
    def __init__(self, generator):
        self.generator = generator
        self.buffer = b""

    def read(self, size=-1):
        while len(self.buffer) < size or size == -1:
            try:
                # Fetch the next line from the generator
                self.buffer += next(self.generator)
            except StopIteration:
                break

        # Return the requested size of bytes
        if size == -1:  # Read all
            result, self.buffer = self.buffer, b""
        else:
            result, self.buffer = self.buffer[:size], self.buffer[size:]
        return result


def process_file(file_url, company_name, filing_date, form_type):
    try:
        response = requests.head(file_url, headers=headers)
        response.raise_for_status()
        # print(f"HTTP Status: {response.status_code}")
        # print(f"Headers: {response.headers}")

        # Check file size
        content_length = response.headers.get("Content-Length")
        max_size_mb = 50
        if content_length and int(content_length) > max_size_mb * 1024 * 1024:
            print(f"File too large ({int(content_length) / (1024 * 1024):.2f} MB). Skipping.")
            log_skipped_file(file_url, "File too large", size_mb=int(content_length) / (1024 * 1024))
            return []

        # Stream file for parsing
        response = requests.get(file_url, headers=headers, stream=True, timeout=30)
        response.raise_for_status()

        # Handle gzip compression
        if response.headers.get("Content-Encoding") == "gzip":
            # print("Decompressing gzipped content.")
            file_stream = gzip.GzipFile(fileobj=response.raw)
        else:
            file_stream = response.raw

        cleaned_stream = StreamWrapper(strip_header_and_stream(file_stream))
        
        if form_type == "NPORT-P":
            return extract_nport_positions(cleaned_stream, company_name, filing_date)
        return []
    except Exception as e:
        print(f"Error processing file {file_url}: {e}")
        return []

# Write positions incrementally to the output file
def write_positions_to_csv(positions):
    if not positions:
        return
    write_header = not os.path.exists(output_csv)
    with open(output_csv, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=nport_headers)
        if write_header:
            writer.writeheader()
        writer.writerows(positions)

# Read input CSV
df = pd.read_csv(input_csv)

# Iterate over rows in the CSV
for _, row in df.iterrows():
    file_url = f"{base_url}{row['File Name']}"
    print(f"Processing file: {file_url}")
    positions = process_file(file_url, row["Company Name"], row["Date Filed"], row["Form Type"])
    time.sleep(.2)
    write_positions_to_csv(positions)
    del positions
    gc.collect()
