import pandas as pd
import requests
from bs4 import BeautifulSoup
import csv
import os
import time

headers = {
    "User-Agent": "MyApp/2.0 (myemail@example.com)"
}

# Define input CSV, output CSV, and target CUSIP prefixes
input_csv = "NPORT-P_filtered_form_index.csv"  # Input CSV with filing information
output_csv = "NPORT_filtered_positions.csv"  # Output CSV for filtered positions
base_url = "https://www.sec.gov/Archives/"  # Base URL for SEC filings
target_cusip_prefixes = {"225447", "977852"}  # CUSIP prefixes to match

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

# Extract positions from SGML-like NPORT-P content
def extract_nport_positions(content, company_name, filing_date):
    positions = []
    try:
        # Parse the content using BeautifulSoup
        soup = BeautifulSoup(content, "html.parser")

        # Locate <invstOrSec> tags
        invst_or_secs = soup.find_all("invstorsec")
        for sec in invst_or_secs:
            cusip = sec.find("cusip").text.strip() if sec.find("cusip") else None
            if cusip and any(cusip.startswith(prefix) for prefix in target_cusip_prefixes):
                position = {key: "N/A" for key in nport_headers}  # Initialize all fields with default values
                position.update({
                    "doc_type": "NPORT-P",
                    "company_name": company_name,
                    "filing_date": filing_date,
                    "name": sec.find("name").text.strip() if sec.find("name") else "N/A",
                    "lei": sec.find("lei").text.strip() if sec.find("lei") else "N/A",
                    "title": sec.find("title").text.strip() if sec.find("title") else "N/A",
                    "cusip": sec.find("cusip").text.strip() if sec.find("cusip") else "N/A",
                    "balance": sec.find("balance").text.strip() if sec.find("balance") else "N/A",
                    "units": sec.find("units").text.strip() if sec.find("units") else "N/A",
                    "curCd": sec.find("curcd").text.strip() if sec.find("curcd") else "N/A",
                    "valUSD": sec.find("valusd").text.strip() if sec.find("valusd") else "N/A",
                    "pctVal": sec.find("pctval").text.strip() if sec.find("pctval") else "N/A",
                    "payoffProfile": sec.find("payoffprofile").text.strip() if sec.find("payoffprofile") else "N/A",
                    "assetCat": sec.find("assetcat").text.strip() if sec.find("assetcat") else "N/A",
                    "fairValLevel": sec.find("fairvallevel").text.strip() if sec.find("fairvallevel") else "N/A",
                })

                # Handle nested <identifiers> tag
                identifiers = sec.find("identifiers")
                if identifiers:
                    other = identifiers.find("other")
                    if other:
                        position["identifiers_otherDesc"] = other.get("otherdesc", "N/A")
                        position["identifiers_value"] = other.get("value", "N/A")

                # Handle nested <issuerConditional> tag
                issuer_conditional = sec.find("issuerconditional")
                if issuer_conditional:
                    position["issuerConditional_desc"] = issuer_conditional.get("desc", "N/A")
                    position["issuerConditional_issuerCat"] = issuer_conditional.get("issuercat", "N/A")

                # Handle <derivativeInfo> and its child elements
                derivative_info = sec.find("derivativeinfo")
                if derivative_info:
                    deriv = derivative_info.find("optionswaptionwarrantderiv")
                    if deriv:
                        position["derivCat"] = deriv.get("derivcat", "N/A")
                        counterparties = deriv.find("counterparties")
                        if counterparties:
                            counterparty_name = counterparties.find("counterpartyname")
                            if counterparty_name:
                                position["counterpartyName"] = counterparty_name.text.strip()
                            counterparty_lei = counterparties.find("counterpartylei")
                            if counterparty_lei:
                                position["counterpartyLei"] = counterparty_lei.text.strip()
                        position["putOrCall"] = deriv.find("putorcall").text.strip() if deriv.find("putorcall") else "N/A"
                        position["writtenOrPur"] = deriv.find("writtenorpur").text.strip() if deriv.find("writtenorpur") else "N/A"
                        position["shareNo"] = deriv.find("shareno").text.strip() if deriv.find("shareno") else "N/A"
                        position["exercisePrice"] = deriv.find("exerciseprice").text.strip() if deriv.find("exerciseprice") else "N/A"
                        position["exercisePriceCurCd"] = deriv.find("exercisepricecurcd").text.strip() if deriv.find("exercisepricecurcd") else "N/A"
                        position["expDt"] = deriv.find("expdt").text.strip() if deriv.find("expdt") else "N/A"
                        position["delta"] = deriv.find("delta").text.strip() if deriv.find("delta") else "N/A"
                        position["unrealizedAppr"] = deriv.find("unrealizedappr").text.strip() if deriv.find("unrealizedappr") else "N/A"

                # Handle <securityLending> and its child elements
                security_lending = sec.find("securitylending")
                if security_lending:
                    position["isCashCollateral"] = security_lending.find("iscashcollateral").text.strip() if security_lending.find("iscashcollateral") else "N/A"
                    position["isNonCashCollateral"] = security_lending.find("isnoncashcollateral").text.strip() if security_lending.find("isnoncashcollateral") else "N/A"
                    position["isLoanByFund"] = security_lending.find("isloanbyfund").text.strip() if security_lending.find("isloanbyfund") else "N/A"
                print(position)
                print("~~~~~~~")
                positions.append(position)

    except Exception as e:
        print(f"Error parsing NPORT-P content: {e}")
    return positions

# Process a single file
def process_file(file_url, company_name, filing_date, form_type):
    try:
        response = requests.get(file_url, headers=headers)
        time.sleep(.2)
        response.raise_for_status()
        content = response.content.decode("utf-8", errors="replace")  # Decode content as text
        if form_type == "NPORT-P":
            return extract_nport_positions(content, company_name, filing_date)
        return []
    except Exception as e:
        print(f"Error processing file {file_url}: {e}")
        return []

# Write positions incrementally to the output file
def write_positions_to_csv(positions):
    if not positions:
        return
    write_header = not os.path.exists(output_csv)  # Write header only if file doesn't exist
    with open(output_csv, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=nport_headers)
        if write_header:
            writer.writeheader()
        writer.writerows(positions)

# Read the input CSV
df = pd.read_csv(input_csv)

# Iterate over rows in the CSV
for _, row in df.iterrows():
    file_url = f"{base_url}{row['File Name']}"
    print(f"Processing file: {file_url}")
    positions = process_file(file_url, row["Company Name"], row["Date Filed"], row["Form Type"])
    write_positions_to_csv(positions)
