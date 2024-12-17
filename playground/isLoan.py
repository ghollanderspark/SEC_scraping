import pandas as pd

# File paths
source_csv = "NPORT_filtered_form_index_Q3Q4.csv"  # The CSV file to search for matching company names
reference_csv = "NPORT_filtered_positions_Q3Q4.csv"  # The CSV file containing rows with isLoanByFund == N/A
output_csv = "matched_loaning_companies.csv"  # Output file

# Load the reference CSV and filter where isLoanByFund == 'N/A'
reference_data = pd.read_csv(reference_csv, na_values=["N/A", "n/a", "NA", "", " "])
filtered_companies = reference_data[reference_data['isLoanByFund'].isna()]['company_name'].unique()
print(filtered_companies)


# Load the source CSV
source_data = pd.read_csv(source_csv)

# Filter rows in source CSV where company_name matches the filtered list
matched_rows = source_data[source_data['Company Name'].isin(filtered_companies)]

# Save the matched rows to a new CSV file
matched_rows.to_csv(output_csv, index=False)

print(f"Matching rows saved to {output_csv}.")
