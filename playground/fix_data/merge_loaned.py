import pandas as pd

# File paths
file_q3q4 = "NPORT_filtered_positions_Q3Q4_new.csv"  # First file
file_loaned = "NPORT_filtered_positions_loaned.csv"  # Second file
output_file = "NPORT_filtered_positions_updated.csv"

# Load the datasets
q3q4_data = pd.read_csv(file_q3q4)
loaned_data = pd.read_csv(file_loaned)

# Add missing columns to the first dataset with default values (0)
q3q4_data['isLoanByFund'] = 'N'  # Default is 'N'
q3q4_data['estSharesLoaned'] = 0  # Default is 0

# Define the key to merge on: company_name and cusip (or adjust if necessary)
merge_keys = ['company_name', 'cusip','balance','valUSD']

# Merge the two datasets, prioritizing values from the loaned file
updated_data = pd.merge(
    q3q4_data,
    loaned_data[['company_name', 'cusip','balance','valUSD', 'isLoanByFund', 'estSharesLoaned']],
    on=merge_keys,
    how='left',
    suffixes=('', '_loaned')
)

# Replace the columns with values from the second dataset where available
updated_data['isLoanByFund'] = updated_data['isLoanByFund_loaned'].fillna(updated_data['isLoanByFund'])
updated_data['estSharesLoaned'] = updated_data['estSharesLoaned_loaned'].fillna(updated_data['estSharesLoaned'])

# Drop temporary columns
updated_data = updated_data.drop(columns=['isLoanByFund_loaned', 'estSharesLoaned_loaned'])

# Save the updated dataset to a new file
updated_data.to_csv(output_file, index=False)

print(f"Updated file saved to: {output_file}")
