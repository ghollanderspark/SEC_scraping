import pandas as pd

# File path (replace with your file path)
file_path = "NPORT_filtered_positions_Q3Q4.csv"

# Load the data into a pandas DataFrame
data = pd.read_csv(file_path)

# Ensure numeric columns are correctly parsed
data['balance'] = pd.to_numeric(data['balance'], errors='coerce')

# Filter for common stock (EC)
ec_positions = data[data['assetCat'].str.contains('EC', case=False, na=False)]

# Total count of EC shares
total_ec_shares = ec_positions['balance'].sum()

# Count of EC shares not loaned (isLoanByFund == 'N')
ec_non_loaned = ec_positions[ec_positions['isLoanByFund'].str.upper() == 'N']
non_loaned_ec_shares = ec_non_loaned['balance'].sum()

# Display the results
print(f"Total EC (common stock) shares: {total_ec_shares}")
print(f"EC (common stock) shares not loaned: {non_loaned_ec_shares}")
