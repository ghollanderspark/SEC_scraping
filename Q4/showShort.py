import pandas as pd

# File path (replace with your file path)
file_path = "NPORT_filtered_positions_Q4.csv"

# Load the data into a pandas DataFrame
data = pd.read_csv(file_path)

# Ensure numeric columns are correctly parsed
data['balance'] = pd.to_numeric(data['balance'], errors='coerce')
data['units'] = pd.to_numeric(data['units'], errors='coerce')

# Filter for short positions
short_positions = data[data['payoffProfile'].str.contains('Short', case=False, na=False)]

# Replace missing or invalid entries in 'assetCat' with 'Unknown'
short_positions['assetCat'] = short_positions['assetCat'].fillna('Unknown')

# Display short positions in the terminal
if not short_positions.empty:
    print("Short Positions Found:\n")
    print(short_positions[['company_name', 'balance', 'valUSD', 'assetCat']])

       # Calculate and display the total balance
    total_balance = short_positions['balance'].sum()
    print(f"\nTotal Balance of Short Positions: {total_balance}")
else:
    print("No short positions found in the dataset.")
