import pandas as pd

# Load the dataset
file_path = 'NPORT_filtered_positions_Q3Q4.csv'  # Update with your file path
data = pd.read_csv(file_path)

# Fill NaN values for necessary columns with defaults
data['payoffProfile'] = data['payoffProfile'].fillna('')
data['putOrCall'] = data['putOrCall'].fillna('')
data['isLoanByFund'] = data['isLoanByFund'].fillna('Y')  # Default to 'N' if missing
data['balance'] = data['balance'].fillna(0)
data['estSharesLoaned'] = data['estSharesLoaned'].fillna(0)

# Convert filing_date to datetime for consistency
data['filing_date'] = pd.to_datetime(data['filing_date'])

# Define a function to create the adjusted unique identifier (excluding date and loan status)
def create_unique_identifier(row):
    if row['assetCat'] == 'EC':  # Special case for Equity
        if row['putOrCall']:  # Include putOrCall only if not empty
            return f"{row['company_name']}_{row['assetCat']}_{row['payoffProfile']}_{row['putOrCall']}"
        else:
            return f"{row['company_name']}_{row['assetCat']}_{row['payoffProfile']}"
    else:
        return f"{row['company_name']}_{row['assetCat']}_{row['payoffProfile']}"

# Apply the function to create the adjusted unique identifiers
data['adjusted_identifier'] = data.apply(create_unique_identifier, axis=1)

# Create the list of objects for visualization
unique_assets = []
for _, row in data.iterrows():
    asset_object = {
        'unique_identifier': row['adjusted_identifier'],
        'filing_date': row['filing_date'].strftime('%Y-%m-%d'),
        'isLoanByFund': row['isLoanByFund'],
        'balance': row['balance'],
        'estSharesLoaned': row['estSharesLoaned']

    }
    unique_assets.append(asset_object)

# Print the first 10 unique asset objects as a sample
print("Sample Unique Asset Objects:")
for asset in unique_assets[:10]:
    print(asset)

# Optionally save the results to a JSON file
import json
output_file = 'unique_assets.json'
with open(output_file, 'w') as f:
    json.dump(unique_assets, f, indent=4)
print(f"Unique assets saved to {output_file}.")
