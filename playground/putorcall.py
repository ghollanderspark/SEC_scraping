import pandas as pd

# Load the dataset
file_path = 'NPORT_filtered_positions_Q3Q4.csv'  # Update with your file path
data = pd.read_csv(file_path)

# Extract unique values from the 'putOrCall' column
unique_put_or_call_values = data['isLoanByFund'].dropna().unique()

# Display the unique values
print("Unique values under 'putOrCall':")
for value in unique_put_or_call_values:
    print(value)
