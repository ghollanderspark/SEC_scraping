import pandas as pd

# Load the dataset
file_path = "13F_filtered_positions_Q4.csv"  # Replace with your file path
data = pd.read_csv(file_path)

# Ensure 'sshPrnamt' is numeric
data['sshPrnamt'] = pd.to_numeric(data['sshPrnamt'], errors='coerce')

# Filter the dataset
filtered_shares = data[
    (data['sshprnamttype'] == 'SH') & 
    (data['investmentDiscretion'] == 'SOLE') & 
    (data['putCall'].isna())
]

# Filter the dataset
filtered_shares2 = data[
    (data['sshprnamttype'] == 'SH') & 
    (data['investmentDiscretion'] != 'SOLE') & 
    (data['putCall'].isna())
]

# Calculate the total number of shares
total_shares_with_sole_voting = filtered_shares['sshPrnamt'].sum()
total_shares_without_sole_voting = filtered_shares2['sshPrnamt'].sum()
# Output the result
print(f"Total shares with sole voting authority: {total_shares_with_sole_voting}")
print(f"Total shares without sole voting authority: {total_shares_without_sole_voting}")