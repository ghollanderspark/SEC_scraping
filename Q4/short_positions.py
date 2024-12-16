import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import ScalarFormatter

# File path (replace with actual path if running locally)
file_path = "NPORT_filtered_positions.csv"

# Load the data into a pandas DataFrame
data = pd.read_csv(file_path)

# Display the first few rows to confirm the data is loaded correctly
print(data.head())

# Ensure numeric columns are correctly parsed
data['balance'] = pd.to_numeric(data['balance'], errors='coerce')
data['units'] = pd.to_numeric(data['units'], errors='coerce')

# Filter for short positions
short_positions = data[data['payoffProfile'].str.contains('Short', case=False, na=False)]

# Replace missing or invalid entries in 'assetCat' with 'Unknown'
short_positions['assetCat'] = short_positions['assetCat'].fillna('Unknown')

# Group data by asset type and company name, summing up balance or units
metric = 'units'  # Change this to 'units' if you want to plot units
grouped = short_positions.groupby(['assetCat', 'company_name'])[metric].sum().unstack(fill_value=0)

# Plot stacked bar chart
plt.figure(figsize=(12, 8))
grouped.plot(kind='bar', stacked=True, figsize=(12, 8), colormap='tab20')

# Chart formatting
plt.title(f"Short Positions by Asset Type and Company Name ({metric.title()})", fontsize=16)
plt.xlabel('Asset Type', fontsize=14)
plt.ylabel(metric.title(), fontsize=14)
plt.xticks(rotation=45)
plt.legend(title='Company Name', bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=10)
# Disable scientific notation on the y-axis
plt.gca().yaxis.set_major_formatter(ScalarFormatter(useOffset=False))
plt.gca().ticklabel_format(style='plain', axis='y')
plt.tight_layout()

# Show the plot
plt.show()
