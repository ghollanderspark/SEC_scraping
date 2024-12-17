import pandas as pd
import plotly.graph_objects as go

# Load data (replace 'your_file.json' with the actual file path)
df = pd.read_json('unique_assets.json')

# Filter positions with 'EC_Long' in unique_identifier
ec_long_data = df[df['unique_identifier'].str.contains('EC_Long')].copy()

# Convert 'filing_date' to datetime and sort data
ec_long_data['filing_date'] = pd.to_datetime(ec_long_data['filing_date'])
ec_long_data = ec_long_data.sort_values(['unique_identifier', 'filing_date'])

# Step 1: Aggregate positions with the same unique_identifier and filing_date
aggregated = ec_long_data.groupby(['unique_identifier', 'filing_date']).agg({
    'balance': 'sum',
    'estSharesLoaned': 'sum'
}).reset_index()

# Step 2: Calculate 'unloaned_shares'
aggregated['unloaned_shares'] = aggregated['balance'] - aggregated['estSharesLoaned']

# Step 3: Create a global date range
global_date_range = pd.date_range(start=aggregated['filing_date'].min(), 
                                  end=aggregated['filing_date'].max(), freq='D')

# Step 4: Forward fill for each unique_identifier until the next filing date
filled_data = []

for uid, group in aggregated.groupby('unique_identifier'):
    # Reindex the group to the global date range
    group = group.set_index('filing_date').reindex(global_date_range)
    
    # Forward fill logic: Carry values only up to the next available filing date
    last_value = None
    for i, row in group.iterrows():
        if pd.notnull(row['balance']):  # A new filing resets the carry
            last_value = row['balance'], row['estSharesLoaned']
        elif last_value is not None:  # Carry over the previous value
            group.at[i, 'balance'], group.at[i, 'estSharesLoaned'] = last_value

    # Recalculate 'unloaned_shares'
    group['unloaned_shares'] = group['balance'] - group['estSharesLoaned']
    group['unique_identifier'] = uid  # Add back the UID column
    filled_data.append(group.reset_index().rename(columns={'index': 'filing_date'}))

# Step 5: Combine all forward-filled data
filled_data = pd.concat(filled_data)

# Step 6: Aggregate across all unique_identifiers for each date
final_aggregated = filled_data.groupby('filing_date').agg({
    'unloaned_shares': 'sum',
    'estSharesLoaned': 'sum'
}).reset_index()

# Step 7: Generate the interactive stacked area chart using Plotly
fig = go.Figure()

# Add 'Loaned Shares' trace
fig.add_trace(go.Scatter(
    x=final_aggregated['filing_date'],
    y=final_aggregated['estSharesLoaned'],
    mode='lines',
    fill='tonexty',
    name='Loaned Shares',
    hovertemplate='Loaned Shares: %{y}<extra></extra>'
))

# Add 'Unloaned Shares' trace
fig.add_trace(go.Scatter(
    x=final_aggregated['filing_date'],
    y=final_aggregated['unloaned_shares'],
    mode='lines',
    fill='tonexty',
    name='Unloaned Shares',
    hovertemplate='Unloaned Shares: %{y}<extra></extra>'
))


# Update layout
fig.update_layout(
    title="Interactive Loaned vs. Unloaned Shares Over Time",
    xaxis_title="Filing Date",
    yaxis_title="Shares",
    legend_title="Legend",
    template="plotly_white",
    hovermode="x unified"
)

fig.show()
