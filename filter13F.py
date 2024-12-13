import pandas as pd

# Define file paths
input_csv = "form_to_name_index.csv"  # Replace with the path to your input CSV file
output_csv = "filtered_form_index.csv"  # Replace with the desired output file name

# Load the CSV file
df = pd.read_csv(input_csv)

# Filter rows where 'Form Type' is '13F-HR' or 'NPORT-P'
filtered_df = df[df["Form Type"].isin(["13F-HR", "NPORT-P"])]

# Save the filtered DataFrame to a new CSV file
filtered_df.to_csv(output_csv, index=False)

print(f"Filtered data saved to {output_csv}")

# AFTER THIS SCRIPT YOU CAN MANUALLY SEPARATE THE FILES INTO TWO
# FILES, ONE FOR 13F ONE FOR NPORT, SORRY I DIDNT MAKE IT AUTO
# SPLIT THEM.
