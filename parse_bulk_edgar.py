import re
import csv

# Define input and output file paths
input_file = "./form.txt"  # Replace with the actual path to your .txt file
output_file = "form_to_name_index.csv"  # Replace with the desired output .csv file name

# Initialize variables to store header information and data rows
header_info = {}
data_rows = []

# Open and read the input file
with open(input_file, 'r') as file:
    lines = file.readlines()

# Process the header and data
data_section_start = False
for line in lines:
    # Check if we are in the data section
    if line.startswith("Form Type"):
        data_section_start = True
        continue
    elif line.startswith("-"):
        continue

    # Process header information
    if not data_section_start:
        if ":" in line:
            key, value = map(str.strip, line.split(":", 1))
            header_info[key] = value
    # Process data rows
    else:
        # Use regex to split on two or more spaces
        parts = re.split(r'\s{2,}', line.strip())
        if len(parts) == 5:  # Ensure the line has the correct number of elements
            data_rows.append(parts)

# Write the data to a CSV file
csv_columns = ["Form Type", "Company Name", "CIK", "Date Filed", "File Name"]
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(csv_columns)  # Write headers
    csvwriter.writerows(data_rows)   # Write data rows

print(f"CSV file saved to {output_file}")
