import pandas as pd

input_csv = "processed_data/csv/RAW.csv"  
output_csv = "processed_data/csv/RAW.csv"

df = pd.read_csv(input_csv)

required_columns = ["Source", "Destination", "Protocol"]
if not all(col in df.columns for col in required_columns):
    print("Missing necessary columns: Source, Destination, Protocol")
    exit()

df_deduplicated = df.drop_duplicates(subset=["Source", "Destination", "Protocol"])

# Save the cleaned dataset
df_deduplicated.to_csv(output_csv, index=False)

print(f"Deduplicated CSV saved as '{output_csv}'.")
