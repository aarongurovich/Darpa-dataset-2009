import pandas as pd

df = pd.read_csv("processed_data/csv/tcp_normal_behavior.csv")

required_columns = ["Source", "Destination"]
missing_columns = [col for col in required_columns if col not in df.columns]

if missing_columns:
    print(f"Missing required columns: {missing_columns}")
    exit(1) 

df["Weight"] = df.groupby(["Source", "Destination"])["Source"].transform("count")

output_csv = "processed_data/csv/tcp_normal_behavior.csv"
df.to_csv(output_csv, index=False)

print(f"\nWeight column added! Data saved in '{output_csv}'.")
