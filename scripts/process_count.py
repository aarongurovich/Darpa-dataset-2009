import pandas as pd

df = pd.read_csv("processed_data/csv/RAW.csv")

required_columns = ["Source", "Destination"]
missing_columns = [col for col in required_columns if col not in df.columns]

if missing_columns:
    print(f"Missing required columns: {missing_columns}")
    exit(1) 

df["NodeWeight"] = df.groupby(["Source", "Destination"])["Source"].transform("count")
df["EdgeWeight"] = df.groupby(["Source", "Destination"])["Source"].transform("count")


output_csv = "processed_data/csv/RAW.csv"
df.to_csv(output_csv, index=False)

print(f"\nWeight column added! Data saved in '{output_csv}'.")
