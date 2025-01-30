import pandas as pd

df = pd.read_csv("processed_data/csv/one_structures.csv")

required_columns = ["No.", "Source", "Destination", "Protocol", "TCP Flags", "Acknowledgment Number"]
missing_columns = [col for col in required_columns if col not in df.columns]

if missing_columns:
    print(f"Missing required columns: {missing_columns}")
    exit(1) 

df_normal = df[~((df["TCP Flags"] == "SYN") & (df["Acknowledgment Number"] == 0))]

ddos_threshold = 50 
target_counts = df_normal["Destination"].value_counts()
normal_targets = target_counts[target_counts <= ddos_threshold].index.tolist()
df_normal = df_normal[df_normal["Destination"].isin(normal_targets)]

scan_threshold = 10 
scan_counts = df_normal["Source"].value_counts()
normal_sources = scan_counts[scan_counts <= scan_threshold].index.tolist()
df_normal = df_normal[df_normal["Source"].isin(normal_sources)]

df_normal = df_normal[df_normal["Protocol"].isin(["TCP", "HTTP", "DNS"])]

df_normal = df_normal.sort_values(by=["No."]).head(200)

output_csv = "processed_data/csv/tcp_normal_behavior.csv"
df_normal.to_csv(output_csv, index=False)

print(f"\n Normal behavior extracted! {len(df_normal)} packets saved in '{output_csv}'.")
