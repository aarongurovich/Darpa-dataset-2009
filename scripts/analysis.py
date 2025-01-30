import pandas as pd

df = pd.read_csv("processed_data/csv/one_structures.csv")

required_columns = ["No.", "Time", "Source", "Destination", "Source Port", "Destination Port", "TCP Flags", "Acknowledgment Number"]
missing_columns = [col for col in required_columns if col not in df.columns]

if missing_columns:
    print(f"Missing required columns: {missing_columns}")
    exit(1)  


ddos_threshold = 50  
ddos_counts = df["Destination"].value_counts()
ddos_targets = ddos_counts[ddos_counts > ddos_threshold].index.tolist()
df_ddos = df[df["Destination"].isin(ddos_targets)]


df_ddos = df_ddos.sort_values(by=["No."]).head(200)

if not df_ddos.empty:
    df_ddos.to_csv("processed_data/csv/tcp_ddos_anomalies.csv", index=False)
    print(f"\nDDoS anomaly detection completed! {len(df_ddos)} packets saved in 'processed_data/csv/tcp_ddos_anomalies.csv'.")
else:
    print("\nNo DDoS anomalies detected.")
