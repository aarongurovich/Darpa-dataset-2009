import pandas as pd
import re


csv_path = "C:/Users/Aaron/New Darpa 2009/processed_data/csv/RAW.csv"
df = pd.read_csv(csv_path)

print("Available columns in CSV:", df.columns)

info_column = None
for col in df.columns:
    if "info" in col.lower():
        info_column = col
        break

if info_column is None:
    print("Warning: No 'Info' column found. Skipping parsing step.")
else:
    print(f"Found 'Info' column: {info_column}")

    def parse_info(info):
        match = re.search(r'(\d+) *>\s*(\d+) \[([A-Z,]+)\]', info)
        seq_match = re.search(r'Seq=(\d+)', info)
        ack_match = re.search(r'Ack=(\d+)', info)
        win_match = re.search(r'Win=(\d+)', info)
        len_match = re.search(r'Len=(\d+)', info)
        tsval_match = re.search(r'TSval=(\d+)', info)
        tsecr_match = re.search(r'TSecr=(\d+)', info)

        return {
            "Source Port": match.group(1) if match else None,
            "Destination Port": match.group(2) if match else None,
            "TCP Flags": match.group(3) if match else None,
            "Sequence Number": seq_match.group(1) if seq_match else None,
            "Acknowledgment Number": ack_match.group(1) if ack_match else None,
            "Window Size": win_match.group(1) if win_match else None,
            "Payload Length": len_match.group(1) if len_match else None,
            "TSval": tsval_match.group(1) if tsval_match else None,
            "TSecr": tsecr_match.group(1) if tsecr_match else None,
        }

    parsed_data = df[info_column].apply(lambda x: parse_info(str(x)))

    parsed_df = pd.DataFrame(parsed_data.tolist())
    df = pd.concat([df, parsed_df], axis=1)

    df.drop(columns=[info_column], inplace=True)

output_csv = "C:/Users/Aaron/New Darpa 2009/processed_data/csv/RAW.csv"
df.to_csv(output_csv, index=False)

print(f"Data successfully structured and saved as '{output_csv}'!")
