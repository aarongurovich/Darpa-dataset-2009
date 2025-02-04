import pandas as pd
from ipaddress import ip_address, ip_network

internal_subnets = [
    ip_network('172.28.0.0/16'),
    ip_network('192.168.61.0/24')
]

def classify_ip(ip):
    try:
        ip_obj = ip_address(ip)
        for subnet in internal_subnets:
            if ip_obj in subnet:
                return 'Internal'
        return 'External'
    except ValueError:
        return 'Invalid IP' 


input_file = 'processed_data/csv/RAW.csv'
output_file = 'processed_data/csv/RAW.csv'

df = pd.read_csv(input_file)

df['Classification'] = df['Source'].apply(classify_ip)

df.to_csv(output_file, index=False)

print(f"Classification completed. Output saved to {output_file}.")
