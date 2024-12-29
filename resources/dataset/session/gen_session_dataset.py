import json
import csv
import random

# Leggi il file di configurazione
with open('config.json', 'r') as f:
    config = json.load(f)

output_file = config['output_file']
num_rows = config['num_rows']
max_timestamp_increment = config['max_timestamp_increment']

# Genera i dati
data = []
current_ts = 1000  # timestamp iniziale
key = 0
value = 20.0

for _ in range(num_rows):
    data.append([current_ts, key, value])
    current_ts += random.randint(1, max_timestamp_increment)

# Scrivi i dati in un file CSV
with open(output_file, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['ts', 'key', 'value'])
    writer.writerows(data)

print(f"File CSV '{output_file}' generato con successo!")
