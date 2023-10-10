import csv
import random
import json


def delay_timestamp(ts, min_d, max_d):
    delay = random.randint(int(min_d), int(max_d))
    return ts - delay


with open('config.json', 'r') as config_file:
    config = json.load(config_file)

csv_filename = config.get('csv_filename')
min_delay = config.get('min_delay')
max_delay = config.get('max_delay')
probability = config.get('probability')
start_line = config.get('start_line')
output_dir = config.get('output_dir')

if probability < 0 or probability > 1:
    print("Probability needs to be between 0 and 1.")
    exit()

with open(csv_filename, 'r') as csv_file, open(output_dir, 'w', newline='') as output_file:
    reader = csv.reader(csv_file)
    writer = csv.writer(output_file)

    header = next(reader)
    writer.writerow(header)

    for i, row in enumerate(reader, start=1):
        if i >= start_line and random.uniform(0, 1) < probability:
            row[0] = str(delay_timestamp(int(row[0]), min_delay, max_delay))
        writer.writerow(row)

print(f"Events file with Out of Order timestamps exported in {output_dir}")
