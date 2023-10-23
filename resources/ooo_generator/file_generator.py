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

# Initialize a list to store all timestamps (original and delayed) and values
all_data = []

with open(csv_filename, 'r') as csv_file, open(output_dir, 'w', newline='') as output_file:
    reader = csv.reader(csv_file)
    writer = csv.writer(output_file)

    header = next(reader)
    writer.writerow(header)

    for i, row in enumerate(reader, start=1):
        original_timestamp = int(row[0])
        original_value = row[2]

        if i >= start_line and random.uniform(0, 1) < probability:
            delayed_timestamp = delay_timestamp(original_timestamp, min_delay, max_delay)
            row[0] = str(delayed_timestamp)
            all_data.append((delayed_timestamp, original_value))
        else:
            all_data.append((original_timestamp, original_value))

        writer.writerow(row)

# Sort all data by timestamp
all_data.sort(key=lambda x: x[0])

# Save sorted data to a new file
sorted_output_dir = output_dir.replace('.csv', '_sorted.csv')
with open(sorted_output_dir, 'w', newline='') as sorted_output_file:
    sorted_writer = csv.writer(sorted_output_file)
    sorted_writer.writerow(['Timestamp', 'Value'])
    for timestamp, value in all_data:
        sorted_writer.writerow([str(timestamp), value])

print(f"Events file with timestamps and values (original and delayed) exported in {output_dir}")
print(f"Sorted timestamps and values exported in {sorted_output_dir}")
