import os
import configparser

# Load the configuration file
config = configparser.ConfigParser()
config.read('evaluation/config_mb.ini')

os.system(f"gcc multi_buffer.c frames.c -o cframes")

for section in config.sections():
    cmd_args = dict(config.items(section))

    input_file = cmd_args["input_file"]
    frame_type = int(cmd_args["frame_type"])

    output_file = f"evaluation/results/output_{section}.csv"

    command = f"./cframes {input_file} {frame_type} > {output_file}"

    print(f"Running test {section}: {command}")
    os.system(command)
    print(f"Test {section} completed\n----------")

print("Evaluation completed")

