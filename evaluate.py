import os
import configparser

# Load the configuration file
config = configparser.ConfigParser()
config.read('evaluation/config.ini')

os.system(f"gcc c_frames_v2.c -o cframes")

for section in config.sections():
    cmd_args = dict(config.items(section))

    input_file = cmd_args["input_file"]
    frame_type = int(cmd_args["frame_type"])
    report_policy = int(cmd_args["report_policy"])
    order_policy = int(cmd_args["order_policy"])
    buffer_type = int(cmd_args["buffer_type"])
    X = int(cmd_args["x"])
    Y = int(cmd_args["y"])

    output_file = f"evaluation/results/output_{order_policy}_{buffer_type}_{X}_{section}.csv"

    command = f"./cframes {input_file} {frame_type} {report_policy} {order_policy} {buffer_type} {X} {Y} > {output_file}"

    print(f"Running test {section}: {command}")
    os.system(command)
    print(f"Test {section} completed\n----------")

print("Evaluation completed")

