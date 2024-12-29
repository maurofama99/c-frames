import os
import configparser

# Load the configuration file
config = configparser.ConfigParser()
config.read('evaluation/config.ini')

os.system(f"gcc single_buffer.c -o cframes")

for section in config.sections():
    cmd_args = dict(config.items(section))

    input_file = cmd_args["input_file"]
    frame_type = int(cmd_args["frame_type"])
    report_policy = int(cmd_args["report_policy"])
    order_policy = int(cmd_args["order_policy"])
    buffer_type = int(cmd_args["buffer_type"])
    X = int(cmd_args["x"])
    Y = int(cmd_args["y"])

    if buffer_type == 0:
        output_buffer = "single"
        if order_policy == 0:
            output_order = "io"
        elif order_policy == 1:
            output_order = "eager"
        elif order_policy == 2:
            output_order = "lazy"
        else:
            exit(255)
    else:
        exit(255)

    output_file = f"evaluation/results/{output_buffer}_{output_order}_{frame_type}.csv"

    command = f"./cframes {input_file} {frame_type} {report_policy} {order_policy} {buffer_type} {X} {Y} > {output_file}"

    print(f"Running test {output_buffer}_{output_order}: {command}")
    os.system(command)
    print(f"Test {output_buffer}_{output_order} completed\n----------")

print("Evaluation completed")

