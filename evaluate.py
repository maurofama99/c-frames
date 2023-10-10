import os

commands = [
    {
        "input_file": "resources/debug/sample-aggregate-1000-5.csv",
        "frame_type": 2,
        "report_policy": 0,
        "order_policy": 0,
        "buffer_type": 0,
        "X": -1,
        "Y": 0
    },
    {
        "input_file": "resources/debug/sample-aggregate-1000-5.csv",
        "frame_type": 2,
        "report_policy": 0,
        "order_policy": 0,
        "buffer_type": 0,
        "X": 25000,
        "Y": 20000
    },
    {
        "input_file": "resources/debug/sample-aggregate-1000-5.csv",
        "frame_type": 2,
        "report_policy": 0,
        "order_policy": 0,
        "buffer_type": 1,
        "X": -1,
        "Y": 0
    },
    {
        "input_file": "resources/debug/sample-aggregate-1000-5.csv",
        "frame_type": 2,
        "report_policy": 0,
        "order_policy": 0,
        "buffer_type": 1,
        "X": 125000000,
        "Y": 20000
    },
    {
        "input_file": "resources/debug/sample-aggregate-1000-5-ooo.csv",
        "frame_type": 2,
        "report_policy": 0,
        "order_policy": 1,
        "buffer_type": 0,
        "X": -1,
        "Y": 0
    },
    {
        "input_file": "resources/debug/sample-aggregate-1000-5-ooo.csv",
        "frame_type": 2,
        "report_policy": 0,
        "order_policy": 1,
        "buffer_type": 0,
        "X": 25000,
        "Y": 20000
    },
    {
        "input_file": "resources/debug/sample-aggregate-1000-5-ooo.csv",
        "frame_type": 2,
        "report_policy": 0,
        "order_policy": 2,
        "buffer_type": 0,
        "X": -1,
        "Y": 0
    },
    {
        "input_file": "resources/debug/sample-aggregate-1000-5-ooo.csv",
        "frame_type": 2,
        "report_policy": 0,
        "order_policy": 2,
        "buffer_type": 0,
        "X": 25000,
        "Y": 20000
    },
]

for idx, cmd_args in enumerate(commands):
    input_file = cmd_args["input_file"]
    frame_type = cmd_args["frame_type"]
    report_policy = cmd_args["report_policy"]
    order_policy = cmd_args["order_policy"]
    buffer_type = cmd_args["buffer_type"]
    X = cmd_args["X"]
    Y = cmd_args["Y"]

    output_file = f"evaluation/results/output_{idx}.csv"

    command = f"./cframes {input_file} {frame_type} {report_policy} {order_policy} {buffer_type} {X} {Y} > {output_file}"

    os.system(command)

    print(f"Running ({idx}): {command}")

print("Execution completed")
