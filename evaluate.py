import os

commands = [
    # MULTI BUFFER, IN ORDER, NO EVICT
    {
        "input_file": "'resources/dataset/sample-delta-1000;5.csv'",
        "frame_type": 1,
        "report_policy": 0,
        "order_policy": 0,
        "buffer_type": 1,
        "X": -1,
        "Y": 5000
    },
    # MULTI BUFFER, IN ORDER, EVICT
    {
        "input_file": "'resources/dataset/sample-delta-1000;5.csv'",
        "frame_type": 1,
        "report_policy": 0,
        "order_policy": 0,
        "buffer_type": 1,
        "X": 34000000,
        "Y": 5000
    },
    # SINGLE BUFFER, IN ORDER, NO EVICT
    {
        "input_file": "'resources/dataset/sample-delta-1000;5.csv'",
        "frame_type": 1,
        "report_policy": 0,
        "order_policy": 0,
        "buffer_type": 0,
        "X": -1,
        "Y": 5000
    },
    # SINGLE BUFFER, IN ORDER, EVICT
    {
        "input_file": "'resources/dataset/sample-delta-1000;5.csv'",
        "frame_type": 1,
        "report_policy": 0,
        "order_policy": 0,
        "buffer_type": 0,
        "X": 6000,
        "Y": 5000
    },
    # SINGLE BUFFER, OOO EAGER, EVICT
    {
        "input_file": "'resources/dataset/sample-delta-1000;5.csv'",
        "frame_type": 1,
        "report_policy": 0,
        "order_policy": 1,
        "buffer_type": 0,
        "X": 6000,
        "Y": 5000
    },
    # SINGLE BUFFER, OOO EAGER, NO EVICT
    {
        "input_file": "'resources/dataset/sample-delta-1000;5.csv'",
        "frame_type": 1,
        "report_policy": 0,
        "order_policy": 1,
        "buffer_type": 0,
        "X": -1,
        "Y": 5000
    },
    # SINGLE BUFFER, OOO LAZY, EVICT
    {
        "input_file": "'resources/dataset/sample-delta-1000;5.csv'",
        "frame_type": 1,
        "report_policy": 0,
        "order_policy": 2,
        "buffer_type": 0,
        "X": 6000,
        "Y": 5000
    },
    # SINGLE BUFFER, OOO LAZY, NO EVICT
    {
        "input_file": "'resources/dataset/sample-delta-1000;5.csv'",
        "frame_type": 1,
        "report_policy": 0,
        "order_policy": 2,
        "buffer_type": 0,
        "X": -1,
        "Y": 5000
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

    output_file = f"evaluation/results/output_{order_policy}_{buffer_type}_{X}.csv"

    command = f"./cframes {input_file} {frame_type} {report_policy} {order_policy} {buffer_type} {X} {Y} > {output_file}"

    print(f"Running test {idx}: {command}")
    os.system(command)
    print(f"Test {idx} completed\n----------")

print("Evaluation completed")
