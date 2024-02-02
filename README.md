# C Frames
C implementation of FRAMES[0], data-driven windows

## Usage
The executables takes several inputs to customize the framing operation on the input file, here is the ordered list of the program arguments (all required):
### Single-Buffer program arguments
* (_string_) `input file path` path to the csv file representing the events to be processed, should be formatted as "ts, key, value"
* (_int_) `frame type` THRESHOLD = 0 | DELTA = 1 | AGGREGATE = 2
* (_int_) `report policy` ON CLOSE = 0 | ON UPDATE = 1
* (_int_) `order policy` IN ORDER = 0 | OUT OF ORDER = 1
* (_int_) `buffer type` SINGLE BUFFER = 0 | MULTI BUFFER = 1
* (_int_) `X` SINGLE BUFFER: after X frames created / MULTI BUFFER: after X ms passed / X = -1 to not evict frames
* (_int_) `Y` evict the older Y frames

### Multi-Buffer program arguments
* (_string_) `input file path` path to the csv file representing the events to be processed, should be formatted as "ts, key, value"
* (_int_) `frame type` THRESHOLD = 0 | DELTA = 1 | AGGREGATE = 2
  
### Program macros
Several `define` inside the single/multi_buffer.c file are used to customize the execution:
#### Frame parameters
* `THRESHOLD` threshold of the event's value being evaluated for the Threshold Frames construction
* `DELTA` a Delta Frame is emitted whenever the delta between the minimum and maximum value of "value" becomes greater than this parameters
* `AGGREGATE` specifies the aggregation function for the Aggregate Frames construction (AVG = 0 | SUM = 1)
* `AGGREGATE THRESHOLD` reports a new frame if the aggregate value becomes greater than this parameter
#### Memory Parameters
* `MAX CHARS` Max admitted characters in a line representing the event
* `MAX FRAMES`  Max size of multi-buffer, pay attention to choosing an eviction policy that does not cause overflow
#### Debug
* `DEBUG` set to `true` to print the current frame when the report policy is satisfied
## Evaluation
To perform the evaluation, we measure the execution time of the SECRET[1] methods while framing the input stream of events, we also save the number of tuples and frames created until that moment to measure the algorithmic complexity of the program. To customize an evaluation, it is possible to specify which tests will be executed: you can find in the `/evaluation` floder two `.ini` configuration file, one for single and one for multi buffer, list the commands set for all the configuration that you want to run following the Usage instructions, then run the python evaluation script corresponding to the chosen buffer structure in the main folder. The output files will be saved in the `evaluation/results` folder, in a format that can be processed on the jupyther notebook available in the `evaluation` folder.
## Out-of-Order dataset
In the `resources` folder you can find the dataset used to perform the evaluation, it is also available a script `ooo_generator/file_generator.py` to create an input csv file containing out of order events. Use the `config.json` file to configure a new input stream file, created from an existing one, with some delayed events, pay attention to include the name of the new file (with .csv extension) in the `output_dir` field. 



### Resources
A [technical report](https://github.com/maurofama99/c-frames/blob/main/docs/OVERT_Technical_report.pdf) of this project is available in the `docs` folder 
#### Citations
[0] [Grossniklaus, Michael & Maier, David & Miller, James & Moorthy, Sharmadha & Tufte, Kristin. (2016). Frames: Data-driven Windows. 13-24. 10.1145/2933267.2933304.](https://www.researchgate.net/publication/303972106_Frames_Data-driven_Windows) \
[1] [Botan, Irina & Derakhshan, Roozbeh & Dindar, Nihal & Haas, Laura & Miller, Renée & Tatbul, Nesime. (2010). SECRET: A Model for Analysis of the Execution Semantics of Stream Processing Systems. PVLDB. 3. 232-243.](https://www.researchgate.net/publication/220538262_SECRET_A_Model_for_Analysis_of_the_Execution_Semantics_of_Stream_Processing_Systems)

