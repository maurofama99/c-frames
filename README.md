# c-frames
C implementation of FRAMES, data-driven windows. \
[Grossniklaus, Michael & Maier, David & Miller, James & Moorthy, Sharmadha & Tufte, Kristin. (2016). Frames: Data-driven Windows. 13-24. 10.1145/2933267.2933304.](https://www.researchgate.net/publication/303972106_Frames_Data-driven_Windows)

### `single_buffer.c`
Single-buffer version, the tuples are processed using only one linked list, implemented as a queue, to store them. Use `define`s at the top of the code to set the desired parameters.
