cmake_minimum_required(VERSION 3.26)
project(CFrames C)

set(CMAKE_C_STANDARD 11)

add_executable(CFrames
        multi_buffer.c
        frames.c)
