cmake_minimum_required(VERSION 3.26)
project(CFrames C)

set(CMAKE_C_STANDARD 11)

add_executable(CFrames
        frames.c
        frames.h
        multi_buffer.c)
