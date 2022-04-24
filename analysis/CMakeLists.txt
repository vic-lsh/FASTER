cmake_minimum_required(VERSION 3.10)
project(analysis)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_BUILD_TYPE RelWithDebInfo)

add_executable(compute compute.cpp)

target_link_libraries(compute pthread)
