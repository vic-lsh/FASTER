cmake_minimum_required(VERSION 3.10)
project(analysis)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_BUILD_TYPE RelWithDebInfo)

add_executable(mrc mrc.cpp)
add_executable(mm_miss mm_miss.cpp)

target_link_libraries(mrc pthread)
target_link_libraries(mm_miss pthread)
