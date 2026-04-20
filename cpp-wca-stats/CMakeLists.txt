cmake_minimum_required(VERSION 3.28)
project(wca_stats CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)
add_compile_options($<$<CONFIG:Release>:-O3>)
add_compile_options($<$<CONFIG:Release>:-flto>)
add_link_options($<$<CONFIG:Release>:-flto>)

find_library(ZIP_LIB NAMES zip REQUIRED)

add_executable(wca-stats
    src/main.cpp
    src/loader.cpp
    src/calc.cpp
    src/output.cpp
)
target_link_libraries(wca-stats PRIVATE ${ZIP_LIB})
