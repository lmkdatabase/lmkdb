cmake_minimum_required(VERSION 3.15)
set(CMAKE_C_COMPILER "/usr/bin/clang")
set(CMAKE_CXX_COMPILER "/usr/bin/clang++")

project(lmkdb)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set paths for external libraries (if any)
# For this basic setup, external libraries are not necessary

# Include directories
set(INCLUDE_PATHS include)

# Include directories
include_directories(${INCLUDE_PATHS})

# Source and header files
set(SOURCE_FILES 
    src/main.cpp
    src/test.cpp
)
set(HEADER_FILES
    include/test.h
)

# Main project executable
add_executable(lmkdb ${SOURCE_FILES})

# Include directories for main executable
target_include_directories(lmkdb PUBLIC ${INCLUDE_PATHS})

