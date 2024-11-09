cmake_minimum_required(VERSION 3.15)
set(CMAKE_C_COMPILER "/usr/bin/clang")
set(CMAKE_CXX_COMPILER "/usr/bin/clang++")

project(lmkdb)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set paths for external libraries
set(INCLUDE_PATHS include)

# Include directories
include_directories(${INCLUDE_PATHS})

# Source and header files
set(SOURCE_FILES 
    src/main.cpp
    src/Test.cpp
)
set(HEADER_FILES
    include/Test.h
)

# Set Boost root directory to the Homebrew path
set(BOOST_ROOT "/opt/homebrew/opt/boost") 
find_package(Boost REQUIRED COMPONENTS filesystem)

# Main project executable
add_executable(lmkdb ${SOURCE_FILES})

# Link Boost Filesystem
target_link_libraries(lmkdb PUBLIC Boost::filesystem)

# Include directories for main executable
target_include_directories(lmkdb PUBLIC ${INCLUDE_PATHS})
