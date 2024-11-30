cmake_minimum_required(VERSION 3.15)

project(lmkdb)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set paths for external libraries
set(INCLUDE_PATHS include)

# Include directories
include_directories(${INCLUDE_PATHS})

# Source and header files
file(GLOB_RECURSE SOURCE_FILES src/*.cpp)
file(GLOB_RECURSE HEADER_FILES include/*.h)
set(SOURCE_NON_MAIN ${SOURCE_FILES})
list(FILTER SOURCE_NON_MAIN EXCLUDE REGEX "src/main.cpp$")

# Enable necessary policies
cmake_policy(SET CMP0144 NEW) # Find package uses upper-case <PACKAGENAME>_ROOT
cmake_policy(SET CMP0167 NEW) # Use modern Boost CMake package

# Boost library 
find_package(Boost REQUIRED COMPONENTS filesystem) # Find and link Boost

# Ensure Boost is found
if(Boost_FOUND)
    message(STATUS "Boost found: ${Boost_INCLUDE_DIRS}")
    include_directories(${Boost_INCLUDE_DIRS})
else()
    message(FATAL_ERROR "Boost not found!")
endif()

# Readline library
# Add an option to specify the Readline installation path manually
set(READLINE_ROOT "$ENV{HOME}/readline" CACHE PATH "Path to locally installed Readline library")

find_path(READLINE_INCLUDE_DIR readline/readline.h PATHS ${READLINE_ROOT}/include)
find_library(READLINE_LIBRARY readline PATHS ${READLINE_ROOT}/lib)

if (READLINE_INCLUDE_DIR AND READLINE_LIBRARY)
    message(STATUS "Readline found: ${READLINE_INCLUDE_DIR}")
    message(STATUS "Readline library found: ${READLINE_LIBRARY}")
else()
    message(FATAL_ERROR "Readline not found! Set READLINE_ROOT to the installation directory.")
endif()

# Find Threads
find_package(Threads REQUIRED)

# Add the main executable
add_executable(lmkdb ${SOURCE_FILES})
target_link_libraries(lmkdb
    PUBLIC
        Boost::filesystem
        Threads::Threads
        ${READLINE_LIBRARY}
)
target_include_directories(lmkdb
    PUBLIC
        ${INCLUDE_PATHS}
        ${Boost_INCLUDE_DIRS}
        ${READLINE_INCLUDE_DIR}
)

# Add stricter warning options (optional)
target_compile_options(lmkdb PRIVATE -Wall -Wextra -Wpedantic)

# Testing stuff
enable_testing()
include(FetchContent)
# Fetch googletest - no need to brew install ;)
FetchContent_Declare(
  googletest
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
)
FetchContent_MakeAvailable(googletest)
add_executable(tests test/main.cpp ${SOURCE_NON_MAIN})
target_link_libraries(tests PRIVATE gtest Boost::filesystem)
target_include_directories(tests PRIVATE src ${INCLUDE_PATHS} ${Boost_INCLUDE_DIRS})

include(GoogleTest)
gtest_discover_tests(tests)
