cmake_minimum_required(VERSION 3.15)
set(CMAKE_C_COMPILER "/usr/bin/clang")
set(CMAKE_CXX_COMPILER "/usr/bin/clang++")

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

# boost library 
find_package(Boost REQUIRED COMPONENTS filesystem) # Find and link Boost

# Ensure Boost is found
if(Boost_FOUND)
    message(STATUS "Boost found: ${Boost_INCLUDE_DIRS}")
    include_directories(${Boost_INCLUDE_DIRS})
else()
    message(FATAL_ERROR "Boost not found!")
endif()

# readline library
find_path(READLINE_INCLUDE_DIR readline/readline.h PATHS /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/include)
find_library(READLINE_LIBRARY readline PATHS /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk/usr/lib)


add_executable(lmkdb ${SOURCE_FILES}) # Add executable
target_link_libraries(lmkdb PUBLIC Boost::filesystem) # Link Boost libraries
target_include_directories(lmkdb PUBLIC ${INCLUDE_PATHS} ${Boost_INCLUDE_DIRS}) # Include directories for the target

if (READLINE_INCLUDE_DIR AND READLINE_LIBRARY)
	message(STATUS "Readline found: ${READLINE_INCLUDE_DIR}")
    target_include_directories(lmkdb PRIVATE ${READLINE_INCLUDE_DIR})
    target_link_libraries(lmkdb PRIVATE ${READLINE_LIBRARY})
else()
    message(FATAL_ERROR "Readline not found!")
endif()

# Add stricter warning options (optional)
target_compile_options(lmkdb PRIVATE -Wall -Wextra -Wpedantic)

# testing stuff
enable_testing()
include(FetchContent)
# fetch googletest - no need to brew install ;)
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
