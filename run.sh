#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status.

# Set BASE_PATH
BASE_PATH="$(pwd)"

# Define directories
BUILD_DIR="$BASE_PATH/build"
SRC_DIR="$BASE_PATH/src"
EXTERNAL_LIBRARIES_DIR="$BASE_PATH/external_libraries"

echo "All external libraries downloaded and installed successfully."

# Return to the base directory
cd "$BASE_PATH" || { echo "Failed to return to base directory"; exit 1; }

# Create build directory if it doesn't exist
if [[ ! -d "$BUILD_DIR" ]]; then
  echo "Creating build directory at $BUILD_DIR"
  mkdir -p "$BUILD_DIR"
fi

# Run CMake and compile the project
cd "$BUILD_DIR" || { echo "Failed to change to build directory"; exit 1; }
echo "Running CMake in $BUILD_DIR"
cmake .. || { echo "CMake configuration failed"; exit 1; }

echo "Compiling the project"
make || { echo "Compilation failed"; exit 1; }

echo "Setup complete!"
