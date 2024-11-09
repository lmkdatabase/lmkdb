# lmkdb

## Build Instructions

1. **Clone the Repository** (if not already done):
   ```bash
   git clone https://github.com/lmkdatabase/lmkdb.git
   cd lmkdb
   ```
2. **Create a Build Directory** 
    ```bash
    mkdir build
    cd build
    ```
3. **Run CMake** Configure the project using CMake from the build directory.
    ```bash
    cmake ..
    ```
4. **Compile the Project** Once configured, use make to build the project.
    ```bash
    make
    ```
5. **Run the Executable** After building, an executable named lmkdb will be created. Run it with:
    ```bash
    ./lmkdb
    ```