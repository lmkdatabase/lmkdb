#include <fstream>
#include <unordered_map>
#include <vector>
#include <string>
#include <sstream>
#include <filesystem>
#include "cuda_runtime.h"
#include "device_launch_parameters.h"
#include "thrust/device_vector.h"
#include "thrust/host_vector.h"
#include <memory>
#include <iostream>
#include <chrono>

// Utility function to split strings (CSV parsing)
__host__ std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

// Load a CSV file and extract rows and a specific column
__host__ void loadCsv(const std::string& filename, int columnIndex,
             std::vector<std::string>& rows, std::vector<int>& column,
             std::unordered_map<std::string, int>& stringToIntMap,
             std::unordered_map<int, std::string>& intToStringMap) {
    std::ifstream file(filename);
    std::string line;
    int currentIndex = 0;

    while (std::getline(file, line)) {
        rows.push_back(line); // Save the full row
        std::vector<std::string> tokens = split(line, ',');
        if (columnIndex < tokens.size()) {
            const std::string& value = tokens[columnIndex];
            if (stringToIntMap.find(value) == stringToIntMap.end()) {
                stringToIntMap[value] = currentIndex;
                intToStringMap[currentIndex] = value; // Reverse mapping
                ++currentIndex;
            }
            column.push_back(stringToIntMap[value]); // Save mapped column value
        }
    }
}

// GPU join kernel to find match indices
__global__ void joinWithIndices(const int* const left, const int* const right, int leftSize, int rightSize, int* matches) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;

    if (idx < leftSize) {
        for (int j = 0; j < rightSize; ++j) {
            if (left[idx] == right[j]) {
                matches[idx] = j; 
                return; 
            }
        }
        matches[idx] = -1;
    }
}

// GPU join execution
std::unique_ptr<thrust::host_vector<int>> runGpuJoinWithIndices(const thrust::device_vector<int>& left,
                                                                const thrust::device_vector<int>& right) {
    int leftSize = left.size();
    thrust::device_vector<int> matches(leftSize, -1);

    int threadCount = 1024;
    int nBlocks = (leftSize + threadCount - 1) / threadCount;

    joinWithIndices<<<nBlocks, threadCount>>>(thrust::raw_pointer_cast(left.data()),
                                              thrust::raw_pointer_cast(right.data()),
                                              leftSize, right.size(),
                                              thrust::raw_pointer_cast(matches.data()));

    cudaDeviceSynchronize();

    return std::make_unique<thrust::host_vector<int>>(matches);
}

__host__ std::vector<std::string> getShardPaths(const std::string& shardDir) {
    std::vector<std::string> shardPaths;
    for (const auto& entry : std::filesystem::directory_iterator(shardDir)) {
        shardPaths.push_back(entry.path().string());
    }
    return shardPaths;
}

int main() {
    auto start = std::chrono::high_resolution_clock::now();
    
    std::vector<std::string> leftShards = getShardPaths("/content/drive/My Drive/lmkdb/london2");
    std::vector<std::string> rightShards = getShardPaths("/content/drive/My Drive/lmkdb/stations/");

    auto shardLoadEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Shard paths loaded in " 
              << std::chrono::duration_cast<std::chrono::milliseconds>(shardLoadEnd - start).count()
              << " ms.\n";

    std::ofstream outputFile("joined_table.csv", std::ios::app);
    if (!outputFile.is_open()) {
        std::cerr << "Error: Could not open output file.\n";
        return 1;
    }

    for (const auto& leftShard : leftShards) {
        for (const auto& rightShard : rightShards) {
            std::cout << "Processing shards: " << leftShard << " and " << rightShard << std::endl;

            // Start timing for loading CSV files
            auto loadStart = std::chrono::high_resolution_clock::now();

            std::vector<std::string> leftRows, rightRows;
            std::vector<int> leftColumn, rightColumn;
            std::unordered_map<std::string, int> stringToIntMap;
            std::unordered_map<int, std::string> intToStringMap;

            loadCsv(leftShard, 5, leftRows, leftColumn, stringToIntMap, intToStringMap);
            loadCsv(rightShard, 1, rightRows, rightColumn, stringToIntMap, intToStringMap);

            auto loadEnd = std::chrono::high_resolution_clock::now();
            std::cout << "CSV files loaded in " 
                      << std::chrono::duration_cast<std::chrono::milliseconds>(loadEnd - loadStart).count()
                      << " ms.\n";

            // Start timing for GPU operations
            // auto gpuStart = std::chrono::high_resolution_clock::now();

            // thrust::device_vector<int> leftGpu(leftColumn.begin(), leftColumn.end());
            // thrust::device_vector<int> rightGpu(rightColumn.begin(), rightColumn.end());

            // auto matches = runGpuJoinWithIndices(leftGpu, rightGpu);

            // auto gpuEnd = std::chrono::high_resolution_clock::now();
            // std::cout << "GPU operations completed in " 
            //           << std::chrono::duration_cast<std::chrono::milliseconds>(gpuEnd - gpuStart).count()
            //           << " ms.\n";

            // // Start timing for writing output
            // auto writeStart = std::chrono::high_resolution_clock::now();

            // for (size_t i = 0; i < matches->size(); ++i) {
            //     int matchIndex = (*matches)[i];
            //     if (matchIndex != -1) {
            //         std::string leftRow = leftRows[i];
            //         std::string rightRow = rightRows[matchIndex];
            //         outputFile << leftRow << "," << rightRow << "\n";
            //     }
            // }

            // auto writeEnd = std::chrono::high_resolution_clock::now();
            // std::cout << "Output writing completed in " 
            //           << std::chrono::duration_cast<std::chrono::milliseconds>(writeEnd - writeStart).count()
            //           << " ms.\n";
        }
    }

    outputFile.close();

    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Total execution time: " 
              << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << " ms.\n";

    return 0;
}
