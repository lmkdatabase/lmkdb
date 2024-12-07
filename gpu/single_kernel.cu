#include <fstream>
#include <unordered_map>
#include <vector>
#include <string>
#include <sstream>
#include <filesystem>
#include <memory>
#include <iostream>
#include <chrono>

#include "cuda_runtime.h"
#include "device_launch_parameters.h"
#include "thrust/device_vector.h"
#include "thrust/host_vector.h"

using namespace std;

__host__ vector<string> split(const string& str, char delimiter) {
    vector<string> tokens;
    size_t start = 0, end;

    while ((end = str.find(delimiter, start)) != string::npos) {
        tokens.push_back(str.substr(start, end - start));
        start = end + 1;
    }

    tokens.push_back(str.substr(start));
    return tokens;
}

__host__ void loadCsv(const string& filename, int joinIdx,
             vector<string>& rows, vector<int>& column,
             unordered_map<string, int>& strToIntMap,
             unordered_map<int, string>& intToStrMap) {
    ifstream file(filename);
    if (!file.is_open()) {
        cerr << "Error: Could not open file " << filename << "\n";
        return;
    }

    string line;
    int currentIndex = 0;
    int line_count = 0;

    while (getline(file, line)) {
        line_count++;
        rows.push_back(line);

        istringstream lineStream(line);
        string field;
        int colIndex = 0;
        while (getline(lineStream, field, ',')) {
            if (colIndex == joinIdx) {
                if (strToIntMap.find(field) == strToIntMap.end()) {
                    strToIntMap[field] = currentIndex;
                    intToStrMap[currentIndex] = field;
                    ++currentIndex;
                }
                column.push_back(strToIntMap[field]);
                break;
            }
            colIndex++;
        }
    }

    file.close();
}


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

unique_ptr<thrust::host_vector<int>> gpuJoin(const thrust::device_vector<int>& left,
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

    return make_unique<thrust::host_vector<int>>(matches);
}

__host__ vector<string> getShardPaths(const string& shardDir) {
    vector<string> shardPaths;
    for (const auto& entry : filesystem::directory_iterator(shardDir)) {
        shardPaths.push_back(entry.path().string());
    }
    return shardPaths;
}

int main() {    
    vector<string> leftShards = getShardPaths("/content/drive/My Drive/lmkdb/london");
    vector<string> rightShards = getShardPaths("/content/drive/My Drive/lmkdb/stations");

    ofstream outputFile("joined_table.csv");
    if (!outputFile.is_open()) {
        cerr << "Error: Could not open output file.\n";
        return 1;
    }

    for (const auto& leftShard : leftShards) {
        for (const auto& rightShard : rightShards) {
            vector<string> leftRows, rightRows;
            vector<int> leftColumn, rightColumn;
            
            unordered_map<string, int> strToIntMap;
            unordered_map<int, string> intToStrMap;

            loadCsv(leftShard, 5, leftRows, leftColumn, strToIntMap, intToStrMap);
            loadCsv(rightShard, 1, rightRows, rightColumn, strToIntMap, intToStrMap);

            thrust::device_vector<int> leftGpu(leftColumn.begin(), leftColumn.end());
            thrust::device_vector<int> rightGpu(rightColumn.begin(), rightColumn.end());

            auto matches = gpuJoin(leftGpu, rightGpu);

            for (size_t i = 0; i < matches->size(); ++i) {
                int matchIndex = (*matches)[i];
                if (matchIndex != -1) {
                    outputFile << leftRows[i] << "," << rightRows[matchIndex] << "\n";
                }
            }
        }
    }

    outputFile.close();
    return 0;
}
