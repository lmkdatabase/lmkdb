#include <iostream>
#include <vector>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <string>
#include <cuda_runtime.h>
#include <filesystem>
#include <thread>

using namespace std;
namespace fs = std::filesystem;

#define CUDA_CALL(call)                                                      \
    {                                                                        \
        cudaError_t err = call;                                              \
        if (err != cudaSuccess) {                                            \
            cerr << "CUDA Error at " << __FILE__ << ":" << __LINE__ << " : " \
                 << cudaGetErrorString(err) << endl;                         \
            exit(EXIT_FAILURE);                                              \
        }                                                                    \
    }

const int MAX_STRING_LEN = 256;
const int MAX_BATCH_SIZE = 10000;

struct JoinTask {
    string tb1File;
    string tb2File;
    string outputFile;
    string metadata1;
    string metadata2;
};

vector<string> getShardPaths(const string& folderPath) {
    vector<string> shardPaths;

    for (const auto& entry : fs::directory_iterator(folderPath)) {
        if (entry.is_regular_file() && entry.path().extension() == ".csv") {
            shardPaths.push_back(entry.path().string());
        }
    }

    return shardPaths;
}

unordered_map<string, int> loadMetadata(const string& metadataFile) {
    unordered_map<string, int> metadata;
    ifstream file(metadataFile);
    if (!file.is_open()) {
        cerr << "Error: Could not open metadata file " << metadataFile << endl;
        exit(EXIT_FAILURE);
    }

    string line;
    while (getline(file, line)) {
        istringstream ss(line);
        string attribute;
        int index;
        if (getline(ss, attribute, ',') && ss >> index) {
            metadata[attribute] = index;
        }
    }
    return metadata;
}

__device__ int device_strcmp(const char* s1, const char* s2) {
    while (*s1 && (*s1 == *s2)) {
        s1++;
        s2++;
    }
    return *(const unsigned char*)s1 - *(const unsigned char*)s2;
}

__device__ void device_strcpy(char* dest, const char* src) {
    while ((*dest++ = *src++) != '\0');
}

__global__ void joinKernel(char* tb1, char* tb2, char* results, int* resultCount,
                           int cols1, int cols2, int size1, int size2,
                           int joinIdx1, int joinIdx2, int maxResults) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= size2) return;

    for (int i = 0; i < size1; i++) {
        if (device_strcmp(&tb1[i * cols1 * MAX_STRING_LEN + joinIdx1 * MAX_STRING_LEN],
                          &tb2[idx * cols2 * MAX_STRING_LEN + joinIdx2 * MAX_STRING_LEN]) == 0) {
            int insertIdx = atomicAdd(resultCount, 1);

            if (insertIdx < maxResults) {
                for (int j = 0; j < cols1; j++) {
                    device_strcpy(&results[insertIdx * (cols1 + cols2) * MAX_STRING_LEN + j * MAX_STRING_LEN],
                                  &tb1[i * cols1 * MAX_STRING_LEN + j * MAX_STRING_LEN]);
                }
                for (int j = 0; j < cols2; j++) {
                    device_strcpy(&results[insertIdx * (cols1 + cols2) * MAX_STRING_LEN +
                                           (cols1 + j) * MAX_STRING_LEN],
                                  &tb2[idx * cols2 * MAX_STRING_LEN + j * MAX_STRING_LEN]);
                }
            } else {
                return;
            }
        }
    }
}

size_t readBatch(ifstream& file, char* buffer, int numCols) {
    size_t currSize = 0;
    string line;

    while (currSize < MAX_BATCH_SIZE && getline(file, line)) {
        istringstream ss(line);
        string value;
        int col = 0;

        while (col < numCols && getline(ss, value, ',')) {
            strncpy(&buffer[currSize * numCols * MAX_STRING_LEN + col * MAX_STRING_LEN],
                    value.c_str(), MAX_STRING_LEN - 1);
            buffer[currSize * numCols * MAX_STRING_LEN + col * MAX_STRING_LEN + MAX_STRING_LEN - 1] = '\0';
            col++;
        }
        currSize++;
    }

    return currSize;
}

void processPairOnGPU(const JoinTask& jTask, const string& joinAttr) {
    auto metadata1 = loadMetadata(jTask.metadata1);
    auto metadata2 = loadMetadata(jTask.metadata2);

    if (metadata1.find(joinAttr) == metadata1.end() ||
        metadata2.find(joinAttr) == metadata2.end()) {
        cerr << "Error: Join attribute not found in metadata" << endl;
        return;
    }

    int joinIdx1 = metadata1[joinAttr];
    int joinIdx2 = metadata2[joinAttr];

    int cols1 = metadata1.size();
    int cols2 = metadata2.size();

    ifstream file1(jTask.tb1File);
    ifstream file2(jTask.tb2File);

    if (!file1.is_open() || !file2.is_open()) {
        cerr << "Error: Could not open input files" << endl;
        return;
    }

    ofstream outFile(jTask.outputFile, ios::out | ios::trunc);
    if (!outFile.is_open()) {
        cerr << "Error: Could not open output file" << endl;
        return;
    }

    char *batch1, *batch2;
    CUDA_CALL(cudaHostAlloc(&batch1, MAX_BATCH_SIZE * cols1 * MAX_STRING_LEN, cudaHostAllocDefault));
    CUDA_CALL(cudaHostAlloc(&batch2, MAX_BATCH_SIZE * cols2 * MAX_STRING_LEN, cudaHostAllocDefault));

    char *d_tb1, *d_tb2, *d_results;
    int *d_resultCount;
    CUDA_CALL(cudaMalloc(&d_tb1, MAX_BATCH_SIZE * cols1 * MAX_STRING_LEN));
    CUDA_CALL(cudaMalloc(&d_tb2, MAX_BATCH_SIZE * cols2 * MAX_STRING_LEN));
    CUDA_CALL(cudaMalloc(&d_resultCount, sizeof(int)));

    cudaStream_t stream;
    CUDA_CALL(cudaStreamCreate(&stream));

    size_t batch1Size = 0;
    while ((batch1Size = readBatch(file1, batch1, cols1)) > 0) {
        CUDA_CALL(cudaMemcpyAsync(d_tb1, batch1, batch1Size * cols1 * MAX_STRING_LEN, cudaMemcpyHostToDevice, stream));

        file2.clear();
        file2.seekg(0, ios::beg);

        size_t batch2Size = 0;
        while ((batch2Size = readBatch(file2, batch2, cols2)) > 0) {
            CUDA_CALL(cudaMemcpyAsync(d_tb2, batch2, batch2Size * cols2 * MAX_STRING_LEN, cudaMemcpyHostToDevice, stream));

            int maxResLen = batch1Size * batch2Size;
            CUDA_CALL(cudaMalloc(&d_results, maxResLen * (cols1 + cols2) * MAX_STRING_LEN));

            vector<char> results(maxResLen * (cols1 + cols2) * MAX_STRING_LEN);
            int zero = 0;
            CUDA_CALL(cudaMemcpyAsync(d_resultCount, &zero, sizeof(int), cudaMemcpyHostToDevice, stream));

            joinKernel<<<(batch2Size + 127) / 128, 128, 0, stream>>>(
                d_tb1, d_tb2, d_results, d_resultCount,
                cols1, cols2, batch1Size, batch2Size,
                joinIdx1, joinIdx2, maxResLen);

            CUDA_CALL(cudaStreamSynchronize(stream)); 
            int resultCount = 0;
            CUDA_CALL(cudaMemcpyAsync(&resultCount, d_resultCount, sizeof(int), cudaMemcpyDeviceToHost, stream));
            CUDA_CALL(cudaMemcpyAsync(results.data(), d_results, resultCount * (cols1 + cols2) * MAX_STRING_LEN, cudaMemcpyDeviceToHost, stream));

            CUDA_CALL(cudaStreamSynchronize(stream));

            for (int i = 0; i < resultCount; i++) {
                for (int j = 0; j < cols1 + cols2; j++) {
                    outFile << &results[i * (cols1 + cols2) * MAX_STRING_LEN + j * MAX_STRING_LEN];
                    if (j < cols1 + cols2 - 1) outFile << ",";
                }
                outFile << endl;
            }

            CUDA_CALL(cudaFree(d_results));
        }
    }

    CUDA_CALL(cudaFree(d_tb1));
    CUDA_CALL(cudaFree(d_tb2));
    CUDA_CALL(cudaFree(d_resultCount));
    CUDA_CALL(cudaFreeHost(batch1));
    CUDA_CALL(cudaFreeHost(batch2));
    CUDA_CALL(cudaStreamDestroy(stream));
}

void processJoinTasksSubset(const vector<JoinTask>& jTasksSubset, const string& joinAttr) {
    for (const auto& jTask : jTasksSubset) {
        processPairOnGPU(jTask, joinAttr);
    }
}

void processJoinTasksParallel(const vector<jTask>& jTasks, const string& joinAttr, int numThreads) {
    vector<thread> threads;
    size_t jTasksPerThread = jTasks.size() / numThreads;

    for (int i = 0; i < numThreads; i++) {
        size_t startIdx = i * jTasksPerThread;
        size_t endIdx = (i == numThreads - 1) ? jTasks.size() : (i + 1) * jTasksPerThread;

        threads.emplace_back(processJoinTasksSubset, vector<jTask>(jTasks.begin() + startIdx, jTasks.begin() + endIdx), joinAttr);
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}


int main() {
    string tb1Folder = "/content/drive/My Drive/lmkdb/london";
    string tb2Folder = "/content/drive/My Drive/lmkdb/stations";

    vector<string> tb1Shards = getShardPaths(tb1Folder);
    vector<string> tb2Shards = getShardPaths(tb2Folder);

    vector<JoinTask> jTasks;

    int outputFileIndex = 1;
    for (const string& tb1File : tb1Shards) {
        for (const string& tb2File : tb2Shards) {
            JoinTask jTask;
            jTask.tb1File = tb1File;
            jTask.tb2File = tb2File;

            jTask.outputFile = "res_" + to_string(outputFileIndex++) + ".csv";

            jTask.metadata1 = tb1Folder + "/metadata.txt";
            jTask.metadata2 = tb2Folder + "/metadata.txt";

            jTasks.push_back(jTask);
        }
    }

    string joinAttr = "start_station_id";

    processJoinTasksParallel(jTasks, joinAttr, 2);
    return 0;
}
