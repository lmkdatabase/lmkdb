#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <omp.h>

// Function to process a single chunk of the file
void readChunk(std::ifstream& file, size_t start, size_t end, std::vector<std::vector<std::string>>& result) {
    file.seekg(start);

    // Skip partial row if not at the beginning of the file
    if (start != 0) {
        std::string dummy;
        std::getline(file, dummy);
    }

    std::string line;
    while (file.tellg() < static_cast<std::streampos>(end) && std::getline(file, line)) {
        std::vector<std::string> row;
        size_t pos = 0;
        while ((pos = line.find(',')) != std::string::npos) {
            row.push_back(line.substr(0, pos));
            line.erase(0, pos + 1);
        }
        row.push_back(line); // Add the last value
        result.push_back(row);
    }
}

// Main function for parallel CSV reading
std::vector<std::vector<std::string>> parallelReadCSV(const std::string& filename, int num_threads) {
    std::ifstream file(filename, std::ios::in | std::ios::ate);
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " + filename);
    }

    size_t file_size = file.tellg();
    size_t chunk_size = file_size / num_threads;

    std::vector<std::vector<std::string>> final_result;
    std::vector<std::vector<std::vector<std::string>>> thread_results(num_threads);

    #pragma omp parallel num_threads(num_threads)
    {
        int thread_id = omp_get_thread_num();
        size_t start = thread_id * chunk_size;
        size_t end = (thread_id == num_threads - 1) ? file_size : start + chunk_size;

        // Each thread has its own file stream to avoid conflicts
        std::ifstream thread_file(filename);
        readChunk(thread_file, start, end, thread_results[thread_id]);
        thread_file.close();
    }

    // Combine results from all threads
    for (const auto& thread_result : thread_results) {
        final_result.insert(final_result.end(), thread_result.begin(), thread_result.end());
    }

    return final_result;
}

int main() {
    std::string filename = "large_file.csv";
    int num_threads = 4;

    try {
        auto rows = parallelReadCSV(filename, num_threads);

        // Print a few rows to verify
        for (size_t i = 0; i < std::min(rows.size(), size_t(10)); ++i) {
            for (const auto& col : rows[i]) {
                std::cout << col << " ";
            }
            std::cout << "\n";
        }
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
    }

    return 0;
}
