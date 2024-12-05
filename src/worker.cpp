#include "worker.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace std;

JoinWorker::HashTable JoinWorker::buildHashTable(const string& shard_path,
                                                 int attr_pos) {
    unordered_multimap<string, vector<string>> table;
    ifstream file(shard_path);
    string line;

    while (getline(file, line)) {
        vector<string> record;
        istringstream ss(line);
        string field;
        while (getline(ss, field, ',')) {
            record.push_back(field);
        }
        table.insert({record[attr_pos], record});
    }
    return table;
}

void JoinWorker::processShardBatch(const vector<string>& shard_batch_A,
                                   const vector<string>& all_shards_B,
                                   int attr_pos_A, int attr_pos_B) {
    for (const auto& shard_A : shard_batch_A) {
        // Build hash table for this shard of A
        auto hash_table = buildHashTable(shard_A, attr_pos_A);

        // Stream through all shards of B
        for (const auto& shard_B : all_shards_B) {
            ifstream file_B(shard_B);
            string line;

            while (getline(file_B, line)) {
                vector<string> record_B;
                istringstream ss(line);
                string field;
                while (getline(ss, field, ',')) {
                    record_B.push_back(field);
                }

                // Find matches
                auto range = hash_table.equal_range(record_B[attr_pos_B]);
                for (auto it = range.first; it != range.second; ++it) {
                    lock_guard<mutex> lock(output_mutex);
                    // Write joined record to output
                    ofstream out(output_path, ios::app);
                    for (const auto& field : it->second) {
                        out << field << ",";
                    }
                    for (size_t i = 0; i < record_B.size(); i++) {
                        out << record_B[i];
                        if (i < record_B.size() - 1) out << ",";
                    }
                    out << "\n";
                }
            }
        }
    }
}
