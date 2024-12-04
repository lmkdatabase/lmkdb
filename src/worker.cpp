#include "worker.h"
#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

using namespace std;

unordered_multimap<string, vector<string>> JoinWorker::buildHashTable(
    const string& shard_path, int attr_pos) {
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
        ifstream check_file(shard_A);
        if (!check_file.good()) {
            cerr << "Error: Cannot open shard file: " << shard_A << endl;
            continue;
        }

        // Build hash table for this shard of A
        auto hash_table = buildHashTable(shard_A, attr_pos_A);

        // Stream through all shards of B
        for (const auto& shard_B : all_shards_B) {
            ifstream file_B(shard_B);
            if (!file_B.good()) {
                cerr << "Error: Cannot open shard file: " << shard_B << endl;
                continue;
            }

            string line;
            while (getline(file_B, line)) {
                vector<string> record_B;
                istringstream ss(line);
                string field;
                while (getline(ss, field, ',')) {
                    record_B.push_back(field);
                }

                if (record_B.size() <= attr_pos_B) {
                    cerr << "Error: Invalid record structure in shard B"
                         << endl;
                    continue;
                }

                // Find matches
                auto range = hash_table.equal_range(record_B[attr_pos_B]);
                for (auto it = range.first; it != range.second; ++it) {
                    lock_guard<mutex> lock(output_mutex);
                    ofstream out(output_path, ios::app);
                    if (!out.good()) {
                        cerr
                            << "Error: Cannot open output file: " << output_path
                            << endl;
                        continue;
                    }

                    // Write joined record
                    const auto& record_A = it->second;
                    string output_line;

                    // Write all fields from A
                    for (size_t i = 0; i < record_A.size(); ++i) {
                        if (i > 0) output_line += ",";
                        output_line += record_A[i];
                    }

                    // Write all fields from B
                    for (const auto& field : record_B) {
                        output_line += "," + field;
                    }

                    out << output_line << "\n";
                }
            }
        }
    }
}
