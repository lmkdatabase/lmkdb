#include "worker.h"
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

bool JoinWorker::processShardBatch(const string& shard_A,
                                   const vector<string>& all_shards_B,
                                   int attr_pos_A, int attr_pos_B) {
    unordered_multimap<string, pair<string, streampos>> index;

    ifstream file_A(shard_A);
    string line;
    streampos pos = 0;

    while (getline(file_A, line)) {
        vector<string> record;
        istringstream ss(line);
        string field;
        int current_pos = 0;

        while (getline(ss, field, ',')) {
            if (current_pos == attr_pos_A) {
                index.insert({field, {shard_A, pos}});
                break;
            }
            current_pos++;
        }
        pos = file_A.tellg();
    }

    for (const auto& shard_B : all_shards_B) {
        ifstream file_B(shard_B);
        while (getline(file_B, line)) {
            vector<string> record_B;
            istringstream ss(line);
            string join_key;

            for (int i = 0; i <= attr_pos_B; i++) {
                getline(ss, join_key, ',');
            }

            auto range = index.equal_range(join_key);
            for (auto it = range.first; it != range.second; ++it) {
                ifstream reader(it->second.first);
                reader.seekg(it->second.second);

                string matching_record;
                getline(reader, matching_record);

                lock_guard<mutex> lock(output_mutex);
                ofstream out(output_path, ios::app);
                out << matching_record << "," << line << "\n";
            }
        }
    }
    return true;
}
