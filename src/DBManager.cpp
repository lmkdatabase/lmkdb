#include "DBManager.h"
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include "Shard.h"

namespace fs = std::filesystem;
using namespace std;

DBManager::DBManager(string dbPath) : database_path(std::move(dbPath)) {
    fs::create_directory(database_path);
}

DBManager::~DBManager() = default;

string DBManager::getTablePath(const string& table_name) const {
    return database_path + table_name;
}

string DBManager::getShardPath(const string& table_name,
                               size_t shard_num) const {
    return getTablePath(table_name) + "/shard_" + to_string(shard_num) + ".csv";
}

vector<string> DBManager::getShardPaths(const string& table_name) {
    vector<string> paths;
    string table_path = getTablePath(table_name);

    for (const auto& entry : fs::directory_iterator(table_path)) {
        if (entry.path().extension() == ".csv") {
            paths.push_back(entry.path().string());
        }
    }
    return paths;
}

string DBManager::getTargetShard(const string& table_name) {
    auto shards = getShardPaths(table_name);

    if (shards.empty() || fs::file_size(shards.back()) >= MAX_SHARD_SIZE) {
        return getShardPath(table_name, shards.size());
    }

    return shards.back();
}

string DBManager::getMetadataPath(const string& table_name) const {
    return getTablePath(table_name) + "/metadata.txt";
}

bool DBManager::createTable(const string& table_name,
                            const vector<string>& attributes) {
    string table_path = getTablePath(table_name);

    if (fs::exists(table_path)) {
        cerr << "Table already exists: " << table_name << endl;
        return false;
    }

    fs::create_directory(table_path);

    int idx = 0;
    ofstream metadata(getMetadataPath(table_name));

    for (const auto& attr : attributes) {
        metadata << attr << "," << idx++ << "\n";
    }

    return true;
}

bool DBManager::insertRecord(const string& table_name,
                             const unordered_map<string, string>& attrMap) {
    if (!fs::exists(getTablePath(table_name))) {
        cerr << "Table does not exist: " << table_name << endl;
        return false;
    }

    ofstream file(getTargetShard(table_name), ios::app);
    if (!file.is_open()) {
        cerr << "Failed to open shard for writing" << endl;
        return false;
    }

    unordered_map<string, int> tableAttrMap = getMetadata(table_name);

    if (tableAttrMap.empty()) {
        cerr << "Failed to retrieve attribute mapping for table: " << table_name
             << endl;
        return false;
    }

    vector<string> values(tableAttrMap.size(), "");

    for (const auto& [attr, val] : attrMap) {
        if (tableAttrMap.find(attr) != tableAttrMap.end()) {
            values[tableAttrMap[attr]] = val;
        } else {
            cerr << "Unknown attribute: " << attr
                 << " for table: " << table_name << endl;
            return false;
        }
    }

    string record;
    for (size_t i = 0; i < values.size(); ++i) {
        record += values[i];
        if (i < values.size() - 1) record += ",";
    }

    file << record << "\n";

    return true;
}

unordered_map<string, int> DBManager::getMetadata(const string& table_name) {
    string mappingFilePath = getMetadataPath(table_name);

    if (!fs::exists(mappingFilePath)) {
        cerr << "Attribute mapping file does not exist for table: "
             << table_name << endl;
        return {};
    }

    ifstream mapping_file(mappingFilePath);
    if (!mapping_file.is_open()) {
        cerr << "Failed to open metadata file table: " << table_name << endl;
        return {};
    }

    unordered_map<string, int> attributes_map;
    string line;
    while (getline(mapping_file, line)) {
        if (line.empty()) continue;

        size_t pos = line.find(',');
        if (pos == string::npos) {
            cerr << "Invalid mapping format in line: " << line << endl;
            continue;
        }

        string attr_name = line.substr(0, pos);
        int index = stoi(line.substr(pos + 1));

        attributes_map[attr_name] = index;
    }

    return attributes_map;
}

void DBManager::readTable(const string& table_name,
                          const vector<int>& line_numbers) {
    if (!fs::exists(getTablePath(table_name))) {
        cerr << "Table does not exist: " << table_name << endl;
        return;
    }

    vector<string> shards = getShardPaths(table_name);

    if (shards.empty()) {
        cerr << "Table is empty: " << table_name << endl;
        return;
    }

    int index = 0;

    for (const auto& shard : shards) {
        ifstream file(shard);
        string line;

        while (getline(file, line)) {
            istringstream ss(line);
            string field;
            bool first = true;

            if (!line_numbers.empty() &&
                ranges::find(line_numbers, index) == line_numbers.end()) {
                index++;
                continue;
            }

            while (getline(ss, field, ',')) {
                if (!first) cout << ",";
                cout << field;
                first = false;
            }
            cout << endl;
            index++;
        }
    }
}

ShardLocation DBManager::findShardForUpdate(const string& table_name,
                                            size_t target_idx) {
    size_t idx = 0;
    for (const auto& shard : getShardPaths(table_name)) {
        ifstream file(shard);
        size_t shard_records = count(istreambuf_iterator<char>(file),
                                     istreambuf_iterator<char>(), '\n');

        if (idx + shard_records > target_idx) {
            return {.shard_path = shard, .record_index = target_idx - idx};
        }
        idx += shard_records;
    }
    return {.shard_path = "", .record_index = 0};
}

bool DBManager::updateShardRecord(
    const string& table_name, const string& shard_path, size_t target_idx,
    const unordered_map<string, string>& updates) {
    auto metadata = getMetadata(table_name);

    for (const auto& [attr, _] : updates) {
        if (metadata.find(attr) == metadata.end()) {
            cerr << "Invalid attribute: " << attr << endl;
            return false;
        }
    }

    string tmp_file = shard_path + ".tmp";

    ifstream in_file(shard_path);
    ofstream out_file(tmp_file);

    string line;
    size_t curr_idx = 0;

    while (getline(in_file, line)) {
        if (curr_idx != target_idx) {
            out_file << line << "\n";
        } else {
            vector<string> record{};

            istringstream ss(line);
            string field;

            while (getline(ss, field, ',')) {
                record.push_back(field);
            }

            for (const auto& [attr, value] : updates) {
                record[metadata.at(attr)] = value;
            }

            bool first = true;
            for (const auto& field : record) {
                if (!first) out_file << ",";
                out_file << field;
                first = false;
            }
            out_file << "\n";
        }
        curr_idx++;
    }

    fs::rename(tmp_file, shard_path);
    return true;
}

bool DBManager::updateRecord(const string& table_name, size_t id,
                             const unordered_map<string, string>& attrMap) {
    auto location = findShardForUpdate(table_name, id);

    if (location.shard_path.empty()) {
        return false;
    }

    return updateShardRecord(table_name, location.shard_path,
                             location.record_index, attrMap);
}

bool DBManager::deleteByIndex(const string& table_name, size_t id) {
    if (!fs::exists(getTablePath(table_name))) {
        cerr << "Table does not exist: " << table_name << endl;
        return false;
    }

    auto location = findShardForUpdate(table_name, id);
    if (location.shard_path.empty()) {
        cerr << "Record not found in table: " << table_name << endl;
        return false;
    }

    string tmp_file = location.shard_path + ".tmp";
    ifstream in_file(location.shard_path);
    ofstream out_file(tmp_file);

    string line;
    size_t curr_idx = 0;
    while (getline(in_file, line)) {
        if (curr_idx != location.record_index) {
            out_file << line << "\n";
        }
        curr_idx++;
    }

    in_file.close();
    out_file.close();

    fs::rename(tmp_file, location.shard_path);
    return true;
}

bool DBManager::deleteByAttributes(
    const string& table_name, const unordered_map<string, string>& attrMap) {
    if (!fs::exists(getTablePath(table_name))) {
        cerr << "Table does not exist: " << table_name << endl;
        return false;
    }

    auto metadata = getMetadata(table_name);
    if (metadata.empty()) {
        cerr << "Failed to retrieve metadata for table: " << table_name << endl;
        return false;
    }

    for (const auto& [attr, _] : attrMap) {
        if (metadata.find(attr) == metadata.end()) {
            cerr << "Invalid attribute: " << attr << endl;
            return false;
        }
    }

    auto shards = getShardPaths(table_name);
    for (const auto& shard : shards) {
        string tmp_file = shard + ".tmp";
        ifstream in_file(shard);
        ofstream out_file(tmp_file);

        string line;
        while (getline(in_file, line)) {
            vector<string> record;
            istringstream ss(line);
            string field;
            while (getline(ss, field, ',')) {
                record.push_back(field);
            }

            bool match = true;
            for (const auto& [attr, value] : attrMap) {
                if (record[metadata[attr]] != value) {
                    match = false;
                    break;
                }
            }

            if (!match) {
                out_file << line << "\n";
            }
        }

        in_file.close();
        out_file.close();

        fs::rename(tmp_file, shard);
    }

    return true;
}

bool DBManager::deleteTable(const string& table_name) {
    string table_path = getTablePath(table_name);

    if (!fs::exists(table_path)) {
        cerr << "Table does not exist: " << table_name << endl;
        return false;
    }

    fs::remove_all(table_path);

    cout << "Table deleted successfully: " << table_name << endl;

    return true;
}

// Validation functions
bool validateInputTables(const vector<string>& tables) {
    if (tables.size() < 2) {
        cerr << "Error: At least two tables are required for a join." << endl;
        return false;
    }
    return true;
}

bool validateTableAttributes(
    const string& table, const string& attr,
    const unordered_map<string, unordered_map<string, int>>& metadata) {
    if (metadata.at(table).find(attr) == metadata.at(table).end()) {
        cerr << "Error: Invalid attribute mapping for table " << table << endl;
        return false;
    }
    return true;
}

JoinPositions DBManager::calculateJoinPositions(
    const string& current_table, const string& next_table,
    const string& temp_result, const unordered_map<string, string>& attrMap,
    bool is_first_join) {
    JoinPositions positions{
        .attr_pos1 = 0, .attr_pos2 = 0, .current_num_columns = 0};
    auto metadata2 = getMetadata(next_table);
    positions.attr_pos2 = metadata2.at(attrMap.at(next_table));

    if (is_first_join) {
        auto metadata1 = getMetadata(current_table);
        positions.attr_pos1 = metadata1.at(attrMap.at(current_table));
        positions.current_num_columns = (int)metadata1.size();
    } else {
        positions.attr_pos1 = 0;
        ifstream temp_read(temp_result);
        string first_line;
        if (getline(temp_read, first_line)) {
            positions.current_num_columns =
                count(first_line.begin(), first_line.end(), ',') + 1;
        }
    }
    return positions;
}

vector<string> processTableShards(const vector<string>& shards1,
                                  const vector<string>& shards2,
                                  const string& table_path, int attr_pos1,
                                  int attr_pos2) {
    constexpr size_t MAX_THREADS = 8;
    vector<string> thread_temp_files;
    vector<thread> workers;

    for (size_t batch_start = 0; batch_start < shards1.size();
         batch_start += MAX_THREADS) {
        size_t batch_end = min(batch_start + MAX_THREADS, shards1.size());
        vector<string> current_batch(shards1.begin() + batch_start,
                                     shards1.begin() + batch_end);

        for (const auto& shard : current_batch) {
            string thread_temp = table_path + "/thread_temp_" +
                                 to_string(batch_start) + "_" +
                                 to_string(time(nullptr)) + ".csv";
            thread_temp_files.push_back(thread_temp);

            workers.emplace_back([&, shard, thread_temp]() {
                JoinWorker worker(thread_temp);
                worker.processShardBatch({shard}, shards2, attr_pos1,
                                         attr_pos2);
            });
        }

        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        workers.clear();
    }

    return thread_temp_files;
}

bool mergeThreadResults(const vector<string>& thread_temp_files,
                        const string& output_file) {
    ofstream final_out(output_file, ios::trunc);
    bool has_data = false;

    for (const auto& temp_file : thread_temp_files) {
        if (fs::exists(temp_file)) {
            ifstream thread_in(temp_file);
            if (thread_in.is_open()) {
                final_out << thread_in.rdbuf();
                has_data = true;
                thread_in.close();
            }
            fs::remove(temp_file);
        }
    }
    final_out.close();

    if (!has_data) {
        cerr << "Error: No data written to output file" << endl;
        return false;
    }
    return true;
}

void displayResults(const string& result_file) {
    ifstream final_result(result_file);
    if (!final_result.is_open()) {
        cerr << "Error: Cannot open final result file: " << result_file << endl;
        return;
    }

    string line;
    while (getline(final_result, line)) {
        cout << line << endl;
    }
}

bool DBManager::joinTables(const vector<string>& tables,
                           unordered_map<string, string>& attrMap) {
    if (!validateInputTables(tables)) {
        return false;
    }

    vector<shared_ptr<Shard>> current_shards;
    auto paths = getShardPaths(tables[0]);

    for (const auto& shard_path : paths) {
        current_shards.push_back(make_shared<Shard>(shard_path));
    }

    auto current_metadata = getMetadata(tables[0]);
    string current_table = tables[0];

    for (size_t i = 1; i < tables.size(); ++i) {
        const string& next_table = tables[i];
        auto next_metadata = getMetadata(next_table);

        if (!fs::exists(getTablePath(next_table))) {
            fs::create_directories(getTablePath(next_table));
        }

        // Get shards for next table
        vector<shared_ptr<Shard>> next_shards;
        auto next_paths = getShardPaths(next_table);

        for (const auto& shard_path : next_paths) {
            next_shards.push_back(make_shared<Shard>(shard_path));
        }

        vector<shared_ptr<Shard>> joined_shards;
        vector<future<Shard::JoinResult>> join_futures;

        for (size_t j = 0; j < current_shards.size(); j++) {
            auto current = current_shards[j];
            join_futures.push_back(current->joinAsync(
                next_shards, attrMap[current_table], attrMap[next_table],
                current_metadata, next_metadata));
        }

        // Collect results
        for (auto& future : join_futures) {
            auto result = future.get();
            if (!result.success) {
                cerr << "Join failed: " << result.error_message << endl;
                return false;
            }
            joined_shards.push_back(result.result_shard);
        }

        auto merged_shard = make_shared<Shard>();

        {
            ofstream out(merged_shard->path());
            bool first = true;

            for (const auto& shard : joined_shards) {
                ifstream in(shard->path());
                if (in.is_open()) {
                    // Keep header only from first shard
                    if (!first) {
                        string header;
                        getline(in, header);
                    }
                    out << in.rdbuf();
                    first = false;
                } else {
                    cout << "Failed to open shard for reading: "
                         << shard->path() << endl;
                }
            }
        }

        // Keep the joined shards in scope until after merge
        current_shards = {merged_shard};
        current_metadata = next_metadata;
        current_table = next_table;
    }

    if (!current_shards.empty()) {
        displayResults(current_shards[0]->path());
    }

    return true;
}
