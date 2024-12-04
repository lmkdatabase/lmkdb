#include "DBManager.h"
#include <algorithm>
#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <fstream>
#include <iostream>
#include <iterator>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include "worker.h"

namespace fs = boost::filesystem;
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
    size_t current_idx = 0;

    while (getline(in_file, line)) {
        if (current_idx != target_idx) {
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
        current_idx++;
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

bool DBManager::deleteByIndex(const string& table_name, size_t id,
                              const vector<string>& attributes) {
    /* string table_file = getFilePath(table_name); */
    /* if (!fs::exists(table_file)) { */
    /*     cerr << "Table does not exist: " << table_name << endl; */
    /*     return false; */
    /* } */
    /**/
    /* vector<vector<string>> records = readAllRecords(table_name); */
    /* if (id < 0 || id >= records.size()) { */
    /*     cerr << "Index out of bounds: " << id << endl; */
    /*     return false; */
    /* } */
    /**/
    /* unordered_map<string, int> tableAttrMap = getMetadata(table_name); */
    /* if (tableAttrMap.empty()) { */
    /*     cerr << "Failed to retrieve attribute mapping for table: " <<
     * table_name */
    /*          << endl; */
    /*     return false; */
    /* } */
    /**/
    /* if (attributes.empty()) { */
    /*     records.erase(records.begin() + id); */
    /* } else { */
    /*     for (const auto& attr : attributes) { */
    /*         if (tableAttrMap.find(attr) != tableAttrMap.end()) { */
    /*             int attrIndex = tableAttrMap[attr]; */
    /*             records[id][attrIndex] = "NULL"; */
    /*         } else { */
    /*             cerr << "Unknown attribute: " << attr << endl; */
    /*             return false; */
    /*         } */
    /*     } */
    /* } */
    /**/
    /* ofstream file(table_file, ios::trunc); */
    /* if (!file.is_open()) { */
    /*     cerr << "Failed to write to table: " << table_name << endl; */
    /*     return false; */
    /* } */
    /**/
    /* for (const auto& record : records) { */
    /*     for (size_t i = 0; i < record.size(); ++i) { */
    /*         file << record[i]; */
    /*         if (i < record.size() - 1) file << ","; */
    /*     } */
    /*     file << "\n"; */
    /* } */
    /**/
    /* file.close(); */
    return true;
}

bool DBManager::deleteByAttributes(
    const string& table_name, const unordered_map<string, string>& attrMap) {
    /* string table_file = getFilePath(table_name); */
    /* if (!fs::exists(table_file)) { */
    /*     cerr << "Table does not exist: " << table_name << endl; */
    /*     return false; */
    /* } */
    /**/
    /* vector<vector<string>> records = readAllRecords(table_name); */
    /* unordered_map<string, int> tableAttrMap = getMetadata(table_name); */
    /* if (tableAttrMap.empty()) { */
    /*     cerr << "Failed to retrieve attribute mapping for table: " <<
     * table_name */
    /*          << endl; */
    /*     return false; */
    /* } */
    /**/
    /* vector<vector<string>> updatedRecords; */
    /* for (const auto& record : records) { */
    /*     bool match = true; */
    /**/
    /*     for (const auto& [key, value] : attrMap) { */
    /*         if (tableAttrMap.find(key) == tableAttrMap.end()) { */
    /*             cerr << "Unknown attribute: " << key << endl; */
    /*             return false; */
    /*         } */
    /**/
    /*         int index = tableAttrMap[key]; */
    /*         if (record[index] != value) { */
    /*             match = false; */
    /*             break; */
    /*         } */
    /*     } */
    /**/
    /*     if (!match) { */
    /*         updatedRecords.push_back(record); */
    /*     } */
    /* } */
    /**/
    /* ofstream file(table_file, ios::trunc); */
    /* if (!file.is_open()) { */
    /*     cerr << "Failed to write to table: " << table_name << endl; */
    /*     return false; */
    /* } */
    /**/
    /* for (const auto& record : updatedRecords) { */
    /*     for (size_t i = 0; i < record.size(); ++i) { */
    /*         file << record[i]; */
    /*         if (i < record.size() - 1) file << ","; */
    /*     } */
    /*     file << "\n"; */
    /* } */
    /**/
    /* file.close(); */
    return true;
}

bool DBManager::deleteTable(const string& table_name) {
    /* string table_file = getFilePath(table_name); */
    /* string attr_mapping = getFilePath(table_name + "_mapping"); */
    /**/
    /* if (!fs::exists(table_file)) { */
    /*     cerr << "Table does not exist: " << table_name << endl; */
    /*     return false; */
    /* } */
    /**/
    /* if (!fs::remove(table_file)) { */
    /*     cerr << "Failed to delete table file: " << table_file << endl; */
    /*     return false; */
    /* } */
    /**/
    /* if (fs::exists(attr_mapping)) { */
    /*     if (!fs::remove(attr_mapping)) { */
    /*         cerr << "Failed to delete attribute mapping file: " <<
     * attr_mapping */
    /*              << endl; */
    /*         return false; */
    /*     } */
    /* } */
    /**/
    /* cout << "Table and mapping deleted successfully: " << table_name << endl;
     */
    return true;
}

/* vector<vector<string>> DBManager::readTable(const string& table_name) { */
/* string table_file = getFilePath(table_name); */
/* vector<vector<string>> records = {}; */
/**/
/* if (!fs::exists(table_file)) { */
/*     cerr << "Table does not exist: " << table_name << endl; */
/*     return records; */
/* } */
/**/
/* ifstream file(table_file); */
/* if (!file.is_open()) { */
/*     cerr << "Failed to read records from table: " << table_name << endl;
 */
/*     return records; */
/* } */
/**/
/* string line; */
/* while (getline(file, line)) { */
/*     istringstream stream(line); */
/*     string field; */
/*     vector<string> record; */
/**/
/*     while (getline(stream, field, ',')) { */
/*         record.push_back(field); */
/*     } */
/*     records.push_back(record); */
/* } */
/**/
/* file.close(); */
/* return records; */
/* } */

/* bool DBManager::updateRecord(const string& table_name, size_t id, */
/*                              const unordered_map<string, string>& attrMap) {
 */
/* string table_file = getFilePath(table_name); */
/* if (!fs::exists(table_file)) { */
/*     cerr << "Table does not exist: " << table_name << endl; */
/*     return false; */
/* } */
/**/
/* vector<vector<string>> records = readAllRecords(table_name); */
/* if (id < 0 || id >= records.size()) { */
/*     cerr << "Record ID out of bounds: " << id << endl; */
/*     return false; */
/* } */
/**/
/* unordered_map<string, int> tableAttrMap = getMetadata(table_name); */
/* if (tableAttrMap.empty()) { */
/*     cerr << "Failed to retrieve attribute mapping for table: " <<
 * table_name */
/*          << endl; */
/*     return false; */
/* } */
/**/
/* vector<string> updatedRecord = records[id]; */
/**/
/* for (const auto& [attr, val] : attrMap) { */
/*     if (tableAttrMap.find(attr) != tableAttrMap.end()) { */
/*         updatedRecord[tableAttrMap[attr]] = val; */
/*     } else { */
/*         cerr << "Unknown attribute: " << attr */
/*              << " for table: " << table_name << endl; */
/*         return false; */
/*     } */
/* } */
/**/
/* records[id] = updatedRecord; */
/**/
/* ofstream file(table_file, ios::trunc); */
/* if (!file.is_open()) { */
/*     cerr << "Failed to update record in table: " << table_name << endl;
 */
/*     return false; */
/* } */
/**/
/* for (const auto& record : records) { */
/*     for (size_t i = 0; i < record.size(); ++i) { */
/*         file << record[i]; */
/*         if (i < record.size() - 1) file << ","; */
/*     } */
/*     file << "\n"; */
/* } */
/**/
/* file.close(); */
/*     return true; */
/* } */

bool DBManager::joinTables(const vector<string>& tables,
                           unordered_map<string, string>& attrMap) {
    constexpr size_t MAX_THREADS = 8;

    if (tables.size() < 2) {
        cerr << "Error: At least two tables are required for a join." << endl;
        return false;
    }

    string temp_result = getTablePath(tables[0]) + "/temp_join_0_" +
                         to_string(time(nullptr)) + ".csv";
    string current_table = tables[0];

    for (size_t i = 1; i < tables.size(); ++i) {
        const string& next_table = tables[i];
        string new_temp = getTablePath(next_table) + "/temp_" + to_string(i) +
                          "_" + current_table + "_" + next_table + "_" +
                          to_string(rand()) + ".csv";

        if (!fs::exists(getTablePath(next_table))) {
            fs::create_directories(getTablePath(next_table));
        }

        // Get metadata for both tables
        auto metadata2 = getMetadata(next_table);
        if (metadata2.find(attrMap[next_table]) == metadata2.end()) {
            cerr << "Error: Invalid attribute mapping for table " << next_table
                 << endl;
            return false;
        }

        // Calculate join attribute positions
        int attr_pos1;
        int attr_pos2 = metadata2[attrMap[next_table]];

        if (i == 1) {
            // First join: use original metadata
            auto metadata1 = getMetadata(current_table);
            if (metadata1.find(attrMap[current_table]) == metadata1.end()) {
                cerr << "Error: Invalid attribute mapping for table "
                     << current_table << endl;
                return false;
            }
            attr_pos1 = metadata1[attrMap[current_table]];
            current_num_columns = metadata1.size();
        } else {
            // Subsequent joins: use position from first table in intermediate
            // results
            attr_pos1 = 0;  // Always use first occurrence of join attribute
            // Count columns in intermediate result by reading first line
            ifstream temp_read(temp_result);
            string first_line;
            if (getline(temp_read, first_line)) {
                current_num_columns =
                    count(first_line.begin(), first_line.end(), ',') + 1;
            }
        }

        vector<string> shards1 = (i == 1) ? getShardPaths(current_table)
                                          : vector<string>{temp_result};
        vector<string> shards2 = getShardPaths(next_table);

        vector<thread> workers;
        vector<string> thread_temp_files;

        for (size_t batch_start = 0; batch_start < shards1.size();
             batch_start += MAX_THREADS) {
            size_t batch_end = min(batch_start + MAX_THREADS, shards1.size());
            vector<string> current_batch(shards1.begin() + batch_start,
                                         shards1.begin() + batch_end);

            for (const auto& shard : current_batch) {
                string thread_temp = getTablePath(next_table) +
                                     "/thread_temp_" + to_string(batch_start) +
                                     "_" + to_string(time(nullptr)) + ".csv";
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

        // Merge thread temp files into new_temp
        ofstream final_out(new_temp, ios::trunc);
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

        // Cleanup previous temp result if it exists
        if (i > 1 && fs::exists(temp_result)) {
            fs::remove(temp_result);
        }

        temp_result = new_temp;
        current_table = next_table;
    }

    if (!fs::exists(temp_result)) {
        cerr << "Error: Final result file not found: " << temp_result << endl;
        return false;
    }

    ifstream final_result(temp_result);
    if (!final_result.is_open()) {
        cerr << "Error: Cannot open final result file: " << temp_result << endl;
        return false;
    }

    cout << "Reading final results from: " << temp_result << endl;
    string line;
    while (getline(final_result, line)) {
        cout << line << endl;
    }

    fs::remove(temp_result);
    return true;
}

vector<vector<string>> DBManager::joinRecords(
    const vector<vector<string>>& left_records,
    const vector<vector<string>>& right_records, int left_index,
    int right_index) {
    vector<vector<string>> result;

    /* // Join is performed here */
    /* for (const auto& left_row : left_records) { */
    /*     for (const auto& right_row : right_records) { */
    /*         if (left_row[left_index] == right_row[right_index]) { */
    /*             vector<string> combined_row = left_row; */
    /*             combined_row.insert(combined_row.end(),
     * right_row.begin(), */
    /*                                 right_row.end()); */
    /*             result.push_back(combined_row); */
    /*         } */
    /*     } */
    /* } */

    return result;
}

unordered_map<string, int> DBManager::createJoinAttributeMap(
    const unordered_map<string, int>& left_attributes,
    const unordered_map<string, int>& right_attributes,
    const string& left_table_name, const string& right_table_name) {
    unordered_map<string, int> new_attr_map = {};
    /* int col_idx = 0; */
    /**/
    /* for (const auto& [attr, idx] : left_attributes) { */
    /*     new_attr_map[left_table_name + "." + attr] = col_idx++; */
    /* } */
    /**/
    /* for (const auto& [attr, idx] : right_attributes) { */
    /*     new_attr_map[right_table_name + "." + attr] = col_idx++; */
    /* } */
    return new_attr_map;
}
