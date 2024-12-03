#include "DBManager.h"
#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fs = boost::filesystem;
using namespace std;

DBManager::DBManager(string dbPath) : database_path(std::move(dbPath)) {
    fs::create_directory(database_path);
}

DBManager::~DBManager() = default;

string DBManager::getTablePath(const string& table_name) const {
    return database_path + "/" + table_name;
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

void DBManager::readTable(const string& table_name) {
    if (!fs::exists(getTablePath(table_name))) {
        cerr << "Table does not exist: " << table_name << endl;
        return;
    }

    vector<string> shards = getShardPaths(table_name);

    if (shards.empty()) {
        cerr << "Table is empty: " << table_name << endl;
        return;
    }

    for (const auto& shard : shards) {
        ifstream file(shard);
        string line;

        while (getline(file, line)) {
            istringstream ss(line);
            string field;
            bool first = true;

            while (getline(ss, field, ',')) {
                if (!first) cout << ",";
                cout << field;
                first = false;
            }
            cout << endl;
        }
    }
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

bool DBManager::updateRecord(const string& table_name, size_t id,
                             const unordered_map<string, string>& attrMap) {
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
    /*             combined_row.insert(combined_row.end(), right_row.begin(), */
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

bool DBManager::joinTables(const vector<string>& tables,
                           unordered_map<string, string>& attrMap) {
    /* unordered_map<string, vector<vector<string>>> record_map; */
    /* unordered_map<string, unordered_map<string, int>> attr_maps; */
    /**/
    /* for (const auto& [table, attr] : attrMap) { */
    /*     string table_path = getFilePath(table); */
    /**/
    /*     if (!fs::exists(table_path)) { */
    /*         cerr << "Table does not exist: " << table << endl; */
    /*         return false; */
    /*     } */
    /**/
    /*     record_map[table] = readAllRecords(table); */
    /*     attr_maps[table] = getMetadata(table); */
    /* } */
    /**/
    /* size_t idx = 0; */
    /* string join_table = tables[idx]; */
    /**/
    /* while (idx < tables.size() - 1) { */
    /*     const string& right_table = tables[idx + 1]; */
    /**/
    /*     vector<vector<string>> join_records = record_map[join_table]; */
    /*     vector<vector<string>> right_records = record_map[right_table]; */
    /**/
    /*     auto& join_attr_map = attr_maps[join_table]; */
    /*     auto& right_attr_map = attr_maps[right_table]; */
    /**/
    /*     int join_index = join_attr_map[attrMap[join_table]]; */
    /*     int right_index = right_attr_map[attrMap[right_table]]; */
    /**/
    /*     // Create new attribute map for joined result */
    /*     unordered_map<string, int> new_attr_map = createJoinAttributeMap( */
    /*         join_attr_map, right_attr_map, join_table, right_table); */
    /**/
    /*     vector<vector<string>> result = */
    /*         joinRecords(join_records, right_records, join_index,
     * right_index); */
    /**/
    /*     // Update values for next iteration */
    /*     string joined_table_name = join_table + "_" + right_table; */
    /*     record_map[joined_table_name] = result; */
    /*     attr_maps[joined_table_name] = new_attr_map; */
    /**/
    /*     idx++; */
    /*     join_table = joined_table_name; */
    /* } */
    /**/
    /* printRecords(record_map[join_table]); */

    return true;
}
