#include "DBManager.h"
#include <boost/filesystem.hpp>
#include <boost/filesystem/operations.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <utility>

namespace fs = boost::filesystem;
using namespace std;

DBManager::DBManager(string dbPath) : database_path(std::move(dbPath)) {
    fs::create_directory(database_path);
}

DBManager::~DBManager() = default;

string DBManager::getFilePath(const string& file_name) const {
    return database_path + "/" + file_name + ".txt";
}

unordered_map<string, int> DBManager::getTableAttributesMap(
    const string& table_name) {
    string mappingFilePath = getFilePath(table_name + "_mapping");

    if (!fs::exists(mappingFilePath)) {
        cerr << "Attribute mapping file does not exist for table: "
             << table_name << endl;
        return {};
    }

    ifstream mapping_file(mappingFilePath);
    if (!mapping_file.is_open()) {
        cerr << "Failed to open attribute mapping file for table: "
             << table_name << endl;
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

    mapping_file.close();
    return attributes_map;
}

bool DBManager::createTable(const string& table_name,
                            const vector<string>& attributes) {
    string table_file = getFilePath(table_name);
    if (fs::exists(table_file)) {
        cerr << "Table already exists: " << table_name << endl;
        return false;
    }

    string attr_mapping = getFilePath(table_name + "_mapping");
    if (fs::exists(attr_mapping)) {
        cerr << "Attribute mapping for table already exists: " << table_name
             << endl;
        return false;
    }

    ofstream table_file_stream(table_file);
    if (!table_file_stream.is_open()) {
        cerr << "Failed to create table: " << table_name << endl;
        return false;
    }

    ofstream mapping_file(attr_mapping);
    if (!mapping_file.is_open()) {
        cerr << "Failed to create attribute mapping for table: " << table_name
             << endl;
        return false;
    }

    for (size_t i = 0; i < attributes.size(); ++i) {
        mapping_file << attributes[i] << ", " << i << '\n';
    }

    table_file_stream.close();
    mapping_file.close();

    return true;
}

bool DBManager::deleteByIndex(const std::string& table_name, const int& id,
                              const std::vector<std::string>& attributes) {
    std::string table_file = getFilePath(table_name);
    if (!fs::exists(table_file)) {
        std::cerr << "Table does not exist: " << table_name << std::endl;
        return false;
    }

    std::vector<std::vector<std::string>> records = readAllRecords(table_name);
    if (id < 0 || id >= static_cast<int>(records.size())) {
        std::cerr << "Index out of bounds: " << id << std::endl;
        return false;
    }

    std::unordered_map<std::string, int> tableAttrMap =
        getTableAttributesMap(table_name);
    if (tableAttrMap.empty()) {
        std::cerr << "Failed to retrieve attribute mapping for table: "
                  << table_name << std::endl;
        return false;
    }

    if (attributes.empty()) {
        records.erase(records.begin() + id);
    } else {
        for (const auto& attr : attributes) {
            if (tableAttrMap.find(attr) != tableAttrMap.end()) {
                int attrIndex = tableAttrMap[attr];
                records[id][attrIndex] = "NULL";
            } else {
                std::cerr << "Unknown attribute: " << attr << std::endl;
                return false;
            }
        }
    }

    std::ofstream file(table_file, std::ios::trunc);
    if (!file.is_open()) {
        std::cerr << "Failed to write to table: " << table_name << std::endl;
        return false;
    }

    for (const auto& record : records) {
        for (size_t i = 0; i < record.size(); ++i) {
            file << record[i];
            if (i < record.size() - 1) file << ",";
        }
        file << "\n";
    }

    file.close();
    return true;
}

bool DBManager::deleteByAttributes(
    const std::string& table_name,
    const std::unordered_map<std::string, std::string>& attrMap) {
    std::string table_file = getFilePath(table_name);
    if (!fs::exists(table_file)) {
        std::cerr << "Table does not exist: " << table_name << std::endl;
        return false;
    }

    std::vector<std::vector<std::string>> records = readAllRecords(table_name);
    std::unordered_map<std::string, int> tableAttrMap =
        getTableAttributesMap(table_name);
    if (tableAttrMap.empty()) {
        std::cerr << "Failed to retrieve attribute mapping for table: "
                  << table_name << std::endl;
        return false;
    }

    std::vector<std::vector<std::string>> updatedRecords;
    for (const auto& record : records) {
        bool match = true;

        for (const auto& [key, value] : attrMap) {
            if (tableAttrMap.find(key) == tableAttrMap.end()) {
                std::cerr << "Unknown attribute: " << key << std::endl;
                return false;
            }

            int index = tableAttrMap[key];
            if (record[index] != value) {
                match = false;
                break;
            }
        }

        if (!match) {
            updatedRecords.push_back(record);
        }
    }

    std::ofstream file(table_file, std::ios::trunc);
    if (!file.is_open()) {
        std::cerr << "Failed to write to table: " << table_name << std::endl;
        return false;
    }

    for (const auto& record : updatedRecords) {
        for (size_t i = 0; i < record.size(); ++i) {
            file << record[i];
            if (i < record.size() - 1) file << ",";
        }
        file << "\n";
    }

    file.close();
    return true;
}

bool DBManager::deleteTable(const std::string& table_name) {
    std::string table_file = getFilePath(table_name);
    std::string attr_mapping = getFilePath(table_name + "_mapping");

    if (!fs::exists(table_file)) {
        std::cerr << "Table does not exist: " << table_name << std::endl;
        return false;
    }

    if (!fs::remove(table_file)) {
        std::cerr << "Failed to delete table file: " << table_file << std::endl;
        return false;
    }

    if (fs::exists(attr_mapping)) {
        if (!fs::remove(attr_mapping)) {
            std::cerr << "Failed to delete attribute mapping file: "
                      << attr_mapping << std::endl;
            return false;
        }
    }

    std::cout << "Table and mapping deleted successfully: " << table_name
              << std::endl;
    return true;
}

bool DBManager::insertRecord(const string& table_name,
                             const unordered_map<string, string>& attrMap) {
    string table_file = getFilePath(table_name);
    if (!fs::exists(table_file)) {
        cerr << "Table does not exist: " << table_name << endl;
        return false;
    }

    ofstream file(table_file, ios::app);
    if (!file.is_open()) {
        cerr << "Failed to insert record into table: " << table_name << endl;
        return false;
    }

    unordered_map<string, int> tableAttrMap = getTableAttributesMap(table_name);
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

    string record = "";
    for (size_t i = 0; i < values.size(); ++i) {
        record += values[i];
        if (i < values.size() - 1) record += ",";
    }

    file << record << "\n";

    file.close();
    return true;
}

vector<vector<string>> DBManager::readAllRecords(const string& table_name) {
    string table_file = getFilePath(table_name);
    vector<vector<string>> records;

    if (!fs::exists(table_file)) {
        cerr << "Table does not exist: " << table_name << endl;
        return records;
    }

    ifstream file(table_file);
    if (!file.is_open()) {
        cerr << "Failed to read records from table: " << table_name << endl;
        return records;
    }

    string line;
    while (getline(file, line)) {
        istringstream stream(line);
        string field;
        vector<string> record;

        while (getline(stream, field, ',')) {
            record.push_back(field);
        }
        records.push_back(record);
    }

    file.close();
    return records;
}

bool DBManager::updateRecord(const string& table_name, const int& id,
                             const unordered_map<string, string>& attrMap) {
    string table_file = getFilePath(table_name);
    if (!fs::exists(table_file)) {
        cerr << "Table does not exist: " << table_name << endl;
        return false;
    }

    vector<vector<string>> records = readAllRecords(table_name);
    if (id < 0 || id >= (int)records.size()) {
        cerr << "Record ID out of bounds: " << id << endl;
        return false;
    }

    unordered_map<string, int> tableAttrMap = getTableAttributesMap(table_name);
    if (tableAttrMap.empty()) {
        cerr << "Failed to retrieve attribute mapping for table: " << table_name
             << endl;
        return false;
    }

    vector<string> updatedRecord = records[id];

    for (const auto& [attr, val] : attrMap) {
        if (tableAttrMap.find(attr) != tableAttrMap.end()) {
            updatedRecord[tableAttrMap[attr]] = val;
        } else {
            cerr << "Unknown attribute: " << attr
                 << " for table: " << table_name << endl;
            return false;
        }
    }

    records[id] = updatedRecord;

    ofstream file(table_file, ios::trunc);
    if (!file.is_open()) {
        cerr << "Failed to update record in table: " << table_name << endl;
        return false;
    }

    for (const auto& record : records) {
        for (size_t i = 0; i < record.size(); ++i) {
            file << record[i];
            if (i < record.size() - 1) file << ",";
        }
        file << "\n";
    }

    file.close();
    return true;
}

bool DBManager::joinTables(const vector<string>& tables,
                           unordered_map<string, string>& attrMap) {
    unordered_map<string, vector<vector<string>>> record_map;
    unordered_map<string, unordered_map<string, int>> attr_maps;

    for (const auto& [table, attr] : attrMap) {
        string table_path = getFilePath(table);

        if (!fs::exists(table_path)) {
            cerr << "Table does not exist: " << table << endl;
            return false;
        }

        record_map[table] = readAllRecords(table);
        attr_maps[table] = getTableAttributesMap(table);
    }

    size_t idx = 0;
    string join_table = tables[idx];

    while (idx < tables.size() - 1) {
        const string& right_table = tables[idx + 1];

        vector<vector<string>> join_records = record_map[join_table];
        vector<vector<string>> right_records = record_map[right_table];

        auto& join_attr_map = attr_maps[join_table];
        auto& right_attr_map = attr_maps[right_table];

        int join_index = join_attr_map[attrMap[join_table]];
        int right_index = right_attr_map[attrMap[right_table]];

        // Create new attribute map for joined result
        unordered_map<string, int> new_attr_map;
        int col_idx = 0;

        for (const auto& [attr, idx] : join_attr_map) {
            new_attr_map[join_table + "." + attr] = col_idx++;
        }

        for (const auto& [attr, idx] : right_attr_map) {
            new_attr_map[right_table + "." + attr] = col_idx++;
        }

        vector<vector<string>> result;

        // Join is performed here
        for (const auto& left_row : join_records) {
            for (const auto& right_row : right_records) {
                if (left_row[join_index] == right_row[right_index]) {
                    vector<string> combined_row = left_row;
                    combined_row.insert(combined_row.end(), right_row.begin(),
                                        right_row.end());
                    result.push_back(combined_row);
                }
            }
        }

        // Update values for next iteration
        string joined_table_name = join_table + "_" + right_table;
        record_map[joined_table_name] = result;
        attr_maps[joined_table_name] = new_attr_map;

        idx++;
        join_table = joined_table_name;
    }

    for (const auto& record : record_map[join_table]) {
        for (size_t i = 0; i < record.size(); ++i) {
            cout << record[i];
            if (i < record.size() - 1) cout << ", ";
        }
        cout << endl;
    }

    return true;
}
