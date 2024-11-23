#include "DBManager.h"
#include <boost/filesystem.hpp>
#include <fstream>
#include <sstream>
#include <iostream>

DBManager::DBManager(const std::string& db_path) : database_path(db_path) {
    boost::filesystem::create_directory(database_path);
}

DBManager::~DBManager() {}

std::string DBManager::getTableFilePath(const std::string& table_name) const {
    return database_path + "/" + table_name + ".txt";
}

bool DBManager::createTable(const std::string& table_name) {
    std::string table_file = getTableFilePath(table_name);
    if (boost::filesystem::exists(table_file)) {
        std::cerr << "Table already exists: " << table_name << std::endl;
        return false;
    }

    std::ofstream file(table_file);
    if (!file.is_open()) {
        std::cerr << "Failed to create table: " << table_name << std::endl;
        return false;
    }

    file.close();
    return true;
}

bool DBManager::deleteTable(const std::string& table_name) {
    std::string table_file = getTableFilePath(table_name);
    if (!boost::filesystem::exists(table_file)) {
        std::cerr << "Table does not exist: " << table_name << std::endl;
        return false;
    }

    return boost::filesystem::remove(table_file);
}

bool DBManager::insertRecord(const std::string& table_name, const std::vector<std::string>& record) {
    std::string table_file = getTableFilePath(table_name);
    if (!boost::filesystem::exists(table_file)) {
        std::cerr << "Table does not exist: " << table_name << std::endl;
        return false;
    }

    std::ofstream file(table_file, std::ios::app);
    if (!file.is_open()) {
        std::cerr << "Failed to insert record into table: " << table_name << std::endl;
        return false;
    }

    for (size_t i = 0; i < record.size(); ++i) {
        file << record[i];
        if (i < record.size() - 1) file << ",";
    }
    file << "\n";

    file.close();
    return true;
}

std::vector<std::vector<std::string>> DBManager::readAllRecords(const std::string& table_name) {
    std::string table_file = getTableFilePath(table_name);
    std::vector<std::vector<std::string>> records;

    if (!boost::filesystem::exists(table_file)) {
        std::cerr << "Table does not exist: " << table_name << std::endl;
        return records;
    }

    std::ifstream file(table_file);
    if (!file.is_open()) {
        std::cerr << "Failed to read records from table: " << table_name << std::endl;
        return records;
    }

    std::string line;
    while (std::getline(file, line)) {
        std::istringstream stream(line);
        std::string field;
        std::vector<std::string> record;

        while (std::getline(stream, field, ',')) {
            record.push_back(field);
        }
        records.push_back(record);
    }

    file.close();
    return records;
}

bool DBManager::updateRecord(const std::string& table_name, int record_id, const std::vector<std::string>& updated_record) {
    std::string table_file = getTableFilePath(table_name);
    if (!boost::filesystem::exists(table_file)) {
        std::cerr << "Table does not exist: " << table_name << std::endl;
        return false;
    }

    std::vector<std::vector<std::string>> records = readAllRecords(table_name);
    if (record_id < 0 || record_id >= (int)records.size()) {
        std::cerr << "Record ID out of bounds: " << record_id << std::endl;
        return false;
    }

    records[record_id] = updated_record;

    std::ofstream file(table_file, std::ios::trunc);
    if (!file.is_open()) {
        std::cerr << "Failed to update record in table: " << table_name << std::endl;
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

bool DBManager::deleteRecord(const std::string& table_name, int record_id) {
    std::string table_file = getTableFilePath(table_name);
    if (!boost::filesystem::exists(table_file)) {
        std::cerr << "Table does not exist: " << table_name << std::endl;
        return false;
    }

    std::vector<std::vector<std::string>> records = readAllRecords(table_name);
    if (record_id < 0 || record_id >= (int)records.size()) {
        std::cerr << "Record ID out of bounds: " << record_id << std::endl;
        return false;
    }

    records.erase(records.begin() + record_id);

    std::ofstream file(table_file, std::ios::trunc);
    if (!file.is_open()) {
        std::cerr << "Failed to delete record from table: " << table_name << std::endl;
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
