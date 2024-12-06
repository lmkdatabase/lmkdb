#include "DBManager.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace fs = std::filesystem;
using namespace std;

DBManager::DBManager(string dbPath) : database_path(std::move(dbPath)) {
    fs::create_directory(database_path);
    loadTables(database_path);
}

DBManager::~DBManager() = default;

string DBManager::NewTablePath(const string& table_name) const {
    return database_path + table_name;
}

void DBManager::loadTables(const string& directory_path) {
    for (const auto& table : fs::directory_iterator(directory_path)) {
        if (fs::is_directory(table)) {
            string table_name = table.path().filename();
            tables.emplace(table_name, make_shared<Table>(table_name));
        }
    }
}

shared_ptr<Table> DBManager::findTable(const string& table_name) {
    auto it = tables.find(table_name);
    if (it != tables.end()) {
        return it->second;
    }
    return nullptr;
}

bool DBManager::createTable(const std::string& table_name,
                            const std::vector<std::string>& attributes) {
    if (tables.find(table_name) != tables.end()) {
        cerr << "Table already exists: " << table_name << endl;
        return false;
    }

    string table_path = NewTablePath(table_name);
    cout << "New table path " << table_path << endl;

    fs::create_directory(table_path);
    ofstream metadata(table_path + "/metadata.txt");
    ofstream init_shard(table_path + "/shard_0.csv");

    int idx = 0;
    for (const auto& attr : attributes) {
        metadata << attr << "," << idx++ << "\n";
    }

    tables.emplace(table_name, make_shared<Table>(table_name));
    return true;
}

bool DBManager::insertRecord(const string& table_name,
                             const unordered_map<string, string>& record) {
    if (auto table = findTable(table_name)) {
        return table->insert(record);
    }
    return false;
}

void DBManager::readTable(const string& table_name,
                          const vector<int>& line_numbers) {
    if (auto table = findTable(table_name)) {
        table->read(line_numbers);
    } else {
        cerr << "Table does not exist: " << table_name << endl;
    }
}

bool DBManager::updateRecord(const string& table_name, size_t id,
                             const unordered_map<string, string>& attrMap) {
    if (auto table = findTable(table_name)) {
        return table->update(id, attrMap);
    }
    return false;
}

bool DBManager::deleteByIndex(const string& table_name, size_t id) {
    if (auto table = findTable(table_name)) {
        return table->deleteByIndex(id);
    }
    return false;
}

bool DBManager::deleteByAttributes(
    const string& table_name, const unordered_map<string, string>& attrMap) {
    if (auto table = findTable(table_name)) {
        return table->deleteByAttributes(attrMap);
    }
    return false;
}

bool DBManager::deleteTable(const string& table_name) {
    auto table = findTable(table_name);
    string path = table->tablePath();

    if (!fs::exists(path)) {
        cerr << "Table does not exist: " << table_name << endl;
        return false;
    }

    fs::remove_all(path);
    tables.erase(table_name);

    cout << "Table deleted successfully: " << table_name << endl;

    return true;
}

bool DBManager::joinTables(const vector<string>& tables,
                           unordered_map<string, string>& attrMap) {
    try {
        if (tables.size() < 2) {
            cerr << "Error: At least two tables are required for a join."
                 << endl;
            return false;
        }

        auto current_table = make_shared<Table>(tables[0]);
        string join_attr = attrMap[tables[0]];

        for (size_t i = 1; i < tables.size(); ++i) {
            auto next_table = make_shared<Table>(tables[i]);

            auto result_future = current_table->join(
                *next_table, join_attr, attrMap[next_table->getName()]);

            current_table = result_future.get();
        }

        current_table->read({});
        return true;

    } catch (const exception& e) {
        cerr << "Error in join operation: " << e.what() << endl;
        return false;
    }
}
