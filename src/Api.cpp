#include "Api.h"
#include <iostream>
#include <memory>
#include <unordered_map>

using namespace std;

DatabaseAPI::DatabaseAPI(const string &dbPath)
    : dbManager(make_unique<DBManager>(dbPath)) {}

DatabaseAPI::~DatabaseAPI() = default;

bool DatabaseAPI::validateInteger(const string &input) {
    try {
        stoi(input);
        return true;
    } catch (const std::invalid_argument &) {
        return false;
    } catch (const std::out_of_range &) {
        return false;
    }
}

void DatabaseAPI::createOp(const string &tableName,
                           const vector<string> &attributes) {
    for (const auto &attr : attributes) {
        if (attr == "id") {
            cout << " id attribute name not allowed" << endl;
            return;
        }
    }

    if (dbManager->createTable(tableName, attributes)) {
        cout << "Table created: " << tableName << endl;
    } else {
        cout << "Failed to create table: " << tableName << endl;
    }
}

void DatabaseAPI::deleteOp(const string &tableName,
                           const vector<string> &tokens) {
    if (tokens.empty()) {
        if (dbManager->deleteTable(tableName)) {
            cout << "Table " << tableName << " deleted successfully." << endl;
        } else {
            cerr << "Failed to delete table: " << tableName << endl;
        }
        return;
    }
    // Case 1: delete table_name id:2 attr1 attr2 ...
    if (tokens[0].starts_with("id:")) {
        size_t pos = tokens[0].find(':');
        if (pos == string::npos || tokens[0].substr(pos + 1).empty()) {
            cerr << "Error: Invalid format." << endl;
            return;
        }

        string idValue = tokens[0].substr(pos + 1);
        if (!validateInteger(idValue)) {
            cerr << "Error: id must be a valid integer." << endl;
            return;
        }

        vector<string> attributes(tokens.begin() + 1, tokens.end());
        dbManager->deleteByIndex(tableName, stoi(idValue), attributes);
        return;
    }

    // Case 2: delete table_name id:2
    if (tokens.size() == 1 && tokens[0].starts_with("id:")) {
        size_t pos = tokens[0].find(':');
        if (pos == string::npos || tokens[0].substr(pos + 1).empty()) {
            cerr << "Error: Invalid format." << endl;
            return;
        }

        string idValue = tokens[0].substr(pos + 1);
        if (!validateInteger(idValue)) {
            cerr << "Error: id must be a valid integer." << endl;
            return;
        }

        dbManager->deleteByIndex(tableName, stoi(idValue), {});
        return;
    }

    // Case 3: delete table_name attr1:val1 attr2:val2
    unordered_map<string, string> attrMap;
    for (const auto &token : tokens) {
        size_t pos = token.find(':');
        if (pos == string::npos || token.substr(0, pos).empty() ||
            token.substr(pos + 1).empty()) {
            cerr << "Error: Invalid attribute-value pair format: " << token
                 << endl;
            return;
        }

        string key = token.substr(0, pos);
        string value = token.substr(pos + 1);
        attrMap[key] = value;
    }

    dbManager->deleteByAttributes(tableName, attrMap);
}

void DatabaseAPI::insertOp(const string &tableName,
                           const vector<string> &tokens) {
    unordered_map<string, string> mp;

    for (const auto &token : tokens) {
        size_t pos = token.find(':');

        if (pos != string::npos) {
            string key = token.substr(0, pos);
            string value = token.substr(pos + 1);

            mp[key] = value;
        } else {
            cerr << "Invalid token: " << token << endl;
            return;
        }
    }

    if (dbManager->insertRecord(tableName, mp)) {
        cout << "Inserted into table: " << tableName << endl;
    } else {
        cout << "Failed to insert into table: " << tableName << endl;
    }
}

void DatabaseAPI::readOp(const string &tableName,
                         const vector<string> &tokens) {
    dbManager->readTable(tableName);

    /* if (records.empty()) { */
    /*     cout << "No records found or table does not exist: " << tableName */
    /*          << endl; */
    /*     return; */
    /* } */
    /**/
    /* // Case 1: If no tokens are provided, return all records */
    /* if (tokens.empty()) { */
    /*     for (const auto &record : records) { */
    /*         for (size_t i = 0; i < record.size(); ++i) { */
    /*             cout << record[i]; */
    /*             if (i < record.size() - 1) cout << ", "; */
    /*         } */
    /*         cout << endl; */
    /*     } */
    /*     return; */
    /* } */
    /**/
    /* // Case 2: If one token is provided, expect "id:<number>" format */
    /* if (tokens.size() == 1) { */
    /*     const string &token = tokens[0]; */
    /**/
    /*     if (token.starts_with("id:")) { */
    /*         size_t pos = token.find(':'); */
    /*         if (pos == string::npos || token.substr(pos + 1).empty()) { */
    /*             cerr << "Error: Invalid index format." << endl; */
    /*             return; */
    /*         } */
    /**/
    /*         string idValue = token.substr(pos + 1); */
    /*         if (!validateInteger(idValue)) { */
    /*             cerr << "Error: id must be a valid integer." << endl; */
    /*             return; */
    /*         } */
    /**/
    /*         int id = stoi(idValue); */
    /*         if (id < 0 || id >= static_cast<int>(records.size())) { */
    /*             cerr << "Error: Index out of bounds for table: " << tableName
     */
    /*                  << endl; */
    /*             return; */
    /*         } */
    /**/
    /*         const auto &record = records[id]; */
    /*         for (size_t i = 0; i < record.size(); ++i) { */
    /*             cout << record[i]; */
    /*             if (i < record.size() - 1) cout << ", "; */
    /*         } */
    /*         cout << endl; */
    /*         return; */
    /*     } */
    /*     cerr << "Error: Expected id: <number> format." << endl; */
    /*     return; */
    /* } */
    /**/
    /* cerr << "Error: Too many arguments provided for read operation." << endl;
     */
}

void DatabaseAPI::updateOp(const string &tableName, size_t recordId,
                           const vector<string> &updatedRecord) {
    unordered_map<string, string> mp;

    for (const auto &token : updatedRecord) {
        size_t pos = token.find(':');

        if (pos != string::npos) {
            string key = token.substr(0, pos);
            string value = token.substr(pos + 1);

            mp[key] = value;
        } else {
            cerr << "Invalid token: " << token << endl;
            return;
        }
    }

    if (dbManager->updateRecord(tableName, recordId, mp)) {
        cout << "Record updated in table: " << tableName << endl;
    } else {
        cout << "Failed to update record in table: " << tableName << endl;
    }
}

void DatabaseAPI::joinOp(const vector<string> &query) {
    unordered_map<string, string> attrMap;
    vector<string> tables;

    for (const auto &token : query) {
        size_t pos = token.find('.');

        if (pos != string::npos) {
            string key = token.substr(0, pos);
            string value = token.substr(pos + 1);

            attrMap[key] = value;
            tables.push_back(key);
        } else {
            cerr << "Invalid token: " << token << endl;
            return;
        }
    }

    dbManager->joinTables(tables, attrMap);
}
