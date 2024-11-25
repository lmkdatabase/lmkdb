#include "Api.h"
#include <iostream>

using namespace std;

DatabaseAPI::DatabaseAPI() : dbManager(new DBManager("./database")) {}

DatabaseAPI::~DatabaseAPI() { delete dbManager; }

bool DatabaseAPI::validateInteger(const string &input) {
    try {
        stoi(input);
        return true;
    } catch (const std::invalid_argument&) {
        return false;
    } catch (const std::out_of_range&) {
        return false;
    }
}

void DatabaseAPI::createOp(const string &tableName, const vector<string> &attributes) {
    for (auto attr : attributes) {
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

void DatabaseAPI::deleteOp(const string &tableName, const vector<string> &tokens) {
    if (tokens.empty()) {
        if (dbManager->deleteTable(tableName)) {
            cout << "Table " << tableName << " deleted successfully." << endl;
        } else {
            cerr << "Failed to delete table: " << tableName << endl;
        }
        return;
    }
    // Case 1: delete table_name idx:2 attr1 attr2 ...
    if (tokens[0].find("idx:") == 0) {
        size_t pos = tokens[0].find(':');
        if (pos == string::npos || tokens[0].substr(pos + 1).empty()) {
            cerr << "Error: Invalid format." << endl;
            return;
        }

        string idxValue = tokens[0].substr(pos + 1);
        if (!validateInteger(idxValue)) {
            cerr << "Error: idx must be a valid integer." << endl;
            return;
        }

        vector<string> attributes(tokens.begin() + 1, tokens.end());
        dbManager->deleteByIndex(tableName, stoi(idxValue), attributes);
        return;
    }

    // Case 2: delete table_name idx:2
    if (tokens.size() == 1 && tokens[0].find("idx:") == 0) {
        size_t pos = tokens[0].find(':');
        if (pos == string::npos || tokens[0].substr(pos + 1).empty()) {
            cerr << "Error: Invalid format." << endl;
            return;
        }

        string idxValue = tokens[0].substr(pos + 1);
        if (!validateInteger(idxValue)) {
            cerr << "Error: idx must be a valid integer." << endl;
            return;
        }

        dbManager->deleteByIndex(tableName, stoi(idxValue), {});
        return;
    }

    // Case 3: delete table_name attr1:val1 attr2:val2
    unordered_map<string, string> keyValuePairs;
    for (const auto &token : tokens) {
        size_t pos = token.find(':');
        if (pos == string::npos || token.substr(0, pos).empty() || token.substr(pos + 1).empty()) {
            cerr << "Error: Invalid attribute-value pair format: " << token << endl;
            return;
        }

        string key = token.substr(0, pos);
        string value = token.substr(pos + 1);
        keyValuePairs[key] = value;
    }

    dbManager->deleteByAttributes(tableName, keyValuePairs);
}


void DatabaseAPI::insertOp(const string &tableName,
                                  const vector<string> &tokens) {
	unordered_map<string, string> mp;

    for (const auto& token : tokens) {
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

void DatabaseAPI::readOp(const string &tableName) {
	auto records = dbManager->readAllRecords(tableName);
	if (records.empty()) {
		cout << "No records found or table does not exist: " << tableName
		     << endl;
		return;
	}

	for (const auto &record : records) {
		for (const auto &field : record) { cout << field << " "; }
		cout << endl;
	}
}

void DatabaseAPI::updateOp(const string &tableName, int recordId,
                              const vector<string> &updatedRecord) {
	unordered_map<string, string> mp;

    for (const auto& token : updatedRecord) {
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

