#include "DBManager.h"
#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <sstream>
#include <unordered_map>

namespace fs = boost::filesystem;
using namespace std;

DBManager::DBManager(const string &db_path) : database_path(db_path) {
	fs::create_directory(database_path);
}

DBManager::~DBManager() = default;

string DBManager::getFilePath(const string &file_name) const {
	return database_path + "/" + file_name + ".txt";
}

unordered_map<string, int> DBManager::getTableAttributesMap(const string &table_name) {
    string mappingFilePath = getFilePath(table_name + "_mapping");

    if (!fs::exists(mappingFilePath)) {
        cerr << "Attribute mapping file does not exist for table: " << table_name << endl;
        return {};
    }

    ifstream mapping_file(mappingFilePath);
    if (!mapping_file.is_open()) {
        cerr << "Failed to open attribute mapping file for table: " << table_name << endl;
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


bool DBManager::createTable(const string &table_name, const vector<string> &attributes) {
    string table_file = getFilePath(table_name);
    if (fs::exists(table_file)) {
        cerr << "Table already exists: " << table_name << endl;
        return false;
    }

    string attr_mapping = getFilePath(table_name + "_mapping");
    if (fs::exists(attr_mapping)) {
        cerr << "Attribute mapping for table already exists: " << table_name << endl;
        return false;
    }

    ofstream table_file_stream(table_file);
    if (!table_file_stream.is_open()) {
        cerr << "Failed to create table: " << table_name << endl;
        return false;
    }

    ofstream mapping_file(attr_mapping);
    if (!mapping_file.is_open()) {
        cerr << "Failed to create attribute mapping for table: " << table_name << endl;
        return false;
    }

    for (int i = 0; i < attributes.size(); ++i) {
        mapping_file << attributes[i] << ", " << i << '\n';
    }

    table_file_stream.close();
    mapping_file.close();

    return true;
}


bool DBManager::deleteTable(const string &table_name) {
	string table_file = getFilePath(table_name);
	if (!fs::exists(table_file)) {
		cerr << "Table does not exist: " << table_name << endl;
		return false;
	}

	return fs::remove(table_file);
}

bool DBManager::insertRecord(const string &table_name,
                             const unordered_map<string, string> &attrMap) {
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
        cerr << "Failed to retrieve attribute mapping for table: " << table_name << endl;
        return false;
    }

    vector<string> values(tableAttrMap.size(), "");

    for (const auto& [attr, val] : attrMap) {
        if (tableAttrMap.find(attr) != tableAttrMap.end()) {
            values[tableAttrMap[attr]] = val;
        } else {
            cerr << "Unknown attribute: " << attr << " for table: " << table_name << endl;
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

vector<vector<string>> DBManager::readAllRecords(const string &table_name) {
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

		while (getline(stream, field, ',')) { record.push_back(field); }
		records.push_back(record);
	}

	file.close();
	return records;
}

bool DBManager::updateRecord(const string &table_name, int record_id,
                             const vector<string> &updated_record) {
	string table_file = getFilePath(table_name);
	if (!fs::exists(table_file)) {
		cerr << "Table does not exist: " << table_name << endl;
		return false;
	}

	vector<vector<string>> records = readAllRecords(table_name);
	if (record_id < 0 || record_id >= (int)records.size()) {
		cerr << "Record ID out of bounds: " << record_id << endl;
		return false;
	}

	records[record_id] = updated_record;

	ofstream file(table_file, ios::trunc);
	if (!file.is_open()) {
		cerr << "Failed to update record in table: " << table_name << endl;
		return false;
	}

	for (const auto &record : records) {
		for (size_t i = 0; i < record.size(); ++i) {
			file << record[i];
			if (i < record.size() - 1) file << ",";
		}
		file << "\n";
	}

	file.close();
	return true;
}

bool DBManager::deleteRecord(const string &table_name, int record_id) {
	string table_file = getFilePath(table_name);
	if (!fs::exists(table_file)) {
		cerr << "Table does not exist: " << table_name << endl;
		return false;
	}

	vector<vector<string>> records = readAllRecords(table_name);
	if (record_id < 0 || record_id >= (int)records.size()) {
		cerr << "Record ID out of bounds: " << record_id << endl;
		return false;
	}

	records.erase(records.begin() + record_id);

	ofstream file(table_file, ios::trunc);
	if (!file.is_open()) {
		cerr << "Failed to delete record from table: " << table_name << endl;
		return false;
	}

	for (const auto &record : records) {
		for (size_t i = 0; i < record.size(); ++i) {
			file << record[i];
			if (i < record.size() - 1) file << ",";
		}
		file << "\n";
	}

	file.close();
	return true;
}
