#include "DBManager.h"
#include <boost/filesystem.hpp>
#include <fstream>
#include <iostream>
#include <sstream>

namespace fs = boost::filesystem;
using namespace std;

DBManager::DBManager(const string &db_path) : database_path(db_path) {
	fs::create_directory(database_path);
}

DBManager::~DBManager() = default;

string DBManager::getTableFilePath(const string &table_name) const {
	return database_path + "/" + table_name + ".txt";
}

bool DBManager::createTable(const string &table_name) {
	string table_file = getTableFilePath(table_name);
	if (fs::exists(table_file)) {
		cerr << "Table already exists: " << table_name << endl;
		return false;
	}

	ofstream file(table_file);
	if (!file.is_open()) {
		cerr << "Failed to create table: " << table_name << endl;
		return false;
	}

	file.close();
	return true;
}

bool DBManager::deleteTable(const string &table_name) {
	string table_file = getTableFilePath(table_name);
	if (!fs::exists(table_file)) {
		cerr << "Table does not exist: " << table_name << endl;
		return false;
	}

	return fs::remove(table_file);
}

bool DBManager::insertRecord(const string &table_name,
                             const vector<string> &record) {
	string table_file = getTableFilePath(table_name);
	if (!fs::exists(table_file)) {
		cerr << "Table does not exist: " << table_name << endl;
		return false;
	}

	ofstream file(table_file, ios::app);
	if (!file.is_open()) {
		cerr << "Failed to insert record into table: " << table_name << endl;
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

vector<vector<string>> DBManager::readAllRecords(const string &table_name) {
	string table_file = getTableFilePath(table_name);
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
	string table_file = getTableFilePath(table_name);
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
	string table_file = getTableFilePath(table_name);
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
