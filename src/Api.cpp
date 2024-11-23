#include "Api.h"
#include <iostream>

using namespace std;

DatabaseAPI::DatabaseAPI() : dbManager(new DBManager("./database")) {}

DatabaseAPI::~DatabaseAPI() { delete dbManager; }

void DatabaseAPI::createTable(const string &tableName) {
	if (dbManager->createTable(tableName)) {
		cout << "Table created: " << tableName << endl;
	} else {
		cout << "Failed to create table: " << tableName << endl;
	}
}

void DatabaseAPI::deleteTable(const string &tableName) {
	if (dbManager->deleteTable(tableName)) {
		cout << "Table deleted: " << tableName << endl;
	} else {
		cout << "Failed to delete table: " << tableName << endl;
	}
}

void DatabaseAPI::insertIntoTable(const string &tableName,
                                  const vector<string> &record) {
	if (dbManager->insertRecord(tableName, record)) {
		cout << "Inserted into table: " << tableName << endl;
	} else {
		cout << "Failed to insert into table: " << tableName << endl;
	}
}

void DatabaseAPI::readTable(const string &tableName) {
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

void DatabaseAPI::updateTable(const string &tableName, int recordId,
                              const vector<string> &updatedRecord) {
	if (dbManager->updateRecord(tableName, recordId, updatedRecord)) {
		cout << "Record updated in table: " << tableName << endl;
	} else {
		cout << "Failed to update record in table: " << tableName << endl;
	}
}

void DatabaseAPI::removeFromTable(const string &tableName, int recordId) {
	if (dbManager->deleteRecord(tableName, recordId)) {
		cout << "Record deleted from table: " << tableName << endl;
	} else {
		cout << "Failed to delete record from table: " << tableName << endl;
	}
}
