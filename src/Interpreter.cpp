#include "Interpreter.h"
#include "Api.h"
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <sstream>
#include <stdexcept>

using namespace std;

Interpreter::Interpreter() : dbApi(new DatabaseAPI()) {}

Interpreter::~Interpreter() { delete dbApi; }

void Interpreter::processCommand(const string &command) {
	vector<string> tokens;
	istringstream stream(command);
	string token;

	while (stream >> token) { tokens.push_back(token); }

	if (tokens.empty()) {
		cout << "Empty command." << endl;
		return;
	}

	boost::algorithm::to_lower(tokens[0]);

	try {
		if (tokens[0] == "create" && tokens.size() == 3 &&
		    tokens[1] == "table") {
			dbApi->createTable(tokens[2]);
		} else if (tokens[0] == "delete" && tokens.size() == 3 &&
		           tokens[1] == "table") {
			dbApi->deleteTable(tokens[2]);
		} else if (tokens[0] == "insert" && tokens.size() >= 3) {
			string tableName = tokens[1];
			vector<string> record(tokens.begin() + 2, tokens.end());
			dbApi->insertIntoTable(tableName, record);
		} else if (tokens[0] == "read" && tokens.size() == 3 &&
		           tokens[1] == "table") {
			dbApi->readTable(tokens[2]);
		} else if (tokens[0] == "update" && tokens.size() >= 4) {
			string tableName = tokens[1];
			int recordId = stoi(tokens[2]);
			vector<string> updatedRecord(tokens.begin() + 3, tokens.end());
			dbApi->updateTable(tableName, recordId, updatedRecord);
		} else if (tokens[0] == "remove" && tokens.size() == 3) {
			const string &tableName = tokens[1];
			int recordId = stoi(tokens[2]);
			dbApi->removeFromTable(tableName, recordId);
		} else {
			cout << "Unknown command or invalid syntax." << endl;
		}
	} catch (const invalid_argument &e) {
		cout << "Invalid argument: " << e.what() << endl;
	} catch (const out_of_range &e) {
		cout << "Value out of range: " << e.what() << endl;
	}
}
