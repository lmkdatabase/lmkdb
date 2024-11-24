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

    if (tokens.size() < 2) {
		cout << "" << endl;
		return;
	}

	boost::algorithm::to_lower(tokens[0]);

	string operation = tokens[0];
    string tableName = tokens[1];

    if (operation == "create" && tokens.size() >= 3) {
        string tableName = tokens[1];
        vector<string> attributes(tokens.begin() + 2, tokens.end());
        dbApi->createTable(tableName, attributes);
        
    } else if (operation == "insert" && tokens.size() >= 3) {
        string tableName = tokens[1];
        vector<string> values(tokens.begin() + 2, tokens.end());
        dbApi->insertIntoTable(tableName, values);

    } else if (operation == "delete" && tokens.size() >= 2) {
        string tableName = tokens[1];
        dbApi->deleteTable(tokens[2]);
    }  else if (operation == "read" && tokens.size() == 3) {
        dbApi->readTable(tokens[2]);
    } else if (operation == "update" && tokens.size() >= 4) {
        string tableName = tokens[1];
        int recordId = stoi(tokens[2]);
        vector<string> updatedRecord(tokens.begin() + 3, tokens.end());
        dbApi->updateTable(tableName, recordId, updatedRecord);
    } else if (operation == "remove" && tokens.size() == 3) {
        const string &tableName = tokens[1];
        int recordId = stoi(tokens[2]);
        dbApi->removeFromTable(tableName, recordId);
    } else {
        cout << "Unknown command or invalid syntax." << endl;
    }
}
