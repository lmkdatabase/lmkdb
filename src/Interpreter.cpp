#include "Interpreter.h"
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include "Api.h"
#include "utils.h"

using namespace std;

Interpreter::Interpreter(string_view dbDir)
    : dbApi(make_unique<DatabaseAPI>(string(dbDir))) {}

Interpreter::~Interpreter() = default;

bool Interpreter::validateInteger(const string &input) {
    try {
        stoi(input);
        return true;
    } catch (const invalid_argument &) {
        return false;
    } catch (const out_of_range &) {
        return false;
    }
}

void Interpreter::processCommand(const string &command) {
    vector<string> tokens;
    istringstream stream(command);
    string token;

    while (stream >> token) {
        tokens.push_back(token);
    }

    if (tokens.empty()) {
        cout << "Empty command." << endl;
        return;
    }

    boost::algorithm::to_lower(tokens[0]);

    string operation = tokens[0];

    if (operation == "create" && tokens.size() >= 3) {
        string tableName = tokens[1];
        vector<string> attributes(tokens.begin() + 2, tokens.end());
        dbApi->createOp(tableName, attributes);

    } else if (operation == "insert" && tokens.size() >= 3) {
        string tableName = tokens[1];
        vector<string> newRecord(tokens.begin() + 2, tokens.end());
        dbApi->insertOp(tableName, newRecord);

    } else if (operation == "update" && tokens.size() >= 4) {
        string tableName = tokens[1];

        size_t pos = tokens[2].find(':');
        if (pos == string::npos || tokens[0].substr(pos + 1).empty()) {
            cerr << "Error: Invalid format for specifying id." << endl;
            return;
        }

        string idValue = tokens[2].substr(pos + 1);
        if (!validateInteger(idValue)) {
            cerr << "Error: id must be a valid integer." << endl;
            return;
        }

        int recordId = stoi(idValue);
        vector<string> updatedRecord(tokens.begin() + 3, tokens.end());
        dbApi->updateOp(tableName, recordId, updatedRecord);

    } else if (operation == "delete" && tokens.size() >= 2) {
        string tableName = tokens[1];
        vector<string> attributes(tokens.begin() + 2, tokens.end());

        dbApi->deleteOp(tableName, attributes);

    } else if (operation == "read" && tokens.size() >= 2) {
        string tableName = tokens[1];
        vector<string> attr(tokens.begin() + 2, tokens.end());

        dbApi->readOp(tableName, attr);

    } else if (operation == "join" && tokens.size() >= 3) {
        vector<string> query(tokens.begin() + 1, tokens.end());

        dbApi->joinOp(query);

    } else if (operation == "help") {
        printUsage();
    } else {
        cout << "Unknown command \"" << operation
             << "\"\nType \"help\" for usage" << endl;
    }
}
