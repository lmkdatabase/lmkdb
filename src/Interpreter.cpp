#include "Interpreter.h"
#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include "Api.h"
#include "utils.h"

using namespace std;

Interpreter::Interpreter() : dbApi(new DatabaseAPI()) {}

Interpreter::~Interpreter() {
    delete dbApi;
}

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

        if (!validateInteger(tokens[2])) {
            cout << "Please provide a valid id as the third argument." << endl;
            return;
        }

        int recordId = stoi(tokens[2]);
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

    } else if (operation == "join" && tokens.size() >= 4) {
        auto delimiter = find(tokens.begin() + 1, tokens.end(), "-");

        if (delimiter == tokens.end()) {
            cout << "Please follow the specified format of a join query, "
                    "missing -"
                 << endl;
            return;
        }

        vector<string> tables(tokens.begin(), delimiter);
        vector<string> attributes(delimiter + 1, tokens.end());

        dbApi->joinOp(tables, attributes);

    } else if (operation == "help") {
        printUsage();
    } else {
        cout << "Unknown command \"" << operation
             << "\"\nType \"help\" for usage" << endl;
    }
}
