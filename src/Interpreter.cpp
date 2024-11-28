#include "Interpreter.h"
#include <boost/algorithm/string.hpp>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include "Api.h"

using namespace std;

Interpreter::Interpreter() : dbApi(make_unique<DatabaseAPI>()) {}

Interpreter::~Interpreter() = default;

bool Interpreter::validateInteger(const string &input) {
    try {
        stoi(input);
        return true;
    } catch (const std::invalid_argument &) {
        return false;
    } catch (const std::out_of_range &) {
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

    if (tokens.size() < 2) {
        cout << "" << endl;
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

    } else {
        cout << "Unknown command or invalid syntax." << endl;
    }
}
