#include "Interpreter.h"
#include "Api.h"
#include <boost/algorithm/string.hpp>
#include <sstream>
#include <iostream>
#include <stdexcept>

Interpreter::Interpreter() : dbApi(new DatabaseAPI()) {}

Interpreter::~Interpreter() {
    delete dbApi;
}

void Interpreter::processCommand(const std::string& command) {
    std::vector<std::string> tokens;
    std::istringstream stream(command);
    std::string token;

    while (stream >> token) {
        tokens.push_back(token);
    }

    if (tokens.empty()) {
        std::cout << "Empty command." << std::endl;
        return;
    }

    boost::algorithm::to_lower(tokens[0]);

    try {
        if (tokens[0] == "create" && tokens.size() == 3 && tokens[1] == "table") {
            dbApi->createTable(tokens[2]);
        } else if (tokens[0] == "delete" && tokens.size() == 3 && tokens[1] == "table") {
            dbApi->deleteTable(tokens[2]);
        } else if (tokens[0] == "insert" && tokens.size() >= 3) {
            std::string tableName = tokens[1];
            std::vector<std::string> record(tokens.begin() + 2, tokens.end());
            dbApi->insertIntoTable(tableName, record);
        } else if (tokens[0] == "read" && tokens.size() == 3 && tokens[1] == "table") {
            dbApi->readTable(tokens[2]);
        } else if (tokens[0] == "update" && tokens.size() >= 4) {
            std::string tableName = tokens[1];
            int recordId = std::stoi(tokens[2]);
            std::vector<std::string> updatedRecord(tokens.begin() + 3, tokens.end());
            dbApi->updateTable(tableName, recordId, updatedRecord);
        } else if (tokens[0] == "remove" && tokens.size() == 3) {
            std::string tableName = tokens[1];
            int recordId = std::stoi(tokens[2]);
            dbApi->removeFromTable(tableName, recordId);
        } else {
            std::cout << "Unknown command or invalid syntax." << std::endl;
        }
    } catch (const std::invalid_argument& e) {
        std::cout << "Invalid argument: " << e.what() << std::endl;
    } catch (const std::out_of_range& e) {
        std::cout << "Value out of range: " << e.what() << std::endl;
    }
}
