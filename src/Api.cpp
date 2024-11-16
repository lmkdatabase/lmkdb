#include "Api.h"
#include <iostream>

DatabaseAPI::DatabaseAPI() : dbManager(new DBManager("./database")) {}

DatabaseAPI::~DatabaseAPI() {
    delete dbManager;
}

void DatabaseAPI::createTable(const std::string& tableName) {
    if (dbManager->createTable(tableName)) {
        std::cout << "Table created: " << tableName << std::endl;
    } else {
        std::cout << "Failed to create table: " << tableName << std::endl;
    }
}

void DatabaseAPI::deleteTable(const std::string& tableName) {
    if (dbManager->deleteTable(tableName)) {
        std::cout << "Table deleted: " << tableName << std::endl;
    } else {
        std::cout << "Failed to delete table: " << tableName << std::endl;
    }
}

void DatabaseAPI::insertIntoTable(const std::string& tableName, const std::vector<std::string>& record) {
    if (dbManager->insertRecord(tableName, record)) {
        std::cout << "Inserted into table: " << tableName << std::endl;
    } else {
        std::cout << "Failed to insert into table: " << tableName << std::endl;
    }
}

void DatabaseAPI::readTable(const std::string& tableName) {
    auto records = dbManager->readAllRecords(tableName);
    if (records.empty()) {
        std::cout << "No records found or table does not exist: " << tableName << std::endl;
        return;
    }

    for (const auto& record : records) {
        for (const auto& field : record) {
            std::cout << field << " ";
        }
        std::cout << std::endl;
    }
}

void DatabaseAPI::updateTable(const std::string& tableName, int recordId, const std::vector<std::string>& updatedRecord) {
    if (dbManager->updateRecord(tableName, recordId, updatedRecord)) {
        std::cout << "Record updated in table: " << tableName << std::endl;
    } else {
        std::cout << "Failed to update record in table: " << tableName << std::endl;
    }
}

void DatabaseAPI::removeFromTable(const std::string& tableName, int recordId) {
    if (dbManager->deleteRecord(tableName, recordId)) {
        std::cout << "Record deleted from table: " << tableName << std::endl;
    } else {
        std::cout << "Failed to delete record from table: " << tableName << std::endl;
    }
}
