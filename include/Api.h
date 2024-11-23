#ifndef API_H
#define API_H

#include <string>
#include <vector>
#include "DBManager.h"

class DatabaseAPI {
public:
    DatabaseAPI();
    ~DatabaseAPI();

    void createTable(const std::string& tableName);
    void deleteTable(const std::string& tableName);
    void insertIntoTable(const std::string& tableName, const std::vector<std::string>& record);
    void readTable(const std::string& tableName);
    void updateTable(const std::string& tableName, int recordId, const std::vector<std::string>& updatedRecord);
    void removeFromTable(const std::string& tableName, int recordId);

private:
    DBManager* dbManager;
};

#endif
