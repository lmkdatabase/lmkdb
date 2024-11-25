#ifndef API_H
#define API_H

#include <string>
#include <vector>
#include "DBManager.h"

class DatabaseAPI {
public:
    DatabaseAPI();
    ~DatabaseAPI();

    void createOp(const std::string& tableName, const std::vector<std::string>& attributes);
    void deleteOp(const std::string& tableName, const std::vector<std::string>& tokens);
    void insertOp(const std::string& tableName, const std::vector<std::string>& record);
    void readOp(const std::string& tableName, const std::vector<std::string>& tokens);
    void updateOp(const std::string& tableName, const int &recordId, const std::vector<std::string>& updatedRecord);
private:
    DBManager* dbManager;

    bool validateInteger(const std::string &input);
};

#endif
