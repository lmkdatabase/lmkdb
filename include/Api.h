#ifndef API_H
#define API_H

#include <memory>
#include <string>
#include <vector>
#include "DBManager.h"

class DatabaseAPI {
   public:
    DatabaseAPI(const std::string& dbPath);
    ~DatabaseAPI();

    void createOp(const std::string& tableName,
                  const std::vector<std::string>& attributes);
    void deleteOp(const std::string& tableName,
                  const std::vector<std::string>& tokens);
    void insertOp(const std::string& tableName,
                  const std::vector<std::string>& tokens);
    void readOp(const std::string& tableName,
                const std::vector<std::string>& tokens);
    void updateOp(const std::string& tableName, size_t recordId,
                  const std::vector<std::string>& updatedRecord);
    void joinOp(const std::vector<std::string>& query);

   private:
    std::unique_ptr<DBManager> dbManager;

    bool validateInteger(const std::string& input);
};

#endif
