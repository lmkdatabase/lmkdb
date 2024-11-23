#ifndef RECORD_MANAGER_H
#define RECORD_MANAGER_H

#include <string>
#include <vector>

class DBManager {
public:
    DBManager(const std::string& db_path);
    ~DBManager();

    bool createTable(const std::string& table_name);
    bool deleteTable(const std::string& table_name);

    bool insertRecord(const std::string& table_name, const std::vector<std::string>& record);
    std::vector<std::vector<std::string>> readAllRecords(const std::string& table_name);
    bool updateRecord(const std::string& table_name, int record_id, const std::vector<std::string>& updated_record);
    bool deleteRecord(const std::string& table_name, int record_id);

private:
    std::string database_path;

    std::string getTableFilePath(const std::string& table_name) const;
};

#endif
