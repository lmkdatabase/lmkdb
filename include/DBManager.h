#ifndef RECORD_MANAGER_H
#define RECORD_MANAGER_H

#include <string>
#include <vector>
#include <unordered_map>

class DBManager {
public:
    DBManager(const std::string& db_path);
    ~DBManager();

    bool createTable(const std::string& table_name, const std::vector<std::string> &attributes);
    bool deleteTable(const std::string& table_name);

    bool insertRecord(const std::string& table_name, const std::unordered_map<std::string, std::string> &attrMap);
    std::vector<std::vector<std::string>> readAllRecords(const std::string& table_name);
    bool updateRecord(const std::string& table_name, int record_id, const std::vector<std::string>& updated_record);
    bool deleteRecord(const std::string& table_name, int record_id);

private:
    std::string database_path;

    std::string getFilePath(const std::string& file_name) const;
    std::unordered_map<std::string, int> getTableAttributesMap(const std::string &table_name);
};

#endif
