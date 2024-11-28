#ifndef RECORD_MANAGER_H
#define RECORD_MANAGER_H

#include <string>
#include <unordered_map>
#include <vector>

class DBManager {
   public:
    DBManager(std::string dbPath);
    ~DBManager();

    bool createTable(const std::string& table_name,
                     const std::vector<std::string>& attributes);
    bool deleteTable(const std::string& table_name);

    bool insertRecord(
        const std::string& table_name,
        const std::unordered_map<std::string, std::string>& attrMap);
    std::vector<std::vector<std::string>> readAllRecords(
        const std::string& table_name);
    bool updateRecord(
        const std::string& table_name, const int& id,
        const std::unordered_map<std::string, std::string>& attrMap);

    bool deleteByIndex(const std::string& table_name, const int& id,
                       const std::vector<std::string>& attributes);
    bool deleteByAttributes(
        const std::string& table_name,
        const std::unordered_map<std::string, std::string>& attrMap);

   private:
    const std::string database_path;

    std::string getFilePath(const std::string& file_name) const;
    std::unordered_map<std::string, int> getTableAttributesMap(
        const std::string& table_name);
};

#endif
