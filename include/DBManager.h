#ifndef RECORD_MANAGER_H
#define RECORD_MANAGER_H

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include "Table.h"

class DBManager {
   public:
    DBManager(std::string dbPath);
    ~DBManager();

    bool createTable(const std::string& table_name,
                     const std::vector<std::string>& attributes);
    bool deleteTable(const std::string& table_name);

    bool insertRecord(
        const std::string& table_name,
        const std::unordered_map<std::string, std::string>& record);
    void readTable(const std::string& table_name,
                   const std::vector<int>& line_numbers);
    bool updateRecord(
        const std::string& table_name, size_t id,
        const std::unordered_map<std::string, std::string>& attrMap);

    bool deleteByIndex(const std::string& table_name, size_t id);
    bool deleteByAttributes(
        const std::string& table_name,
        const std::unordered_map<std::string, std::string>& attrMap);

    bool joinTables(const std::vector<std::string>& tables,
                    std::unordered_map<std::string, std::string>& attrMap);

   private:
    const std::string database_path;
    std::unordered_map<std::string, std::shared_ptr<Table>> tables;

    void loadTables(const std::string& directory_path);
    std::shared_ptr<Table> findTable(const std::string& table_name);
    std::string NewTablePath(const std::string& table_name) const;
    std::string getMetadataPath(const std::string& table_name) const;
};

#endif
