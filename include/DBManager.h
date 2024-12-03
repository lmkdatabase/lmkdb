#ifndef RECORD_MANAGER_H
#define RECORD_MANAGER_H

#include <string>
#include <unordered_map>
#include <vector>

struct ShardLocation {
    std::string shard_path;
    size_t record_index;
};

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
    void readTable(const std::string& table_name,
                   const std::vector<int>& line_numbers);
    bool updateRecord(
        const std::string& table_name, size_t id,
        const std::unordered_map<std::string, std::string>& attrMap);

    bool deleteByIndex(const std::string& table_name, size_t id,
                       const std::vector<std::string>& attributes);
    bool deleteByAttributes(
        const std::string& table_name,
        const std::unordered_map<std::string, std::string>& attrMap);

    bool joinTables(const std::vector<std::string>& tables,
                    std::unordered_map<std::string, std::string>& attrMap);

   private:
    const std::string database_path;
    const size_t MAX_SHARD_SIZE = 1024 * 1024 * 1024;  // 1GB

    std::string getTablePath(const std::string& table_name) const;
    std::string getShardPath(const std::string& table_name,
                             size_t shard_num) const;
    std::vector<std::string> getShardPaths(const std::string& table_name);
    ShardLocation findShardForUpdate(const std::string& table_name,
                                     size_t target_idx);
    std::string getTargetShard(const std::string& table_name);

    std::string getMetadataPath(const std::string& table_name) const;

    bool updateShardRecord(
        const std::string& table_name, const std::string& shard_path,
        size_t target_idx,
        const std::unordered_map<std::string, std::string>& updates);

    void printRecords(const std::vector<std::vector<std::string>>& records);

    std::vector<std::vector<std::string>> joinRecords(
        const std::vector<std::vector<std::string>>& left_records,
        const std::vector<std::vector<std::string>>& right_records,
        int left_index, int right_index);

    std::unordered_map<std::string, int> createJoinAttributeMap(
        const std::unordered_map<std::string, int>& left_attributes,
        const std::unordered_map<std::string, int>& right_attributes,
        const std::string& left_table_name,
        const std::string& right_table_name);

    std::string getFilePath(const std::string& file_name) const;
    std::unordered_map<std::string, int> getMetadata(
        const std::string& table_name);
};

#endif
