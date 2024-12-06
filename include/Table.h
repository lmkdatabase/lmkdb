#include <future>
#include <memory>
#include <unordered_map>
#include "Shard.h"

struct RecordLocation {
    std::shared_ptr<Shard> shard;
    size_t record_index;
};

class Table {
   private:
    std::string name_;
    std::filesystem::path path_;
    std::vector<std::shared_ptr<Shard>> shards_;
    std::unordered_map<std::string, int> metadata_;
    bool temp_;

    void loadMetadata();
    void loadShards();

    const size_t MAX_SHARD_SIZE = 1024 * 1024 * 1024;  // 1GB

   public:
    explicit Table(const std::string& name,
                   std::filesystem::path base_path = "./database",
                   bool temporary = false);

    std::string tablePath() const;
    std::string getName() const;
    bool isTemp() const;

    const std::unordered_map<std::string, int>& getMetadata() const;
    const std::vector<std::shared_ptr<Shard>>& getShards() const;
    void setMetadata(const std::unordered_map<std::string, int>& metadata);
    RecordLocation findRecord(size_t target_idx) const;

    bool validateAttributes(
        const std::unordered_map<std::string, std::string>& attributes) const;

    template <typename T>
    bool deleteRecord(const T& criteria);

    bool deleteByIndex(size_t index);
    bool deleteByAttributes(
        const std::unordered_map<std::string, std::string>& attr_values);

    std::future<std::shared_ptr<Table>> join(
        const Table& other, const std::string& this_join_attr,
        const std::string& other_join_attr);
    void read(const std::vector<int>& lines);
    bool insert(
        const std::unordered_map<std::string, std::string>& updated_record);
    bool update(size_t id,
                const std::unordered_map<std::string, std::string>& updates);
};
