#include <future>
#include <memory>
#include <unordered_map>
#include "Shard.h"

class Table {
   private:
    std::string name_;
    std::filesystem::path path_;
    std::vector<std::shared_ptr<Shard>> shards_;
    std::unordered_map<std::string, int> metadata_;
    bool temp_;

    void loadMetadata();
    void loadShards();

   public:
    explicit Table(const std::string& name,
                   std::filesystem::path base_path = "./database",
                   bool temporary = false);

    std::string tablePath() const;
    std::string getName() const;
    bool isTemp() const;
    const std::unordered_map<std::string, int>& getMetadata() const;
    void setMetadata(const std::unordered_map<std::string, int>& metadata);
    const std::vector<std::shared_ptr<Shard>>& getShards() const;
    std::future<std::shared_ptr<Table>> join(
        const Table& other, const std::string& this_join_attr,
        const std::string& other_join_attr);
};
