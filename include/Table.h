#include <memory>
#include <unordered_map>
#include "Shard.h"

class Table {
   private:
    std::string name_;
    std::filesystem::path path_;
    std::vector<std::shared_ptr<Shard>> shards_;
    std::unordered_map<std::string, int> metadata_;

    void loadMetadata();
    void loadShards();

   public:
    explicit Table(const std::string& name,
                   std::filesystem::path base_path = "./database");

    std::string tablePath();
    std::unordered_map<std::string, int> getMetadata();
    std::vector<std::shared_ptr<Shard>> getShards();
};
