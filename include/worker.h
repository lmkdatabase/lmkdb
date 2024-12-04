#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

class JoinWorker {
   private:
    std::string output_path;
    int join_attr_pos;
    std::mutex output_mutex;

    // Build hash table from single shard
    //
    std::unordered_multimap<std::string, std::vector<std::string>>
    buildHashTable(const std::string& shard_path, int attr_pos);

   public:
    using HashTable =
        std::unordered_multimap<std::string, std::vector<std::string>>;
    
    JoinWorker(const std::string& output_file, int attr_position)
        : output_path(output_file), join_attr_pos(attr_position) {}

    void processShardBatch(const std::vector<std::string>& shard_batch_A,
                           const std::vector<std::string>& all_shards_B,
                           int attr_pos_A, int attr_pos_B);
};
