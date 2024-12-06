#ifndef WORKER_H
#define WORKER_H

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
    std::unordered_multimap<std::string, std::pair<std::string, std::streampos>>
    buildHashTable(const std::string& shard_path, int attr_pos);

   public:
    JoinWorker(const std::string& output_file) : output_path(output_file) {}

    bool processShardBatch(const std::string& shard_A,
                           const std::vector<std::string>& all_shards_B,
                           int attr_pos_A, int attr_pos_B);
};

#endif
