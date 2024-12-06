#ifndef SHARD_H
#define SHARD_H

#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

class Shard {
   private:
    std::filesystem::path path_;
    bool temp_;
    static std::filesystem::path generateTempPath(
        const std::string& prefix = "shard_");

   public:
    struct JoinResult {
        bool success;
        std::shared_ptr<Shard> result_shard;
        std::string error_message;
    };

    explicit Shard(const std::string& file_path);
    explicit Shard();
    std::string path() const;

    Shard(Shard&& other) noexcept = default;
    Shard& operator=(Shard&& other) noexcept = default;

    // Prevent copying
    Shard(const Shard&) = delete;
    Shard& operator=(const Shard&) = delete;

    ~Shard();

    std::future<JoinResult> joinShards(
        const std::vector<std::shared_ptr<Shard>>& others,
        const std::string& this_join_attr, const std::string& other_join_attr,
        const std::unordered_map<std::string, int>& this_metadata,
        const std::unordered_map<std::string, int>& other_metadata) const;
};

#endif
