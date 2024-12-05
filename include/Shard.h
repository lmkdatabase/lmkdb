#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "worker.h"

namespace fs = std::filesystem;
using namespace std;

class Shard {
   private:
    fs::path path_;
    bool temp_;
    static fs::path generateTempPath(const string& prefix = "shard_");

   public:
    struct JoinResult {
        bool success;
        shared_ptr<Shard> result_shard;
        string error_message;
    };

    explicit Shard(const string& file_path);
    explicit Shard();
    string path() const;

    // Move semantics
    Shard(Shard&& other) noexcept = default;
    Shard& operator=(Shard&& other) noexcept = default;

    // Prevent copying
    Shard(const Shard&) = delete;
    Shard& operator=(const Shard&) = delete;

    ~Shard();

    future<JoinResult> joinAsync(
        const vector<shared_ptr<Shard>>& others, const string& this_join_attr,
        const string& other_join_attr,
        const unordered_map<string, int>& this_metadata,
        const unordered_map<string, int>& other_metadata) const;
};
