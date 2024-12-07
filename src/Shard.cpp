
#include <Shard.hpp>
#include <filesystem>
#include <future>
#include <memory>
#include <random>
#include <string>
#include <vector>
#include <worker.hpp>

namespace fs = std::filesystem;
using namespace std;

fs::path Shard::generateTempPath(const string& prefix) {
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<> dis(0, 999999);

    fs::path temp_dir = fs::temp_directory_path();
    string unique_name =
        prefix +
        to_string(chrono::system_clock::now().time_since_epoch().count()) +
        "_" + to_string(dis(gen)) + ".csv";

    return temp_dir / unique_name;
}

Shard::Shard(const string& file_path) : path_(file_path), temp_(false) {}
Shard::Shard() : path_(generateTempPath()), temp_(true) {}

string Shard::path() const {
    return path_.string();
}

Shard::~Shard() {
    if (temp_ && fs::exists(path_)) {
        fs::remove(path_);
    }
};

future<Shard::JoinResult> Shard::joinShards(
    const vector<shared_ptr<Shard>>& others, const string& this_join_attr,
    const string& other_join_attr,
    const unordered_map<string, int>& this_metadata,
    const unordered_map<string, int>& other_metadata) const {
    return async(
        launch::async,
        [others, this_metadata, this_join_attr, other_metadata, other_join_attr,
         this]() -> JoinResult {
            JoinResult result;
            auto result_shard = make_shared<Shard>();

            vector<string> other_paths{};
            other_paths.reserve(others.size());

            for (const auto& shard : others) {
                other_paths.push_back(shard->path());
            }

            JoinWorker worker(result_shard->path());

            bool success = worker.processShardBatch(
                this->path(), other_paths, this_metadata.at(this_join_attr),
                other_metadata.at(other_join_attr));

            result.success = success;
            result.result_shard = success ? result_shard : nullptr;
            result.error_message = success ? "" : "Failed to join shards";

            return result;
        });
};
