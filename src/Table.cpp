
#include "Table.h"
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>

namespace fs = std::filesystem;
using namespace std;

Table::Table(const string& name, fs::path base_path, bool temporary)
    : name_(name), path_(base_path / name), temp_(temporary) {
    if (!temp_) {
        loadMetadata();
        loadShards();
    } else {
        fs::create_directory(path_);
    }
}

string Table::tablePath() const {
    return path_.string();
}

string Table::getName() const {
    return name_;
}

bool Table::isTemp() const {
    return temp_;
}

void Table::loadShards() {
    vector<shared_ptr<Shard>> shards{};

    for (const auto& shard_path : fs::directory_iterator(tablePath())) {
        if (shard_path.path().extension() == ".csv") {
            auto shard = make_shared<Shard>(Shard(shard_path.path().string()));
            shards.push_back(shard);
        }
    }
    shards_ = shards;
}

void Table::loadMetadata() {
    string metadata_path = tablePath() + "/metadata.txt";

    if (!fs::exists(metadata_path)) {
        throw runtime_error("Metadata file not found for table: " + name_);
    }

    ifstream metadata_file(metadata_path);

    if (!metadata_file.is_open()) {
        throw runtime_error("Failed to open metadata file: " + name_);
    }

    unordered_map<string, int> attributes_map{};
    string line;

    while (getline(metadata_file, line)) {
        if (line.empty()) continue;

        size_t pos = line.find(',');
        if (pos == string::npos) {
            cerr << "Invalid mapping format in line: " << line << endl;
            continue;
        }

        string attr_name = line.substr(0, pos);
        int index = stoi(line.substr(pos + 1));

        attributes_map[attr_name] = index;
    }

    metadata_ = attributes_map;
}

const vector<shared_ptr<Shard>>& Table::getShards() const {
    return shards_;
}

void Table::setMetadata(const unordered_map<string, int>& metadata) {
    if (isTemp()) {
        metadata_ = metadata;
    }
}

const unordered_map<string, int>& Table::getMetadata() const {
    return metadata_;
}

future<shared_ptr<Table>> Table::join(const Table& other,
                                      const string& this_join_attr,
                                      const string& other_join_attr) {
    return async(
        launch::async, [this, other, this_join_attr, other_join_attr]() {
            vector<future<Shard::JoinResult>> join_futures;
            vector<shared_ptr<Shard>> joined_shards;

            // Process each shard of this table
            join_futures.reserve(getShards().size());
            for (const auto& shard : getShards()) {
                join_futures.push_back(shard->joinAsync(
                    other.getShards(), this_join_attr, other_join_attr,
                    getMetadata(), other.getMetadata()));
            }

            // Collect results
            for (auto& future : join_futures) {
                auto result = future.get();
                if (!result.success) {
                    throw runtime_error("Join failed: " + result.error_message);
                }
                joined_shards.push_back(result.result_shard);
            }

            // Create new temporary table for result
            auto result_table =
                make_shared<Table>(name_ + "_join_" + other.getName(),
                                   fs::temp_directory_path(), true);

            unordered_map<string, int> combined_metadata = getMetadata();
            int offset = (int)getMetadata().size();
            for (const auto& [key, value] : other.getMetadata()) {
                if (key == other_join_attr) {
                    combined_metadata[key] = getMetadata().at(this_join_attr);
                } else {
                    combined_metadata[key] = value + offset;
                }
            }
            result_table->setMetadata(combined_metadata);

            // Merge shards into result table
            auto merged_shard = make_shared<Shard>();
            {
                ofstream out(merged_shard->path());
                bool first = true;

                for (const auto& shard : joined_shards) {
                    ifstream in(shard->path());
                    if (in.is_open()) {
                        if (!first) {
                            string header;
                            getline(in, header);
                        }
                        out << in.rdbuf();
                        first = false;
                    }
                }
            }

            result_table->shards_ = {merged_shard};
            return result_table;
        });
}
