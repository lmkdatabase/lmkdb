
#include "Table.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>

namespace fs = std::filesystem;
using namespace std;

Table::Table(const string& name, fs::path base_path)
    : name_(name), path_(base_path.string() + "/" + name) {
    loadMetadata();
}

string Table::tablePath() {
    return path_.string();
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
