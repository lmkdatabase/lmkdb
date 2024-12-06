#include "Table.h"
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <unordered_map>

namespace fs = std::filesystem;
using namespace std;

Table::Table(const string& name, const fs::path& base_path, bool temporary)
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

bool Table::loadMetadata() {
    string metadata_path = tablePath() + "/metadata.txt";

    if (!fs::exists(metadata_path)) {
        return false;
    }

    ifstream metadata_file(metadata_path);

    if (!metadata_file.is_open()) {
        return false;
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
    return true;
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

bool Table::validateAttributes(
    const unordered_map<string, string>& attributes) const {
    const auto& metadata = getMetadata();

    for (const auto& [attr, _] : attributes) {
        if (metadata.find(attr) == metadata.end()) {
            cerr << "Invalid attribute for table " << getName() << ": " << attr
                 << endl;
            return false;
        }
    }

    return true;
}

RecordLocation Table::findRecord(size_t target_idx) const {
    size_t current_index = 0;

    for (const auto& shard : getShards()) {
        // Count records in this shard
        ifstream file(shard->path());
        size_t shard_records = count(istreambuf_iterator<char>(file),
                                     istreambuf_iterator<char>(), '\n');

        // Check if target record is in this shard
        if (current_index + shard_records > target_idx) {
            return {.shard = shard, .record_index = target_idx - current_index};
        }
        current_index += shard_records;
    }
    return {.shard = nullptr, .record_index = 0};
}

bool Table::insert(const unordered_map<string, string>& updated_record) {
    if (!loadMetadata()) return false;
    auto shards = getShards();

    if (shards.empty() ||
        fs::file_size(shards.back()->path()) >= MAX_SHARD_SIZE) {
        shared_ptr<Shard> new_shard = make_shared<Shard>(
            tablePath() + "/shard_" + to_string(shards.size()) + ".csv");

        shards_.push_back(new_shard);
    }

    auto table_columns = getMetadata();
    vector<string> values(table_columns.size(), "");

    for (const auto& [attr, val] : updated_record) {
        if (table_columns.find(attr) != table_columns.end()) {
            values[table_columns[attr]] = val;
        } else {
            cerr << "Unknown attribute: " << attr << " for table: " << getName()
                 << endl;
            return false;
        }
    }

    string record;
    for (size_t i = 0; i < values.size(); ++i) {
        record += values[i];
        if (i < values.size() - 1) record += ",";
    }

    ofstream file(shards.back()->path(), ios::app);

    if (!file.is_open()) {
        cerr << "Failed to open shard for writing" << endl;
        return false;
    }

    file << record << "\n";

    return true;
}

void Table::read(const vector<int>& lines) {
    if (!loadMetadata()) return;

    int index = 0;

    for (const auto& shard : getShards()) {
        ifstream file(shard->path());
        string line;

        while (getline(file, line)) {
            istringstream ss(line);
            string field;
            bool first = true;

            if (!lines.empty() && ranges::find(lines, index) == lines.end()) {
                index++;
                continue;
            }

            while (getline(ss, field, ',')) {
                if (!first) cout << ",";
                cout << field;
                first = false;
            }
            cout << endl;
            index++;
        }
    }
}

bool Table::update(size_t id, const unordered_map<string, string>& updates) {
    if (!validateAttributes(updates)) {
        return false;
    }

    if (!loadMetadata()) return false;

    // Find which shard contains our record
    auto location = findRecord(id);
    if (!location.shard) {
        cerr << "Record " << id << " not found in table " << getName() << endl;
        return false;
    }

    fs::path temp_path = location.shard->path() + ".tmp";
    ifstream in_file(location.shard->path());
    ofstream out_file(temp_path);

    string line;
    size_t current_index = 0;
    auto metadata = getMetadata();

    while (getline(in_file, line)) {
        if (current_index != location.record_index) {
            out_file << line << "\n";
        } else {
            vector<string> record;
            istringstream ss(line);
            string field;

            while (getline(ss, field, ',')) {
                record.push_back(field);
            }

            for (const auto& [attr, value] : updates) {
                record[metadata.at(attr)] = value;
            }

            // Write updated record to temp
            bool first = true;
            for (const auto& field : record) {
                if (!first) out_file << ",";
                out_file << field;
                first = false;
            }
            out_file << "\n";
        }
        current_index++;
    }

    in_file.close();
    out_file.close();

    fs::rename(temp_path, location.shard->path());
    return true;
};

future<shared_ptr<Table>> Table::join(const Table& other,
                                      const string& this_join_attr,
                                      const string& other_join_attr) {
    if (!loadMetadata()) {
        throw runtime_error("Failed loading metadata before join");
    }
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

            for (auto& future : join_futures) {
                auto result = future.get();
                if (!result.success) {
                    throw runtime_error("Join failed: " + result.error_message);
                }
                joined_shards.push_back(result.result_shard);
            }

            // Create new temporary table for result
            string result_table_name = name_ + "_join_" + other.getName();
            fs::path temp_dir = fs::temp_directory_path() / result_table_name;
            fs::create_directories(temp_dir);

            auto result_table = make_shared<Table>(
                result_table_name, fs::temp_directory_path(), true);

            // Create and write metadata for the joined table
            unordered_map<string, int> combined_metadata = getMetadata();
            int offset = (int)getMetadata().size();
            for (const auto& [key, value] : other.getMetadata()) {
                if (key == other_join_attr) {
                    combined_metadata[key] = getMetadata().at(this_join_attr);
                } else {
                    combined_metadata[key] = value + offset;
                }
            }

            ofstream metadata_file(temp_dir / "metadata.txt");
            for (const auto& [attr, index] : combined_metadata) {
                metadata_file << attr << "," << index << "\n";
            }
            metadata_file.close();

            result_table->setMetadata(combined_metadata);

            // Merge shards into result table
            auto merged_shard =
                make_shared<Shard>((temp_dir / "shard_0.csv").string());
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

struct IndexCriteria {
    size_t target_index;

    bool operator()(const vector<string>& record, size_t current_index,
                    const unordered_map<string, int>& metadata) const {
        return current_index == target_index;
    }
};

struct AttributeCriteria {
    unordered_map<string, string> attr_values;

    bool operator()(const vector<string>& record, size_t current_index,
                    const unordered_map<string, int>& metadata) const {
        for (const auto& [attr, value] : attr_values) {
            if (record[metadata.at(attr)] != value) {
                return false;
            }
        }
        return true;
    }
};

template <typename T>
bool Table::deleteRecord(const T& criteria) {
    bool deleted_any = false;

    for (const auto& shard : getShards()) {
        fs::path temp_path = shard->path() + ".tmp";
        ifstream in_file(shard->path());
        ofstream out_file(temp_path);

        string line;
        size_t current_index = 0;

        while (getline(in_file, line)) {
            vector<string> record;
            istringstream ss(line);
            string field;
            while (getline(ss, field, ',')) {
                record.push_back(field);
            }

            if (!criteria(record, current_index, getMetadata())) {
                out_file << line << "\n";
            } else {
                deleted_any = true;
            }
            current_index++;
        }

        in_file.close();
        out_file.close();
        fs::rename(temp_path, shard->path());
    }

    return deleted_any;
}

bool Table::deleteByIndex(size_t index) {
    return deleteRecord(IndexCriteria{index});
}

bool Table::deleteByAttributes(
    const unordered_map<string, string>& attr_values) {
    if (!validateAttributes(attr_values)) {
        return false;
    }
    return deleteRecord(AttributeCriteria{attr_values});
}
