#include "utils.h"
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;

class JoinWorker {
   private:
    using HashTable = unordered_multimap<string, vector<string>>;

    string output_path;
    int join_attr_pos;
    mutex output_mutex;

    // Build hash table from one shard
    HashTable buildHashTable(const string& shard_path, int attr_pos) {
        HashTable table;
        ifstream file(shard_path);
        string line;

        while (getline(file, line)) {
            vector<string> record;
            istringstream ss(line);
            string field;
            while (getline(ss, field, ',')) {
                record.push_back(field);
            }
            table.insert({record[attr_pos], record});
        }
        return table;
    }

   public:
    void processShardBatch(const vector<string>& shard_batch_A,
                           const vector<string>& all_shards_B, int attr_pos_A,
                           int attr_pos_B) {
        for (const auto& shard_A : shard_batch_A) {
            // Build hash table for this shard of A
            auto hash_table = buildHashTable(shard_A, attr_pos_A);

            // Stream through all shards of B
            for (const auto& shard_B : all_shards_B) {
                ifstream file_B(shard_B);
                string line;

                while (getline(file_B, line)) {
                    vector<string> record_B;
                    istringstream ss(line);
                    string field;
                    while (getline(ss, field, ',')) {
                        record_B.push_back(field);
                    }

                    // Find matches
                    auto range = hash_table.equal_range(record_B[attr_pos_B]);
                    for (auto it = range.first; it != range.second; ++it) {
                        lock_guard<mutex> lock(output_mutex);
                        // Write joined record to output
                        ofstream out(output_path, ios::app);
                        for (const auto& field : it->second) {
                            out << field << ",";
                        }
                        for (size_t i = 0; i < record_B.size(); i++) {
                            out << record_B[i];
                            if (i < record_B.size() - 1) out << ",";
                        }
                        out << "\n";
                    }
                }
            }
        }
    }
};

string bold(const std::string& text) {
    return BOLD + text + RESET;
}

void printUsage() {
    cout << "Usage:\n"
         << bold("create <name> [attr...]")
         << "\n\tCreate a "
            "table with "
            "name "
            "<name> and list of attribute names [attr...]\n\n"
         << bold("insert <name> [attr:val...]")
         << "\n\tInsert a row to a table <name> "
            "with values val for each attribute attr\n\n"
         << bold("read <name> ") << "\n\tRead all rows from table <name>\n"
         << bold("read <name> id:<id>")
         << "\n\tRead row from table <name> with index "
            "<id>\n\n"
         << bold("delete <name>")
         << "\n\tDelete all rows from table "
            "<name>\n"
         << bold("delete <name> id:<id>")
         << "\n\tDelete row with "
            "index <id> from table <name>\n"
         << bold("delete <name> id:<id> attr1 atr2...")
         << "\n\tDelete values for specified attributes from row with "
            "index <id> from table <name>\n"
         << bold("delete <name> [attr:val...]")
         << "\n\tDelete rows matching _all_ attr:val "
            "combination from table <name>\n\n"
         << bold("update <name> id:<id> [attr:val...]")
         << "\n\tUpdate attributes attr with "
            "values val... for row with index <id> from table "
            "<name>\n\n"
         << bold(
                "join <table1>.<attr1> <table2>.<attr2> "
                "[<table_n>.<attr_n>...]")
         << "\n\tJoin tables <table1> and <table2> (and up to "
            "<table_n>) on attributes <attr1> "
            "and <attr2> (and up to <attr_n>), performs inner join"
         << endl;
}
