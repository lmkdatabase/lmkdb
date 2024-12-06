#include "utils.hpp"
#include <iostream>
#include <string>

using namespace std;

string bold(const string& text) {
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
