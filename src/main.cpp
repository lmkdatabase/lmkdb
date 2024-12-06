#include "main.h"
#include <readline/history.h>
#include <readline/readline.h>
#include <filesystem>
#include <iostream>
#include "Interpreter.h"

namespace fs = std::filesystem;
using namespace std;

fs::path get_or_create_cfg_path() {
#ifdef _WIN32
    const string home(getenv("USERPROFILE"));
#else
    const string home(getenv("HOME"));
#endif
    fs::path cfg_path = home;

    for (auto path : config_dir) {
        cfg_path /= path; /* boost syntax for path appends */
    }

    if (!fs::exists(cfg_path)) {
        cout << "initializing cfg dir at " << cfg_path.string() << "\n";
        fs::create_directory(cfg_path);
    }

    return cfg_path;
}

void init_lmk() {
    fs::path cfg_path = get_or_create_cfg_path();
    fs::path histpath = cfg_path / histfile;
    read_history(histpath.c_str());
}

void exit_lmk() {
    fs::path cfg_path = get_or_create_cfg_path();
    fs::path histpath = cfg_path / histfile;
    write_history(histpath.c_str());
}

int main() {
    Interpreter interpreter(dbDir);
    string command;

    /* initialize lmk*/
    init_lmk();

    while (true) {
        command = string(readline("> "));

        if (command == "exit") {
            break;
        }

        add_history(command.c_str());

        interpreter.processCommand(command);
    }

    /* cleanup lmk
     * TODO: call on SIGTERM */
    exit_lmk();

    return 0;
}
