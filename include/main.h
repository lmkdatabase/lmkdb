// include/main.h
#ifndef MAIN_H
#define MAIN_H
#include <array>
#include <string_view>

using namespace std;

#ifdef _WIN32
/* Don't know what the windows equivalent of ~/.config/ is */
constexpr array<string_view, 2> config_dir = {".config", "lmk"};
#else
constexpr array<string_view, 2> config_dir = {".config", "lmk"};
#endif
constexpr string_view histfile = ".lmkhistory";

constexpr string_view dbDir = "./database/";

#endif /* MAIN_H */
