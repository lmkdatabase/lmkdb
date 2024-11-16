#include <iostream>
#include "Interpreter.h"

int main() {
    Interpreter interpreter;

    std::string command;
    while (true) {
        std::cout << "> ";
        std::getline(std::cin, command);

        if (command == "exit") {
            break;
        }

        interpreter.processCommand(command);
    }

    return 0;
}
