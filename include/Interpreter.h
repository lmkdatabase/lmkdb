#ifndef INTERPRETER_H
#define INTERPRETER_H

#include <string>

class DatabaseAPI;

class Interpreter {
public:
    Interpreter();
    ~Interpreter();

    void processCommand(const std::string& command);

private:
    DatabaseAPI* dbApi;
};

#endif
