#ifndef INTERPRETER_H
#define INTERPRETER_H

#include <string>

class DatabaseAPI;

class Interpreter {
   public:
    Interpreter();
    ~Interpreter();

    void processCommand(const std::string& command);
    static bool validateInteger(const std::string& input);

   private:
    DatabaseAPI* dbApi;
};

#endif
