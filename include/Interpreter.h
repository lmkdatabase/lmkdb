#ifndef INTERPRETER_H
#define INTERPRETER_H

#include <string>

class DatabaseAPI;

class Interpreter {
   public:
    Interpreter(std::string_view db_dir);
    ~Interpreter();

    void processCommand(const std::string& command);
    bool validateInteger(const std::string& input);

   private:
    DatabaseAPI* dbApi;
};

#endif
