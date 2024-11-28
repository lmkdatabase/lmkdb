#ifndef INTERPRETER_H
#define INTERPRETER_H

#include <memory>
#include <string>

class DatabaseAPI;

class Interpreter {
   public:
    Interpreter();
    ~Interpreter();

    void processCommand(const std::string& command);
    bool validateInteger(const std::string& input);

   private:
    std::unique_ptr<DatabaseAPI> dbApi;
};

#endif
