#ifndef INTERPRETER_H
#define INTERPRETER_H

#include <memory>
#include <string>

class DatabaseAPI;

class Interpreter {
   public:
    Interpreter(std::string_view dbDir);
    ~Interpreter();

    void processCommand(const std::string& command);
    static bool validateInteger(const std::string& input);

   private:
    std::unique_ptr<DatabaseAPI> dbApi;
};

#endif
