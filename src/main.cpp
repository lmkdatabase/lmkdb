#include "Interpreter.h"
#include <iostream>

using namespace std;

int main() {
	Interpreter interpreter;

	std::string command;
	while (true) {
		cout << "> ";
		getline(cin, command);

		if (command == "exit") { break; }

		interpreter.processCommand(command);
	}

	return 0;
}
