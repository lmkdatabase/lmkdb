#include <pty.h>
#include <sys/wait.h>
#include <termios.h>
#include <unistd.h>
#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

using namespace std;

void write_input(int fd, std::string input) {
    // Send input to the subprocess
    input += "\n";  // Add newline for readline
    write(fd, input.c_str(), input.size());
    fsync(fd);
}
std::string read_output(int fd) {
    int bytesRead;
    char buffer[1 << 12];
    // Read and print the output
    fd_set readfds;
    FD_ZERO(&readfds);
    FD_SET(fd, &readfds);

    std::string output;

    while (true) {
        int ready = select(fd + 1, &readfds, nullptr, nullptr, nullptr);

        if (ready == 0) {
            std::cout << "timeout\n";
        } else if (ready > 0) {
            if (FD_ISSET(fd, &readfds)) {
                bytesRead = read(fd, buffer, sizeof(buffer) - 1);
                if (bytesRead > 0) {
                    buffer[bytesRead] = '\0';  // Null-terminate the buffer
                }
                fsync(fd);

                output += buffer;

                if (!output.ends_with("> ")) continue;

                return output;
            }
            std::cout << "fd is not set";
        } else {
            std::cout << "select error\n";
        }

        return std::string();
    }
}

int runInteractiveSubprocess(const std::string& command) {
    int masterFd;
    pid_t pid = forkpty(&masterFd, nullptr, nullptr, nullptr);

    if (pid == -1) {
        std::cerr << "Error: Failed to fork process.\n";
        return -1;
    }

    if (pid == 0) {  // Child process
        execlp("/bin/sh", "sh", "-c", command.c_str(), nullptr);
        std::cerr << "Error: Failed to execute command.\n";
        exit(1);
    } else {  // Parent process
        struct termios term_settings;
        if (tcgetattr(masterFd, &term_settings) == -1) {
            perror("tcgetattr");
            return -1;
        }

        // Disable ECHO mode
        term_settings.c_lflag &= ~ECHO;
        if (tcsetattr(masterFd, TCSANOW, &term_settings) == -1) {
            perror("tcsetattr");
            return -1;
        }

        return masterFd;
    }
}

void run_tests(int fd) {
    vector<string> inputs = {
        "read london_1000",
        "read london_10_6",
    };
    string output;

    output = read_output(fd);
    // std::cout << output;
    // std::cout.flush();

    for (auto& input : inputs) {
        // // Get user input
        // std::getline(std::cin, input);
        //
        auto start = std::chrono::system_clock::now();

        write_input(fd, input);
        output = read_output(fd);

        auto end = std::chrono::system_clock::now();
        // std::cout << output;
        // std::cout.flush();
        //
        auto duration =
            std::chrono::duration_cast<std::chrono::seconds>(end - start);

        cout << "operation: " << input << " took " << duration.count() << "s\n";
    }

    close(fd);
    // waitpid(pid, nullptr, 0);  // Wait for the child process to exit
}

int main() {
    std::string command = "./build/lmkdb";  // Replace with your command
    int fd = runInteractiveSubprocess(command);
    run_tests(fd);
    return 0;
}
