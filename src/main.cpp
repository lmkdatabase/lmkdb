// src/main.cpp
#include <iostream>
#include <fstream> 
#include <boost/filesystem.hpp>
#include "test.h"

int main() {
    Test test;
    test.sayHello();

    boost::filesystem::path filePath("example.txt");

    if (!boost::filesystem::exists(filePath)) {
        std::ofstream file(filePath.string());
        if (file) {
            std::cout << "File created: " << filePath << std::endl;
        } else {
            std::cerr << "Failed to create file: " << filePath << std::endl;
        }
    } else {
        std::cout << "File already exists: " << filePath << std::endl;
    }

    return 0;
}
