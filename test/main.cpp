#include <gtest/gtest.h>
#include "Interpreter.hpp"

std::string hw() {
    return "hello world";
}

TEST(HelloWorldTest, saysHelloWorld) {
    ASSERT_EQ(hw(), "hello world");
}

TEST(ValidateInteger, validatesInteger) {
    EXPECT_EQ(Interpreter::validateInteger("0"), 1);
    EXPECT_EQ(Interpreter::validateInteger("0001"), 1);
    EXPECT_EQ(Interpreter::validateInteger("0001a"), 1)
        << "Integer can be base 16";
    EXPECT_EQ(Interpreter::validateInteger("0x1a"), 1)
        << "Integer can be base 16";
    EXPECT_EQ(Interpreter::validateInteger("x"), 0)
        << "Integer must be valid base 16";
    EXPECT_EQ(Interpreter::validateInteger("abcds"), 0);
    EXPECT_EQ(Interpreter::validateInteger("hello there"), 0);
    EXPECT_EQ(Interpreter::validateInteger("-123"), 1)
        << "negative integers are integers too";
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
