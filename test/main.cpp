#include <gtest/gtest.h>

std::string hw() {
    return "hello world";
}

TEST(HelloWorldTest, saysHelloWorld) {
    ASSERT_EQ(hw(), "hello world");
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
