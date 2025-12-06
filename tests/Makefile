CXX = g++
CXXFLAGS = -std=c++20 
LDFLAGS =  -lcpr

TEST_SRC = tests.cpp
TEST_OBJ = $(TEST_SRC:.cpp=.o)

TEST_BIN = run_tests


all: $(TEST_BIN)

$(TEST_BIN): $(TEST_OBJ)
	$(CXX) $(CXXFLAGS) $(TEST_OBJ) -o $@ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(TEST_BIN) $(TEST_OBJ)
