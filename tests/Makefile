# ===============================
# Kompilator i flags
# ===============================
CXX = g++
CXXFLAGS = -std=c++20 -I.. -I../zstd/lib -I../zstd/lib/common -I../cpp-client -I../restbed-old/source -I../csv-parser/include -I/usr/include -g
LDFLAGS = -L../zstd/lib -lssl -lcrypto -lboost_system -lpthread -lzstd -lcpr -lcurl

# ===============================

# ===============================
# Pliki źródłowe testów (relative to this tests/ dir)
# ===============================
TEST_SRC = run_import_test.cpp
TEST_OBJ = $(TEST_SRC:.cpp=.o)

# ===============================
# Pliki źródłowe aplikacji wymagane do testów
# ===============================
APP_SRC = ../serialization/serializator.cpp \
		  ../serialization/deserializator.cpp \
		  ../validation/validator.cpp \
		  ../statistics/statistics.cpp \
		  ../codec/codec_int.cpp \
		  ../codec/codec_string.cpp 

APP_OBJ = $(APP_SRC:.cpp=.o)

# GoogleTest libs (prefer system -lgtest if available)
GTEST_LIB :=
GTEST_MAIN_LIB :=

EXTRA_LIBS :=

# ===============================
# Nazwa pliku wykonywalnego testów
# ===============================
TEST_BIN = run_tests

# ===============================
# Domyślne zadanie
# ===============================
all: $(TEST_BIN)

# ===============================
# Budowanie testów
# ===============================
$(TEST_BIN): $(TEST_OBJ) $(APP_OBJ)
	$(CXX) $(CXXFLAGS) $(TEST_OBJ) $(APP_OBJ) -o $@ $(LDFLAGS)

# ===============================
# Reguły kompilacji obiektów
# ===============================
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# ===============================
# Czyszczenie
# ===============================
clean:
	rm -f $(TEST_BIN) $(TEST_OBJ) $(APP_OBJ)
