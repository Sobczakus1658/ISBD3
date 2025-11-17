CXX = g++
CXXFLAGS = -std=c++20 -g -I zstd/lib -I zstd/lib/common
LDFLAGS = -L zstd/lib -lzstd
TARGET = main

SRC = main.cpp \
      tests/tests.cpp \
      codec/codec_int.cpp \
      codec/codec_string.cpp \
      serialization/serializator.cpp \
      serialization/deserializator.cpp \
      validation/validator.cpp \
      statistics/statistics.cpp

OBJ = $(SRC:.cpp=.o)

all: batches zstd/lib/libzstd.a $(TARGET)

$(TARGET): $(OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

batches:
	mkdir -p batches

zstd/lib/libzstd.a:
	cd zstd && $(MAKE)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(TARGET)
	cd zstd && $(MAKE) clean

.PHONY: all clean
