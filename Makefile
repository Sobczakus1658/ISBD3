CXX = g++
CXXFLAGS = -std=c++20 -g \
           -I zstd/lib \
           -I zstd/lib/common \
           -I cpp-client \
           -I/usr/local/include \
           -I restbed-old/source

LDFLAGS = -L zstd/lib -lssl -lcrypto -lboost_system -lpthread -lzstd

TARGET = main

SRC = main.cpp \
      tests/tests.cpp \
      codec/codec_int.cpp \
      codec/codec_string.cpp \
      serialization/serializator.cpp \
      serialization/deserializator.cpp \
      validation/validator.cpp \
      statistics/statistics.cpp \
      service/utils.cpp \
      service/schemaService.cpp \
      metastore/metastore.cpp

SRC += $(wildcard restbed-old/source/corvusoft/restbed/*.cpp)
SRC += $(wildcard restbed-old/source/corvusoft/restbed/detail/*.cpp)

OBJ = $(SRC:.cpp=.o)

all: batches zstd/lib/libzstd.a $(TARGET)

$(TARGET): $(OBJ)
	$(CXX) $(OBJ) -o $@ $(LDFLAGS)

batches:
	mkdir -p batches

zstd/lib/libzstd.a:
	cd zstd && $(MAKE)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(TARGET)
	cd zstd && $(MAKE) clean

.PHONY: all clean batches
