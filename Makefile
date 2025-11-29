.PHONY: all clean batches tests

CXX = g++
CXXFLAGS = -std=c++20 -g \
           -I zstd/lib \
           -I zstd/lib/common \
           -I cpp-client \
           -I/usr/local/include \
           -I restbed-old/source \
           -I csv-parser/include \
           -I thirdparty/include

LDFLAGS = -L zstd/lib -lssl -lcrypto -lboost_system -lpthread -lzstd

TARGET = main

# Źródła aplikacji
SRC = main.cpp \
      codec/codec_int.cpp \
      codec/codec_string.cpp \
      serialization/serializator.cpp \
      serialization/deserializator.cpp \
      validation/validator.cpp \
      statistics/statistics.cpp \
      service/executionService.cpp \
      metastore/metastore.cpp \
      queries/queries.cpp \
      results/results.cpp \
      controler.cpp \
      errors/errors.cpp

SRC += $(wildcard restbed-old/source/corvusoft/restbed/*.cpp)
SRC += $(wildcard restbed-old/source/corvusoft/restbed/detail/*.cpp)
SRC += $(wildcard csv-parser/include/internal/*.cpp)

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
