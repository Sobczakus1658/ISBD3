.PHONY: all clean batches tests docker

IMAGE_NAME ?= isbd
IMAGE_TAG  ?= latest

CXX = g++
CXXFLAGS = -std=c++20 -g \
           -I zstd/lib \
           -I zstd/lib/common \
           -I cpp-restbed-server/source \
           -I csv-parser/include \
           -I/usr/local/include

LDFLAGS = -L zstd/lib -lssl -lcrypto -lboost_system -lpthread -lzstd

TARGET = main

SRC = \
      controler.cpp \
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
      errors/errors.cpp \
      utils/utils.cpp \
      query/parser/selectQueryParser.cpp \
      query/executor/selectExecutor.cpp \
      query/planer/selectPlaner.cpp \
      query/evaluation/evalColumnExpression.cpp \
      query/evaluation/expression_hasher.cpp

SRC += $(wildcard cpp-restbed-server/source/corvusoft/restbed/*.cpp)
SRC += $(wildcard cpp-restbed-server/source/corvusoft/restbed/detail/*.cpp)
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

.PHONY: docker
docker:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

