.PHONY: all clean batches tests docker

IMAGE_NAME ?= isbd
IMAGE_TAG  ?= latest

CXX = g++
CXXFLAGS = -std=c++20 -g \
           -I zstd/lib \
           -I zstd/lib/common \
           -I cpp-restbed-server/source \
           -I csv-parser/include \
           -I thirdparty/include \
           -I thirdparty/cpr/include \
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
      utils/utils.cpp

SRC += $(wildcard cpp-restbed-server/source/corvusoft/restbed/*.cpp)
SRC += $(wildcard cpp-restbed-server/source/corvusoft/restbed/detail/*.cpp)
SRC += $(wildcard csv-parser/include/internal/*.cpp)

OBJ = $(SRC:.cpp=.o)

CPR_DIR = thirdparty/cpr
CPR_BUILD = $(CPR_DIR)/build
CPR_LIB = $(CPR_BUILD)/libcpr.a

all: batches zstd/lib/libzstd.a $(CPR_LIB) $(TARGET)

$(TARGET): $(OBJ) $(CPR_LIB)
	$(CXX) $(OBJ) $(CPR_LIB) -o $@ $(LDFLAGS)

batches:
	mkdir -p batches

zstd/lib/libzstd.a:
	cd zstd && $(MAKE)

$(CPR_LIB):
	mkdir -p $(CPR_BUILD)
	cd $(CPR_BUILD) && cmake .. -DCPR_BUILD_TESTS=OFF
	cd $(CPR_BUILD) && make

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(TARGET)
	cd zstd && $(MAKE) clean
	cd $(CPR_BUILD) && make clean

.PHONY: docker
docker:
	docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .
