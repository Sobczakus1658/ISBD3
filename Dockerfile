FROM gcc:12.2

RUN apt-get update && apt-get install -y \
    cmake \
    libssl-dev \
    libboost-system-dev \
    libboost-thread-dev \
    nlohmann-json3-dev \
    zlib1g-dev \
    build-essential

WORKDIR /app

COPY . /app/

RUN g++ -std=c++20 \
        -I. \
        -I zstd/lib \
        -I zstd/lib/common \
        -I cpp-client \
        -I restbed-old/source \
        controler.cpp \
        service/utils.cpp \
        service/schemaService.cpp \
        $(ls restbed-old/source/corvusoft/restbed/*.cpp) \
        $(ls restbed-old/source/corvusoft/restbed/detail/*.cpp) \
        -o controler \
        -L zstd/lib \
        -lssl -lcrypto -lboost_system -lpthread -lzstd

CMD ["./controler"]
