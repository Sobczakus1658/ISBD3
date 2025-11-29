FROM debian:12-slim AS build

RUN apt-get update && apt-get install -y \
    g++ make cmake libssl-dev libboost-system-dev libboost-thread-dev \
    nlohmann-json3-dev zlib1g-dev pkg-config

WORKDIR /app

COPY . .

RUN make -C zstd clean && make -C zstd
RUN make all

FROM debian:12-slim

RUN apt-get update && apt-get install -y \
    libssl3 libboost-system1.74.0 libboost-thread1.74.0 zlib1g && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/main /app/main
COPY data /app/data

EXPOSE 8084

CMD ["./main"]
