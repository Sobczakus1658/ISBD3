FROM debian:12-slim AS build

RUN apt-get update && apt-get install -y \
    g++ make cmake \
    libssl-dev libboost-system-dev libboost-thread-dev libboost-filesystem-dev \
    zlib1g-dev pkg-config \
    nlohmann-json3-dev \
    libasio-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY zstd ./zstd
RUN make -C zstd

COPY . .

RUN make all


FROM debian:12-slim

RUN apt-get update && apt-get install -y \
    libssl3 libboost-system1.74.0 libboost-thread1.74.0 libboost-filesystem1.74.0 zlib1g && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/main /app/main
COPY data /app/data

EXPOSE 8086

CMD ["./main"]
