FROM ubuntu:22.04 AS builder
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN rm -rf build && mkdir build && cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build

FROM ubuntu:22.04
RUN apt-get update && apt-get install -y \
    redis-tools \
    curl \
    iproute2 netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/build/DRedis /usr/local/bin/DRedis


WORKDIR /home/redis

EXPOSE 6380
EXPOSE 6381

HEALTHCHECK --interval=30s --timeout=3s \
  CMD redis-cli ping || exit 1

CMD ["DRedis"]