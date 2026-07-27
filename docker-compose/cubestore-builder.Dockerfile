FROM node:24.18.0 AS builder

ENV http_proxy=http://127.0.0.1:7890
ENV https_proxy=http://127.0.0.1:7890

# librocksdb-sys 需要 libclang 来做 bindgen
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 gcc g++ make cmake ca-certificates curl \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile minimal --default-toolchain nightly-2025-08-01 --no-modify-path 2>&1

ENV CARGO_HOME=/root/.cargo
ENV PATH=/root/.cargo/bin:$PATH
ENV LIBCLANG_PATH=/usr/lib/llvm-14/lib

WORKDIR /cube
COPY . .

RUN cd /cube/rust/cubestore && cargo build --release -p cubestore 2>&1

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends libssl3t64 ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /cube/rust/cubestore/target/release/cubestored /usr/local/bin/cubestored
EXPOSE 3306
ENV RUST_BACKTRACE=true
CMD ["cubestored"]
