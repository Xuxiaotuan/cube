FROM node:24.18.0 AS rust-builder

ENV CYPRESS_INSTALL_BINARY=0
ENV CUBESTORE_SKIP_POST_INSTALL=true
ENV http_proxy=http://127.0.0.1:7890
ENV https_proxy=http://127.0.0.1:7890

RUN ln -sf /usr/local/include/node /usr/include/node \
    && ln -sf /usr/local/include/node/common.gypi /usr/common.gypi

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       python3 gcc g++ make cmake ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --profile minimal --default-toolchain stable --no-modify-path 2>&1

ENV CARGO_HOME=/root/.cargo
ENV PATH=/root/.cargo/bin:$PATH

WORKDIR /cube
COPY . .

RUN cd /cube/rust/cubesql && cargo build --release -p cubesql 2>&1 \
    && cp /cube/rust/cubesql/target/release/cubesqld /usr/local/bin/cubesqld

# Build native neon binding (for cubejs-backend-native)
RUN cd /cube/packages/cubejs-backend-native \
    && cargo build --release 2>&1

FROM node:24.18.0 AS js-builder

ENV CYPRESS_INSTALL_BINARY=0
ENV CUBESTORE_SKIP_POST_INSTALL=true
ENV http_proxy=http://127.0.0.1:7890
ENV https_proxy=http://127.0.0.1:7890

RUN ln -sf /usr/local/include/node /usr/include/node \
    && ln -sf /usr/local/include/node/common.gypi /usr/common.gypi

WORKDIR /cube
COPY . .

RUN yarn config set network-timeout 120000 -g \
    && yarn config set registry https://registry.npmjs.org/
RUN yarn install --ignore-scripts && yarn cache clean
RUN rm -f /cube/rust/cubestore/tsconfig.json && grep -v "rust/cubestore" /cube/tsconfig.json > /tmp/tsconfig.json && mv /tmp/tsconfig.json /cube/tsconfig.json && yarn tsc
RUN yarn build && yarn lerna run build

FROM node:24.18.0

ENV NODE_ENV=production
ENV CUBEJS_DOCKER_IMAGE_VERSION=local
ENV CUBEJS_DOCKER_IMAGE_TAG=local
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

WORKDIR /cube
COPY --from=rust-builder /usr/local/bin/cubesqld /usr/local/bin/cubesqld
COPY --from=js-builder /cube .
COPY --from=rust-builder /cube/packages/cubejs-backend-native/target/release/libcubejs_native.so /cube/node_modules/@cubejs-backend/native/index.node

ENV NODE_PATH=/cube/conf/node_modules:/cube/node_modules
RUN ln -s /cube/node_modules/.bin/cubejs-server /usr/local/bin/cubejs 2>/dev/null || true

WORKDIR /cube/conf
EXPOSE 4000 15432
CMD ["cubejs", "server"]
