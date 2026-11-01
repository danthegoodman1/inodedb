# The server and the CLI, with FoundationDB's client library, and its
# server for the example cluster in fdb.yaml. Build from the repository's
# root:
#
#     docker build -t inodedb:dev -f deploy/kubernetes/Dockerfile .
#
# RUNTIME names the image the binaries run on, any Debian bookworm.
ARG FDB_VERSION=7.3.79
ARG RUNTIME=debian:bookworm-slim

FROM rust:1-bookworm AS build
ARG FDB_VERSION
RUN apt-get update \
 && apt-get install -y --no-install-recommends clang libclang-dev protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
RUN case "$(dpkg --print-architecture)" in arm64) arch=aarch64 ;; *) arch=amd64 ;; esac \
 && curl -fsSL -o /tmp/clients.deb \
    "https://github.com/apple/foundationdb/releases/download/$FDB_VERSION/foundationdb-clients_${FDB_VERSION}-1_${arch}.deb" \
 && dpkg -i /tmp/clients.deb \
 && rm /tmp/clients.deb
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p inodedb-server --features fdb -p inodedb-cli \
 && cp target/release/inodedb target/release/idb /usr/local/bin/

FROM ${RUNTIME}
ARG FDB_VERSION
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && case "$(dpkg --print-architecture)" in arm64) arch=aarch64 ;; *) arch=amd64 ;; esac \
 && base="https://github.com/apple/foundationdb/releases/download/$FDB_VERSION" \
 && curl -fsSL -o /tmp/clients.deb "$base/foundationdb-clients_${FDB_VERSION}-1_${arch}.deb" \
 && curl -fsSL -o /tmp/server.deb "$base/foundationdb-server_${FDB_VERSION}-1_${arch}.deb" \
 && dpkg -i /tmp/clients.deb \
 && dpkg-deb -x /tmp/server.deb / \
 && rm /tmp/*.deb \
 && apt-get purge -y curl && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/inodedb /usr/local/bin/idb /usr/local/bin/
ENTRYPOINT ["inodedb"]
