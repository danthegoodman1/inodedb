# Rust and FoundationDB's client and server, for the backend's tests, and
# FUSE, for `idb mount`'s.
FROM rust:1-bookworm
ARG FDB_VERSION
RUN apt-get update \
 && apt-get install -y --no-install-recommends clang libclang-dev protobuf-compiler fuse3 \
 && rm -rf /var/lib/apt/lists/*
# The server package's scripts expect an init system, so it is unpacked.
RUN case "$(dpkg --print-architecture)" in arm64) arch=aarch64 ;; *) arch=amd64 ;; esac \
 && base="https://github.com/apple/foundationdb/releases/download/$FDB_VERSION" \
 && curl -fsSL -o /tmp/clients.deb "$base/foundationdb-clients_${FDB_VERSION}-1_${arch}.deb" \
 && curl -fsSL -o /tmp/server.deb "$base/foundationdb-server_${FDB_VERSION}-1_${arch}.deb" \
 && dpkg -i /tmp/clients.deb \
 && dpkg-deb -x /tmp/server.deb / \
 && rm /tmp/*.deb
COPY start /usr/local/bin/start-fdb
ENTRYPOINT ["start-fdb"]
