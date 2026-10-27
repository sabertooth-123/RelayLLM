# syntax=docker/dockerfile:1
#
# Multi-stage build: compile with the pinned Rust image, ship only the binary
# on a slim Debian of the same release (same glibc).
#
#   docker build -t relayllm:dev .
#   docker run --rm relayllm:dev --help

FROM rust:1.99-slim-trixie AS build
WORKDIR /src
COPY . .
# Cache mounts keep the registry and target dir between builds. `--locked`
# builds exactly the dependency versions in Cargo.lock.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin relay \
 && cp target/release/relay /usr/local/bin/relay

FROM debian:trixie-slim
# Unprivileged user; /data is the SQLite volume and must be writable by it.
RUN useradd --system --uid 10001 --home-dir /data --shell /usr/sbin/nologin relay \
 && mkdir -p /data && chown relay:relay /data
COPY --from=build /usr/local/bin/relay /usr/local/bin/relay
USER relay
WORKDIR /data
VOLUME ["/data"]
EXPOSE 7070
ENTRYPOINT ["relay"]
CMD ["--help"]
