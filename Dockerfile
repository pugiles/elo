FROM rust:1.92-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && printf "fn main() {}\n" > src/main.rs
RUN cargo build --release

COPY src ./src
RUN touch src/main.rs
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -m -u 10001 appuser
WORKDIR /app

COPY --from=builder /app/target/release/elo /usr/local/bin/elo

RUN mkdir -p /data && chown appuser:appuser /data

ENV ELO_HOST=0.0.0.0 \
    ELO_PORT=3000 \
    ELO_DB_PATH=/data/elo.redb

VOLUME ["/data"]
EXPOSE 3000

USER appuser
ENTRYPOINT ["elo"]
