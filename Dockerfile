# Build from the parent directory, so the baileys-rs path dependency is in
# the context:  docker build -f baileys-sqs-rs/Dockerfile -t baileys-sqs ..
FROM rust:1-bookworm AS build
WORKDIR /src
COPY baileys-rs ./baileys-rs
COPY baileys-sqs-rs ./baileys-sqs-rs
WORKDIR /src/baileys-sqs-rs
RUN cargo build --release --bins

FROM debian:bookworm-slim AS production
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1001 --create-home app \
    && mkdir -p /app/auth_info_baileys \
    && chown app /app/auth_info_baileys
COPY --from=build /src/baileys-sqs-rs/target/release/baileys-sqs /src/baileys-sqs-rs/target/release/listener /src/baileys-sqs-rs/target/release/sender /usr/local/bin/
WORKDIR /app
USER app
CMD ["baileys-sqs"]
