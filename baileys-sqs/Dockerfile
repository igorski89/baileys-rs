# Build from the repository root, so the library is in the context:
#   docker build -f baileys-sqs/Dockerfile -t baileys-sqs .
FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p baileys-sqs --bins

FROM debian:bookworm-slim AS production
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1001 --create-home app \
    && mkdir -p /app/auth_info_baileys \
    && chown app /app/auth_info_baileys
COPY --from=build /src/target/release/baileys-sqs /src/target/release/listener /src/target/release/sender /usr/local/bin/
WORKDIR /app
USER app
CMD ["baileys-sqs"]
