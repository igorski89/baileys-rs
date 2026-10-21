# Build from the repository root, so the library is in the context:
#   docker build -f baileys-sqs/Dockerfile -t baileys-sqs .
# Multi-platform (linux/amd64 and linux/arm64): the build stage runs on the
# builder's own platform and cross-compiles, so no emulated Rust build.
FROM --platform=$BUILDPLATFORM rust:1-bookworm AS build
ARG TARGETARCH
RUN case "$TARGETARCH" in \
      amd64) echo x86_64-unknown-linux-gnu > /target; echo x86-64 > /gcc ;; \
      arm64) echo aarch64-unknown-linux-gnu > /target; echo aarch64 > /gcc ;; \
      *) echo "unsupported TARGETARCH $TARGETARCH" >&2; exit 1 ;; \
    esac \
    && if [ "$(dpkg --print-architecture)" != "$TARGETARCH" ]; then \
         apt-get update \
         && apt-get install -y --no-install-recommends "gcc-$(cat /gcc)-linux-gnu" "libc6-dev-${TARGETARCH}-cross" \
         && rm -rf /var/lib/apt/lists/*; \
       fi \
    && rustup target add "$(cat /target)"
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,id=baileys-rs-target-${TARGETARCH},sharing=locked \
    target="$(cat /target)" \
    && prefix="$(echo "$target" | sed 's/-unknown//')" \
    && upper="$(echo "$target" | tr 'a-z-' 'A-Z_')" \
    && export "CARGO_TARGET_${upper}_LINKER=${prefix}-gcc" \
              "CC_$(echo "$target" | tr - _)=${prefix}-gcc" \
              "AR_$(echo "$target" | tr - _)=${prefix}-ar" \
    && cargo build --release --locked -p baileys-sqs --bins --target "$target" \
    && mkdir /out \
    && cp "target/$target/release/baileys-sqs" "target/$target/release/listener" "target/$target/release/sender" /out/

FROM debian:bookworm-slim AS production
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 1001 --create-home app \
    && mkdir -p /app/auth_info_baileys /session \
    && chown app /app/auth_info_baileys /session
COPY --from=build /out/ /usr/local/bin/
WORKDIR /app
USER app
CMD ["baileys-sqs"]
