FROM rust:1.90-trixie AS build


WORKDIR /app

# Copy sources (no special caching per your preference)
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build release binary
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/* \
    && cargo build --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 10001 appuser
USER appuser
WORKDIR /app

# Copy the binary to PATH
COPY --from=build /app/target/release/rds-lite /usr/local/bin/rds-lite

ENV RUST_LOG=info
EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/rds-lite"]
