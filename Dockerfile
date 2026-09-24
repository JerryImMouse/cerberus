ARG DOTNET_TAG=10.0-noble

FROM rust:1.97-bookworm AS builder
WORKDIR /src

COPY . .
ENV SQLX_OFFLINE=true
RUN cargo build --release --locked


FROM mcr.microsoft.com/dotnet/runtime:${DOTNET_TAG} AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN useradd --system --home-dir /app --shell /usr/sbin/nologin cerberus && \
    mkdir -p /app/instances && chown -R cerberus:cerberus /app

COPY --from=builder /src/target/release/cerberus /usr/local/bin/cerberus

USER cerberus
EXPOSE 5000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:5000/health || exit 1

ENTRYPOINT ["/usr/local/bin/cerberus"]
CMD ["daemon"]
