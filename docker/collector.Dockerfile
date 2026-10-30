# hftbacktest collector, pinned and patched. Build context: repo root.
#   docker build -f docker/collector.Dockerfile -t l2tape-collector .
#   docker run --rm -v ./data:/data l2tape-collector /data/raw/bybit bybit btcusdt ethusdt

FROM rust:1.99-trixie AS build
# Last upstream commit as of 2026-10-30; upstream does not track Cargo.lock, ours pins deps.
ARG HFTBACKTEST_SHA=5f3ec40b2afb764e0fea112f941ed85523ef4e88
WORKDIR /src
RUN git init -q . \
 && git fetch -q --depth 1 https://github.com/nkaz001/hftbacktest.git "$HFTBACKTEST_SHA" \
 && git checkout -q FETCH_HEAD
COPY docker/collector.patch docker/collector.Cargo.lock /tmp/
RUN git apply /tmp/collector.patch \
 && cp /tmp/collector.Cargo.lock Cargo.lock \
 && cargo build --release --locked -p collector

FROM debian:trixie-slim
# native-tls links OpenSSL; CA certificates for wss:// and https://.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libssl3t64 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/collector /usr/local/bin/collector
ENV RUST_LOG=info
# The collector only finishes its gzip files on Ctrl-C; docker's default SIGTERM would
# kill it mid-stream and leave a truncated .gz.
STOPSIGNAL SIGINT
# The collector does not create its output directory; first arg is that directory.
ENTRYPOINT ["sh", "-c", "mkdir -p \"$0\" && exec collector \"$0\" \"$@\""]
