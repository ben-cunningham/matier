# Benchmark server for remote runs (Railway). Build context: repository root.
# Railway: set the service variable RAILWAY_DOCKERFILE_PATH=deploy/railway/Dockerfile, add a service domain
# (HTTPS edge) and a TCP proxy to $PORT (direct h1/h2c), then `railway up`. See bench/remote.toml.
FROM rust:1-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake clang perl pkg-config make \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p matier-bench-server

FROM debian:bookworm-slim
COPY --from=build /src/target/release/matier-bench-server /usr/local/bin/
ENV PORT=8080
EXPOSE 8080
# Plain HTTP/1.1 + h2c on $PORT. TLS is terminated by Railway's edge (HTTPS domain) or not used (TCP proxy).
CMD ["sh", "-c", "exec matier-bench-server --addr 0.0.0.0:${PORT} --tls-addr off --threads ${SERVER_THREADS:-2}"]
