FROM rust:latest AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release --features lua

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=build /app/target/release/forgekv .
COPY forgekv.conf ./forgekv.conf
VOLUME ["/data"]
EXPOSE 6379
ENTRYPOINT ["./forgekv", "--config", "/app/forgekv.conf"]
