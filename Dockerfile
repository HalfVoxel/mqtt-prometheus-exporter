FROM rust:1.90

WORKDIR /app
COPY . .

RUN cargo build --release && cp target/release/mqtt-exporter . && rm -rf target

ENTRYPOINT ["./mqtt-exporter"]