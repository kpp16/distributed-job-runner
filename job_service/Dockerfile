FROM rust:1.86-bullseye AS builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN cargo init

COPY Cargo.toml ./

RUN cargo build --release
RUN rm src/*.rs

COPY src/ ./src/

RUN touch src/main.rs
RUN cargo build --release

FROM debian:bullseye-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates libssl1.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/job_service /app/job_service

EXPOSE 3000

ENV CASS_URI=host.docker.internal:9042

CMD ["/app/job_service"]