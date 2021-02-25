FROM rust as planner
WORKDIR naive-strategy
# We only pay the installation cost once, 
# it will be cached from the second build onwards
# To ensure a reproducible build consider pinning 
# the cargo-chef version with `--version X.X.X`
RUN cargo install cargo-chef 

COPY . .

RUN cargo chef prepare  --recipe-path recipe.json

FROM rust as cacher
WORKDIR naive-strategy
RUN cargo install cargo-chef
COPY --from=planner /naive-strategy/recipe.json recipe.json
RUN --mount=type=ssh cargo chef cook --release --recipe-path recipe.json

FROM rust as builder
WORKDIR naive-strategy
COPY . .
# Copy over the cached dependencies
COPY --from=cacher /naive-strategy/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN --mount=type=ssh cargo build --release --bin naive_strategy

FROM debian:buster-slim as runtime
WORKDIR naive-strategy
COPY --from=builder /naive-strategy/target/release/naive_strategy /usr/local/bin
ENV RUST_LOG=naive_strategy=debug
RUN apt-get update && apt-get -y install ca-certificates libssl-dev && rm -rf /var/lib/apt/lists/*
ENTRYPOINT ["/usr/local/bin/naive_strategy"]
