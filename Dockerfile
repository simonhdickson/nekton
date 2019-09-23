FROM rust:latest as cargo-build

RUN apt-get update

RUN apt-get install musl-tools -y

RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /usr/src/nekton

COPY Cargo.toml Cargo.toml

RUN mkdir src/

RUN echo "" > src/lib.rs

RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs

RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl

RUN rm -f target/x86_64-unknown-linux-musl/release/deps/nekton*
RUN rm -f target/x86_64-unknown-linux-musl/release/deps/libnekton*

COPY src src

RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl

FROM alpine:latest

RUN addgroup -g 1000 nekton

RUN adduser -D -s /bin/sh -u 1000 -G nekton nekton

WORKDIR /home/nekton/bin/

COPY --from=cargo-build /usr/src/nekton/target/x86_64-unknown-linux-musl/release/nekton_bin .
COPY ./config_examples/std.yml /nekton.yml

RUN chown nekton:nekton nekton_bin

USER nekton

ENTRYPOINT ["./nekton_bin"]

CMD ["-c", "/nekton.yml"]
