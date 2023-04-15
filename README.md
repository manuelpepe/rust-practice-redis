# Redis in Rust

A minimal Redis server written in Rust.

Made for practice following the [CodeCrafters Redis track](https://app.codecrafters.io/courses/redis?track=rust).


## Implemented Features

* Adheres to the [RESP Specification](https://redis.io/docs/reference/protocol-spec/)
* Handles clients concurrently
* Commands: 
   * `PING` 
   * `ECHO <message>`
   * `SET <key> <value> [PX <expiry>]`
   * `GET <key>`

## Usage:

Start the server with `cargo run`, connect to the server using `redis-cli`.