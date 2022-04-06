NSQ in Rust
===========

[NSQ](https://nsq.io/) client implementation in pure Rust.

## Features

* Pure Rust
* Async implementation based on [Futures 0.3](https://docs.rs/futures/0.3) and [Tokio 1.x](https://docs.rs/tokio/1)
* Low level
    * [x] Snappy
    * [x] Deflate
    * [ ] TLS
    * [x] AUTH
* High level
    * [ ] Producer(PUB)
        - [ ] [Tower](https://crates.io/crates/tower) based `Publish` producer
        - [ ] Tower based Discovery
        - [ ] Connection pool and/or load balance
    * [ ] Comsumer(SUB)
    * [ ] Sampling
    * [ ] Backoff

## Notes

Work in progress

## License

See [LICENSE-MIT](LICENSE-MIT).
