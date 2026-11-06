# streaming_algorithms

[![Crates.io](https://img.shields.io/crates/v/streaming_algorithms.svg?maxAge=86400)](https://crates.io/crates/streaming_algorithms)
[![MIT / Apache 2.0 licensed](https://img.shields.io/crates/l/streaming_algorithms.svg?maxAge=2592000)](#License)

[📖 Docs](https://docs.rs/streaming_algorithms) | [💬 Chat](https://constellation.zulipchat.com/#narrow/stream/213236-subprojects)

SIMD-accelerated implementations of various [streaming algorithms](https://en.wikipedia.org/wiki/Streaming_algorithm).

This library is a work in progress. PRs are very welcome! Currently implemented algorithms include:

 * Count–min sketch
 * Top k (Count–min sketch plus a doubly linked hashmap to track heavy hitters / top k keys when ordered by aggregated value)
 * HyperLogLog
 * Reservoir sampling

A goal of this library is to enable composition of these algorithms; for example Top k + HyperLogLog to enable an approximate version of something akin to `SELECT key FROM table GROUP BY key ORDER BY COUNT(DISTINCT value) DESC LIMIT k`.

The default build works on stable Rust. The optional `stdsimd` feature enables
portable SIMD for HyperLogLog unions and intersections and requires a nightly
compiler, because `std::simd` is still unstable. For an application using this
crate, select nightly explicitly:

```bash
RUSTFLAGS="-C target-cpu=native" cargo +nightly run --features "streaming_algorithms/stdsimd" --release
```

On stable, leave `stdsimd` disabled (including in dependencies that enable it).
There is no Cargo feature named `nightly`; `+nightly` selects the Rust toolchain.
To test this crate with SIMD, run `cargo +nightly test --all-features`.

See [this gist](https://gist.github.com/debasishg/8172796) for a good list of further algorithms to be implemented. Other resources are [Probabilistic data structures – Wikipedia](https://en.wikipedia.org/wiki/Category:Probabilistic_data_structures), [DataSketches – A similar Java library originating at Yahoo](https://datasketches.github.io/), and [Algebird  – A similar Java library originating at Twitter](https://github.com/twitter/algebird).

As these implementations are often in hot code paths, unsafe is used, albeit only when necessary to a) achieve the asymptotically optimal algorithm or b) mitigate an observed bottleneck.

## License
Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE.txt](LICENSE-APACHE.txt) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT.txt](LICENSE-MIT.txt) or http://opensource.org/licenses/MIT)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
