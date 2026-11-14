# Matier

A simpler, faster HTTP client stack for Rust, named after Mount Matier near Pemberton, BC.

**Status: early development (Phase 0/1).** Not usable yet.

Why: hyper's HTTP/2 client keeps one connection per origin, and h2 serializes each connection behind a single mutex
([h2#531](https://github.com/hyperium/h2/issues/531)). High-throughput users end up pooling whole clients. Matier instead uses:

- a **single-owner connection actor**, so there is no connection-wide lock, and writes are batched into one `writev` per tick;
- **load- and capacity-aware multi-connection HTTP/2 pools**;
- **zero-copy bodies** via `bytesbuf`;
- **fetch-style composable layers**;
- **optional OpenTelemetry**, with queueing, transport and flow-control insight.

- Plan: [docs/PLAN.md](docs/PLAN.md)
- Research: [docs/research/](docs/research/)
- Design contracts: [docs/design/](docs/design/)
- Benchmarks: [crates/bench/README.md](crates/bench/README.md)

Platforms: Linux, Windows, macOS.

License: MIT.
