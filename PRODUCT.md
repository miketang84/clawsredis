# clawsredis Product Snapshot

clawsredis is a small Rust Redis-like key-value store with both an interactive CLI and a RESP2 TCP server mode. It is intended as a lightweight learning/utility implementation of common Redis data structures and command behavior, not a full Redis replacement.

## Current capabilities

- String commands: `GET`, `SET`, `SETNX`, `SETEX`, `MSET`, `MSETNX`, `MGET`, `APPEND`, `STRLEN`, `GETSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`.
- Generic key commands: `DEL`, `KEYS`, `TTL`, `EXPIRE`, `PERSISTKEY`, `PERSIST`.
- Lists: `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`.
- Hashes: `HSET`, `HMSET`, `HGET`, `HMGET`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HGETALL`.
- Sorted sets: `ZADD`, `ZCARD`, `ZSCORE`, `ZREM`, `ZRANGE`, `ZINCRBY`; `ZRANGE` is deterministic by score ascending, then member ascending.
- Basic pub/sub metadata commands: `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`.
- Optional bincode persistence via `PERSIST <path>` or startup `--load <path>`.
- RESP2 request parsing and response encoding for Redis-compatible clients.
- TCP server mode with one thread per client and a shared `Arc<Mutex<KVStore>>` store.

## Runtime modes

- Default: interactive CLI.
- Load persisted state: `cargo run -- --load dump.bin`.
- RESP server: `cargo run -- --server 127.0.0.1:6379`.
- Server with persistence loading: `cargo run -- --server 127.0.0.1:6379 --load dump.bin`.

## Architecture and conventions

- `src/lib.rs` owns the source-of-truth `KVStore`, command execution, and shared `RespValue` result type.
- `src/main.rs` is a thin CLI/server launcher and CLI formatter.
- `src/resp.rs` contains RESP2 parsing and encoding.
- `src/server.rs` contains the TCP listener/client loop.
- CLI and server commands both route through `execute_command(store, args)` so command behavior stays shared.
- Tests cover store behavior, RESP codec behavior, TCP handling, and server integration through `TcpStream`.
