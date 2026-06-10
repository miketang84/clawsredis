# clawsredis

A tiny Redis-like CLI key-value store written in Rust.

## Supported commands

- Strings: `GET`, `SET`, `SETNX`, `SETEX`, `MSET`, `MSETNX`, `MGET`, `APPEND`, `STRLEN`, `GETSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`
- Generic: `DEL`, `KEYS`, `TTL`, `EXPIRE`, `PERSISTKEY`, `PERSIST`
- Lists: `RPUSH`, `LPUSH`, `LRANGE`, `LLEN`
- Hashes: `HSET`, `HMSET`, `HGET`, `HMGET`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HGETALL`
- Sorted sets: `ZADD`, `ZCARD`, `ZSCORE`, `ZREM`, `ZRANGE`, `ZINCRBY`
- Pub/Sub (basic): `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`
- `QUIT`

Sorted set `ZRANGE` order is deterministic: `(score asc, member asc)`.

## RESP server mode

Run the Redis-compatible RESP server on an address of your choice:

```bash
cargo run -- --server 127.0.0.1:6379
```

You can combine server mode with persistence loading:

```bash
cargo run -- --server 127.0.0.1:6379 --load dump.bin
```

Manual `redis-cli` smoke check:

```bash
redis-cli -p 6379 SET foo bar
redis-cli -p 6379 GET foo
redis-cli -p 6379 DEL foo
redis-cli -p 6379 GET foo
```

Expected results are `OK`, `bar`, `(integer) 1`, and `(nil)`.
