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
