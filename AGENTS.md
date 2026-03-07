# Repository Guidelines

## Project Structure & Module Organization

This Rust project implements a Redis-like key-value store with pub/sub support.

- `src/main.rs` — CLI application entry point with interactive command-line interface
- `src/lib.rs` — Core library with `KVStore` struct and all business logic
- `Cargo.toml` — Project manifest with dependencies: `serde`, `bincode`, `serde_with`

The codebase follows Rust 2021 edition conventions with unit tests embedded in `src/lib.rs`.

## Build, Test, and Development Commands

| Command | Purpose |
|---------|---------|
| `cargo build` | Compile in debug mode |
| `cargo build --release` | Compile optimized release build |
| `cargo test` | Run all unit and doc tests |
| `cargo clippy -- -D warnings` | Run Clippy linter with warnings as errors |
| `cargo fmt` | Format code per Rust style guide |
| `cargo check` | Quick compilation check without building artifacts |

## Coding Style & Naming Conventions

- Uses official Rust style (2-space indentation, snake_case for functions/variables, PascalCase for types)
- Linted with Clippy; enforce with `cargo clippy -- -D warnings`
- Formatted with `cargo fmt`
- Documentation comments (`///`) required for all public items

## Testing Guidelines

- Unit tests in `src/lib.rs` under `#[cfg(test)] mod tests`
- Tests cover core functionality: basic CRUD, TTL, expiration, persistence, pub/sub, thread safety
- Run all tests: `cargo test`
- No external test frameworks used; standard `std::test` harness

## Commit & Pull Request Guidelines

- Follow conventional commit format: `type: description`
- Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`
- Examples: `feat: add EXPIRE command`, `fix: correct TTL calculation`, `test: add thread safety test`
- All PRs must pass `cargo test`, `cargo clippy`, and `cargo fmt`
- Include test coverage for new features

## Security & Configuration Tips

- Serialization uses `bincode` for efficient binary encoding (no external deserialization risks)
- No sensitive data logging in CLI mode
- Persistence uses file I/O; validate paths before writing
