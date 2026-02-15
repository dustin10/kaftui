# CLAUDE.md — kaftui

## Project Overview

KafTUI is a Rust TUI application for interacting with Apache Kafka. Built with `ratatui` for the UI and `rdkafka` for Kafka connectivity. Supports Schema Registry integration (Avro, Protobuf, JSONSchema), JSONPath filtering, profiles, and custom themes.

## Build & Test

```bash
cargo build           # Debug build
cargo build --Release # Release build
cargo fmt             # Format code
cargo clippy          # Lint
cargo test            # Test
```

Rust edition 2024, MSRV 1.89.

## Project Structure

```
src/
├── main.rs          # Entry point, CLI (clap), event loop
├── event.rs         # Event definitions and EventBus (tokio channels)
├── trace.rs         # Tracing/logging layer
├── util.rs          # Env var and file utilities
├── app/
│   ├── mod.rs       # application state, notification system
│   ├── config.rs    # Config parsing, profiles, persisted config (~/.kaftui.json)
│   └── export.rs    # Record and schema export to disk
├── kafka/
│   ├── mod.rs       # Consumer, Record, format definitions
│   ├── de.rs        # Deserializer traits
│   ├── schema.rs    # Schema Registry client
│   └── admin.rs     # Admin client (topic discovery, config)
└── ui/
    ├── mod.rs       # Component trait, main render dispatch
    ├── records.rs   # Records screen
    ├── topics.rs    # Topics browser screen
    ├── stats.rs     # Consumption statistics screen
    ├── schemas.rs   # Schema Registry browser screen
    ├── settings.rs  # Settings display screen
    ├── logs.rs      # Log viewer screen
    └── widget.rs    # Shared widget implementations
```

## Architecture & Conventions

- **Async runtime**: tokio. Background tasks use `tokio::spawn()` — never block the main loop with `.await` on long operations.
- **Error handling**: `anyhow::Result<T>`, `anyhow::bail!()`, `.context()` for wrapping. Log errors with `tracing::error!()`.
- **UI pattern**: All screens implement the `Component` trait (render, key handling, event processing).
- **Deserialization**: Trait-based (`KeyDeserializer`/`ValueDeserializer`) for pluggable format support.
- **Config**: Hierarchical merging — defaults → profile → CLI args. Persisted config at `~/.kaftui.json`.
- **Naming**: Standard Rust conventions — snake_case functions, PascalCase types, SCREAMING_SNAKE_CASE constants.

