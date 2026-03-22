# CLAUDE.md

## Build & Test Commands

```bash
# Build
cd rust && cargo build --release

# Run (Docker)
docker compose up

# Test
make test                          # or: cd redis-compat-rs && cargo test
```

## Project Structure

- **Stack**: Rust (Tokio async, LSM-tree storage)
- **Binary**: `forgekv`
- **Config**: `rust/forgekv.conf`

## Mistakes to Avoid

- Keep functions small and focused.
- Write tests for new functionality.
- Never commit secrets or credentials.

