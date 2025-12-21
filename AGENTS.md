# Repository Guidelines

## Project Structure & Module Organization
- `Cargo.toml` defines the Rust crate metadata and dependencies.
- `src/main.rs` contains the current application entry point and core logic.
- No dedicated tests directory yet; add integration tests under `tests/` and unit tests in `src/` modules as the project grows.

## Build, Test, and Development Commands
- `cargo run` builds and runs the binary locally.
- `ELO_API_KEY=your_token ELO_HOST=127.0.0.1 ELO_PORT=3000 cargo run` runs the API server with the required auth key.
- `cargo build` compiles the project without running it.
- `cargo test` runs the test suite (currently none).
- `cargo fmt` formats Rust code using the standard formatter.
- `ELO_API_KEY=your_token python3 scripts/test_api.py` runs a quick API smoke test.

## API Examples
- `POST /nodes` with `{"id":"user:123"}` then `PUT /nodes/user:123/data` with `{"key":"type","value":"user"}`.
- `POST /nodes` with `{"id":"team:42"}` then `PUT /nodes/team:42/data` with `{"key":"type","value":"team"}`.
- `POST /edges` with `{"from":"user:123","to":"team:42"}` then `PUT /edges` with `{"from":"user:123","to":"team:42","key":"type","value":"owner"}`.
- `PATCH /nodes/user:123` with `{"data":{"rating":"850","status":"active"}}` updates multiple node fields.
- `PATCH /edges` with `{"from":"user:123","to":"team:42","data":{"role":"owner","since":"2025"}}` updates multiple edge fields.
- `GET /nodes?type=team` lists all team nodes.
- `GET /edges?type=owner` lists all owner edges.
- `GET /recommendations?start=...&type=team&num_key=rating&min=300&max=900&limit=10` returns scored recommendations.
- Curl example (create user):
  `curl -X POST http://127.0.0.1:3000/nodes -H "x-api-key: your_token" -H "content-type: application/json" -d '{"id":"user:123"}'`
- Curl example (set edge type):
  `curl -X PUT http://127.0.0.1:3000/edges -H "x-api-key: your_token" -H "content-type: application/json" -d '{"from":"user:123","to":"team:42","key":"type","value":"owner"}'`
- Curl example (bulk update node):
  `curl -X PATCH http://127.0.0.1:3000/nodes/user:123 -H "x-api-key: your_token" -H "content-type: application/json" -d '{"data":{"rating":"850","status":"active"}}'`
- Curl example (bulk update edge):
  `curl -X PATCH http://127.0.0.1:3000/edges -H "x-api-key: your_token" -H "content-type: application/json" -d '{"from":"user:123","to":"team:42","data":{"role":"owner","since":"2025"}}'`
- Curl example (fetch node):
  `curl http://127.0.0.1:3000/nodes/user:123 -H "x-api-key: your_token"`
- Curl example (list teams):
  `curl "http://127.0.0.1:3000/nodes?type=team" -H "x-api-key: your_token"`
- Curl example (list owner edges):
  `curl "http://127.0.0.1:3000/edges?type=owner" -H "x-api-key: your_token"`
- Curl example (path check):
  `curl "http://127.0.0.1:3000/path?from=user:123&to=team:42" -H "x-api-key: your_token"`
- Curl example (team recommendations):
  `curl "http://127.0.0.1:3000/recommendations?start=user:123&type=team&min=300&limit=5" -H "x-api-key: your_token"`

## Coding Style & Naming Conventions
- Use standard Rust formatting (4-space indentation) and keep code `rustfmt`-clean.
- Naming: `snake_case` for functions/variables, `CamelCase` for types/traits, and `SCREAMING_SNAKE_CASE` for constants.
- Prefer small, focused functions and explicit ownership/borrowing for clarity.

## Testing Guidelines
- Use Rust’s built-in test framework (`#[test]`).
- Unit tests should live next to the code they verify; integration tests go in `tests/`.
- Name tests descriptively, e.g., `exist_path_returns_true_when_connected`.
- Run tests with `cargo test` before opening a PR.

## Commit & Pull Request Guidelines
- Recent commits use short, plain-language messages (e.g., `add Node`, `start`).
- Keep commit subjects brief and action-oriented; include a scope if helpful.
- PRs should include a clear summary, test results (`cargo test`), and any relevant context or screenshots for behavior changes.

## Configuration Tips
- Rust edition is set to 2024 in `Cargo.toml`; keep tools updated accordingly.
