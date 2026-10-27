# Release checklist: v0.1.0

Status legend: ✅ done and verified · ⬜ to do · 👤 needs the maintainer (an
outward-facing or irreversible step).

## Code and tests
- ✅ `cargo fmt --all --check`
- ✅ `cargo clippy --workspace --all-targets -- -D warnings`
- ✅ `cargo test --workspace`: all pass on Windows 11 (see `docs/TEST_REPORT.md`)
- ✅ Timing-sensitive suites repeated without flakes (see the test report)
- ⬜ CI green on GitHub Actions (Ubuntu + Windows). The workflow exists, but it
  has never run because there is no remote yet.
- ⬜ Verify the declared MSRV:
  `rustup toolchain install 1.85 && cargo +1.85 check --workspace`. It is
  declared but **unverified**; dependencies may require something newer.
  Raise `rust-version` if so.

## Demo and benchmarks
- ✅ `scripts/demo.ps1`: `relay demo` all checks pass, then the full test suite
- ✅ `scripts/demo-compose.ps1`: the ten steps against Docker Compose
- ✅ Benchmark results saved under `bench/results/` (produced by `relay bench`
  with the exact arguments `scripts/bench.ps1` passes; the wrapper script
  itself has not been run end to end yet), summarized
  in `bench/README.md` with environment and caveats
- ⬜ Re-run the benchmark on Linux and record it alongside (fsync costs differ by OS)

## Packaging
- ✅ License chosen: `MIT OR Apache-2.0`; `LICENSE-MIT` and `LICENSE-APACHE` at the root
- ✅ Crate metadata (license, authors, description, keywords, categories, readme)
- ✅ Path dependencies carry versions. `cargo package --workspace --allow-dirty`
  packages and verifies all five crates; `relay-server`'s tarball contains
  `migrations/0001–0003` and `build.rs`. **Caveat:** this works only with
  `publish = true`. With `publish = false`, cargo can't resolve the crates'
  dependencies on each other. So set it to `true` on the release branch
  before packaging (the repo keeps `false` to prevent an accidental
  `cargo publish`).
- ✅ Docker image builds from a clean context (`docker compose build`)
- ⬜ Add `repository` / `homepage` to `[workspace.package]` once the GitHub repo exists
- ⬜ Pin `ollama/ollama` in `compose.yaml` (currently `latest`; optional profile only)

## Release
- 👤 Create the GitHub repository and push
- 👤 Move the `[0.1.0] - unreleased` entry in `CHANGELOG.md` to a date
- 👤 Tag: `git tag -a v0.1.0 -m "RelayLLM 0.1.0"` and push the tag
- 👤 GitHub release: attach `relay` binaries (Windows: `target\release\relay.exe`;
  Linux: build in CI) and paste the CHANGELOG section
- 👤 Optional: publish to crates.io. Remove `publish = false`, then publish in
  dependency order: `relay-core`, `relay-client`, `relay-server`,
  `relay-worker`, `relay-cli`. The `relay-*` names may already be taken on
  crates.io; check first.
- 👤 Optional: push the Docker image to a registry
