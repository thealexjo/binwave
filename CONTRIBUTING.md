# Contributing to binwave

Thank you for helping. This page says how to build and test binwave, and what a change needs before it is merged. Bugs and ideas go to [issues](https://github.com/thealexjo/binwave/issues); security issues go to [SECURITY.md](SECURITY.md), not to a public issue.

## Setup

- Go, the version in [`go.mod`](go.mod).
- Docker, for the integration and end-to-end tests: they start their own MySQL and ClickHouse containers. Nothing else is installed or reached.
- `make` and `git`.

The linter and the other tools are installed into `./bin` in the right version on first use.

```bash
git clone https://github.com/thealexjo/binwave.git && cd binwave
make build   # ./bin/binwave
make test    # unit tests, no Docker
```

## Make targets

| Target | What it does |
|---|---|
| `make build` | the binary, `./bin/binwave` |
| `make fmt` | formats the code (gofumpt, goimports) |
| `make lint` | golangci-lint with every build tag |
| `make test` | unit tests with `-race`, no Docker |
| `make test-integration` | integration tests against containers (Docker) |
| `make test-e2e` | end-to-end tests: the real binary against containers, failures made by a TCP proxy and signals (Docker, several minutes); `E2E_PARALLEL` tests at once, 4 by default, which a Docker host of 8 GB takes |
| `make test-all` | all three |
| `make fuzz` | every fuzz test, `FUZZTIME` each (default `30s`) |
| `make cover` | unit test coverage |
| `make vuln` | govulncheck over the dependencies and the standard library |
| `make docker-build` | the image `binwave:dev` (`IMAGE=...`) |
| `make verify-docker` | builds the image and smoke-tests it |
| `make verify-systemd` | checks the unit files in `deploy/systemd` with `systemd-analyze` |
| `make release-check` | validates `.goreleaser.yaml` |
| `make release-snapshot` | release archives into `./dist`, nothing published |

`make help` lists them.

## Tests

A change of behavior comes with tests in the same pull request, and a bug is first reproduced by a test that fails, then fixed.

- **Unit** tests live next to the code (`*_test.go`): table-driven, no network, no Docker. Generated DDL is compared with golden files in `testdata/`.
- **Fuzz** tests cover value conversion and DDL parsing.
- **Integration** tests are in `test/integration/` (build tag `integration`) and run against real servers from `test/testenv`. The servers come from a pool and are reset between tests (`test/testenv/pool.go`): a test must not rely on a new server, and must not change what the reset does not undo.
- **End-to-end** tests are in `test/e2e/` (build tag `e2e`): write to MySQL, run binwave, compare ClickHouse. Restarts, `kill -9` in the middle of a batch, outages, schema changes and changes during a snapshot belong here.

The default servers are MySQL 8.4 and ClickHouse 26.8. Other versions are chosen with variables, as CI does for its matrix:

```bash
BINWAVE_TEST_MYSQL_IMAGE=mysql:8.0.23 make test-integration
BINWAVE_TEST_CLICKHOUSE_IMAGE=clickhouse/clickhouse-server:25.8 make test-e2e
```

A single test: `go test -tags integration -run TestName ./test/integration/`.

Online schema migrations (`pt-online-schema-change`, `gh-ost`) are reproduced by the simulation in `test/migrationsim`, checked against the statements the real tools write (`test/migrationsim/testdata`). The real tools run only on request: `BINWAVE_TEST_REAL_TOOLS=1`, and `BINWAVE_TEST_GHOST_BIN` for gh-ost (see `test/e2e/migration_real_test.go`).

## No private data

The repository is public. Code, tests, examples and documentation contain no hosts, addresses, passwords, company or product names, and no real database schemas. Database and table names come only from a neutral set: `shop`, `warehouse`, `store`, `binwave`, `replica`, `db`, also in output copied from a running service. Output from a real server (`*.log`) is not committed.

## Code

- Errors are wrapped with context: `fmt.Errorf("flush table %s: %w", name, err)`. No `panic` in production code, except for states that cannot happen.
- Logging is `log/slog` only, structured. Secrets never get into logs, and neither do row values: logs and the metrics port name the key column, not its value.
- `context.Context` is the first argument of everything that does I/O or can block. Every goroutine has an owner and a way to stop.
- Packages depend on each other through small interfaces declared where they are used, which also keeps them testable without containers.
- Time and versions are injected: no `time.Now()` in logic, so tests are deterministic.
- All code lives in `internal/`; there is no public Go API.
- A comment is one line next to a one-line entry, and says what the thing is. Why a change was made belongs in the commit message.
- Do not write `''` in Go doc comments: gofmt turns it into a typographic quote.

### Logs and error codes

A log record or a saved reason is short and carries a code from `internal/errcode`: the code is the kind of reason, the detail only the variable part (which column, which type). The table is a separate attribute, not part of the text. A code is broad, one per kind of reason; the full explanation and what to do are written once, in [`docs/reference/error-codes.md`](docs/reference/error-codes.md).

A new code is a constant in `internal/errcode` and a row in `docs/reference/error-codes.md`; `TestEveryCodeIsDocumented` checks that every code has one. Usually an existing code with a new detail is enough.

### A new setting

1. The default in `internal/config/defaults.go`, the validation in `internal/config/validate.go`.
2. A test for the default: the golden file `internal/config/testdata/minimal.effective.golden.yaml`, updated with `go test ./internal/config -update`, and a test for an invalid value.
3. The setting with its default in [`deploy/config.example.yaml`](deploy/config.example.yaml), which a test keeps valid.

### A new type mapping

A row in `TestResolveDefaultTypes` or `TestResolveOverrides` (`internal/typemap/mapping_test.go`), boundary values in `internal/typemap/convert_test.go`, and a fuzz test when a value is parsed from text. A value is converted exactly or by a documented rule, never silently changed; `DECIMAL` never goes through a float.

## Pull requests

- One subject per pull request; a large change is better discussed in an issue first.
- Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/), in English: `fix(sink): ...`, `feat(config): ...`, `docs: ...`, `test: ...`, `build: ...`, `ci: ...`.
- Before you open it: `make fmt lint test` is green. A change in the source, the sink or the pipeline also runs the integration and end-to-end tests it affects: `go test -tags integration -run '...' ./test/integration/`, and the same with `-tags e2e` for `./test/e2e/`.
- The documentation changes with the behavior: the README, `docs/deployment.md`, `docs/reference/error-codes.md`, `deploy/config.example.yaml`.
- CI runs the whole matrix of supported versions; a maintainer may ask for a test on another version.

By contributing you agree that your contribution is licensed under [Apache-2.0](LICENSE).
