<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/binwave-mark-dark.svg">
  <img src="docs/assets/binwave-mark-light.svg" alt="binwave" height="64">
</picture>

# binwave

Real-time replication of MySQL changes (binlog, GTID) into ClickHouse.

## Development

Requirements: Go (version from `go.mod`), Docker for integration/e2e tests.

```bash
make help               # list targets
make test               # unit tests
make test-integration   # integration tests (ephemeral containers)
make lint
make build              # ./bin/binwave
```

Installation: [`docs/deployment.md`](docs/deployment.md). Error and warning codes: [`docs/reference/error-codes.md`](docs/reference/error-codes.md).
