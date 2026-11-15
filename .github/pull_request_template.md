## What and why

<!-- What the change does, and the problem it solves. Link the issue: Fixes #123. -->

## Tests

<!-- What you tested and how: the new or changed tests, the integration or e2e tests you ran. -->

## Checklist

- [ ] `make fmt lint test` is green
- [ ] a change in the source, the sink or the pipeline: the integration and e2e tests it affects pass
- [ ] a bug fix comes with a test that failed before the fix
- [ ] the documentation follows the change (README, `docs/deployment.md`, `deploy/config.example.yaml`)
- [ ] a new error code has a row in `docs/reference/error-codes.md`
- [ ] no hosts, passwords, company names or real schemas; table names from `shop`, `warehouse`, `store`, `binwave`, `replica`, `db`
