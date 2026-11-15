# Security policy

## Supported versions

Security fixes go into the latest release line: until 1.0, the latest `0.x` minor version. Upgrade to its latest patch release before you report.

## Reporting a vulnerability

Please do not open a public issue. Report it privately through GitHub: the **Security** tab of this repository → **Report a vulnerability**.

Include what you can of:

- the binwave version (`binwave version`), the MySQL or Percona Server version and the ClickHouse version;
- how binwave runs: systemd or Docker, and the relevant part of the configuration, with secrets removed (`binwave check --print` masks them);
- what an attacker can do, and the steps to reproduce it;
- whether it is already known publicly.

binwave is maintained by one person. You can expect an acknowledgement within a week, and a fix or a plan as soon as the issue is understood. You will be credited in the release notes unless you ask not to be.

## What binwave protects

- **Secrets.** Passwords are passed to the configuration as `${ENV}` variables and are masked in `binwave check --print`; they never appear in logs.
- **Row values.** The values of replicated rows never go to logs, to `/status` or to metrics; those name the key column only. Values appear only on the control socket.
- **The control socket.** It is a Unix socket in the runtime directory (`/run/binwave/binwave.sock` by default), created with mode `0600`: only the user binwave runs as (and root) can pause, resume or resynchronize replication.
- **The metrics port** (`/metrics`, `/healthz`, `/readyz`, `/status`) has no authentication and listens on `127.0.0.1:9187` by default. Do not expose it beyond a trusted network.
- **MySQL.** binwave needs only `REPLICATION SLAVE`, `REPLICATION CLIENT` and `SELECT`, and never writes to the source.

A way around any of these is a vulnerability; please report it as above.
