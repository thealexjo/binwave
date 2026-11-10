# Installation and setup

A step-by-step guide to installing binwave on a server and replicating from MySQL (including from a replica) into a separately deployed ClickHouse.

## 0. What you need

- A server for binwave: one static binary or Docker, with no special resource requirements (buffers are limited by the config, the `buffer` section).
- Network access from this server to the MySQL source (usually 3306) and to ClickHouse (usually 9000/9440).
- MySQL 8.0.36+ (the 8.0 line) or 8.4 LTS. The service does not accept other versions — exit code 78 at start.
- ClickHouse 25.8 or 26.8.

## 1. Source: checking the requirements

binwave connects to the MySQL server named in the config and checks the requirements on exactly that server. **If it is a replica, the requirements are checked on the replica, not on the primary** — this applies to all items below, including GTID and the binlog format.

```sql
SHOW VARIABLES LIKE 'log_bin';                    -- ON
SHOW VARIABLES LIKE 'log_slave_updates';           -- ON, when binwave reads from a replica
SHOW VARIABLES LIKE 'binlog_format';               -- ROW
SHOW VARIABLES LIKE 'binlog_row_image';            -- FULL
SHOW VARIABLES LIKE 'binlog_row_metadata';         -- FULL
SHOW VARIABLES LIKE 'gtid_mode';                   -- ON
SHOW VARIABLES LIKE 'enforce_gtid_consistency';    -- ON
SHOW VARIABLES LIKE 'binlog_row_value_options';    -- empty (no PARTIAL_JSON)
SHOW VARIABLES LIKE 'server_id';                   -- unique among all replicas of the source
SELECT VERSION();                                  -- 8.0.36+ or 8.4.x
```

The exact list of what the service itself checks at start is `internal/source/mysql/requirements.go`; on a mismatch the service does not start and names what is missing (exit code 78).

To confirm everything at once without a single change in the databases, after the config is written (step 5), use the command `binwave check --connect`.

## 2. MySQL user

```sql
CREATE USER 'binwave'@'%' IDENTIFIED BY '...';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'binwave'@'%';
```

Nothing else is needed: binwave never writes to MySQL (sessions are opened with `transaction_read_only=1`, so the server itself would reject a write), and the snapshot reads with plain `SELECT` statements in autocommit, without a long transaction.

`SELECT` can be narrowed to the specific databases/tables from `tables.include` if your access policy prefers that.

## 3. ClickHouse: database and privileges

```sql
CREATE DATABASE replica;  -- service database: the table with the replication position lives here

CREATE USER binwave IDENTIFIED BY '...';
GRANT CREATE TABLE, INSERT, SELECT, ALTER TABLE, DROP TABLE ON replica.* TO binwave;
```

If `tables.target_database` is not set, the target databases are named like the source databases and are created/checked lazily on the first write into each of them — then the privileges are needed there too (`INSERT`; with auto-DDL on, `CREATE TABLE` and `ALTER TABLE`; and with `ddl.mode: full`, also `DROP TABLE`, because `RENAME TABLE` in ClickHouse requires it on the source table). The simplest way is to grant the same privileges on all databases replication should go to, or to set `tables.target_database` right away so that everything goes into one database with the privileges already granted.

## 4. Code: getting and building it

The repository is needed only for the build — it is not part of the service contract. Do not put the sources into `/etc/binwave` (only the config goes there) or into `/var/lib/binwave` (the service `StateDirectory`, owned by the system user `binwave`; code does not belong there). After the build only one file stays on the host — `/usr/local/bin/binwave`; the sources can be deleted, or kept only for future updates; they do not affect the running service.

```bash
git clone https://github.com/thealexjo/binwave.git
cd binwave
```

The binary is static (`CGO_ENABLED=0`, already built into `make build`), with no dependency on glibc or on what it was built with — so it can be built **on any machine** with the required Go version (see `go.mod`), not necessarily on the host itself:

```bash
make build              # ./bin/binwave, for the OS/architecture of this machine
./bin/binwave version   # version/commit are picked up from git automatically (git describe)
```

Two equivalent options; they differ only in whether Go is needed on the host:

- **Build on your own machine or in CI, copy only the binary** — more compact and safer (production needs no Go, no git, no `make`; less that can break or be broken into). If the host is not the same OS/architecture (for example macOS → a Linux server), cross-compiling is just environment variables:
  ```bash
  uname -m                             # run on the host to find out the architecture: x86_64 → amd64, aarch64 → arm64
  GOOS=linux GOARCH=amd64 make build   # ./bin/binwave — for the host, not for your machine
  scp bin/binwave user@host:/tmp/binwave
  ssh user@host 'sudo install -m 0755 /tmp/binwave /usr/local/bin/binwave'
  ```
- **Build right on the host** — simpler for a one-off installation, but it needs the required Go version installed (`go.mod`):
  ```bash
  ssh user@host

  # if there is no Go yet:
  curl -fsSL https://go.dev/dl/go1.27.1.linux-amd64.tar.gz -o /tmp/go.tar.gz   # arm64 → go1.27.1.linux-arm64.tar.gz
  rm -rf /usr/local/go && tar -C /usr/local -xzf /tmp/go.tar.gz
  export PATH=$PATH:/usr/local/go/bin
  echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile

  git clone https://github.com/thealexjo/binwave.git && cd binwave
  make build
  sudo install -m 0755 bin/binwave /usr/local/bin/binwave
  ```

For Docker (step 6, option B), rebuilding is simplest right on the host: `docker build` needs a running Docker daemon anyway, and the host needs one already to run the container — a separate registry is not required unless it is already part of your infrastructure.

By the end of this step the host should have a ready `/usr/local/bin/binwave` (option A) or a built image (option B, `make docker-build IMAGE=...` from the clone on the host) — step 6 then simply refers to it.

## 5. Configuration

Take [`deploy/config.example.yaml`](../deploy/config.example.yaml) as the base — every commented-out value there is the default. Write the file (for example `binwave.yaml`) where you built the binary, or right on the host — it is needed only at installation time; in step 6 it is copied to its permanent place (`/etc/binwave/config.yaml`). For an installation on a replica these sections matter:

```yaml
source:
  mysql:
    host: <replica address>
    user: binwave
    password: "${BINWAVE_MYSQL_PASSWORD}"
    server_id: 4101              # unique among all replicas, including the existing ones

target:
  clickhouse:
    addresses: ["<address>:9000"]
    database: replica
    user: binwave
    password: "${BINWAVE_CLICKHOUSE_PASSWORD}"

replication:
  initial_position: snapshot     # pick up what is already on the replica, not only new changes
  snapshot:
    parallelism: 2               # do not load the replica more than needed
    max_rows_per_second: 0       # or limit it if the replica is already under load; changes on the fly:
                                  # binwave replication snapshot-rate N|unlimited
    chunk_size: 1000             # rows per chunk; with adaptive_chunks: false this is the size of every chunk,
                                  # otherwise only the initial one. Changes on the fly:
                                  # binwave replication chunk-size N|adaptive

tables:
  include: ["<db>.*"]
  on_replication_error: quarantine   # a problem table is set aside, the others keep replicating
  autoresync: true                    # what a resync resolves without an operator (a generated
                                       # column and the like) fixes itself; the rest stays in quarantine

observability:
  listen: 127.0.0.1:9187         # 0.0.0.0:9187 inside Docker; several instances on a host — different addresses
```

Secrets go only through `${ENV}` (a value from the process environment), not as text in the file.

### The standard place for the config

**`/etc/binwave/config.yaml`** is the default path built into the binary: without `--config`, all commands (`run`, `check`, `replication ...`, `tables ...`) use exactly this file. This guide uses it (step 6 puts the config exactly there), so from here on this guide never passes `--config` explicitly.

If the host has several sources, each has its own process and its own config: then the default path does not fit (there is one per host) and each instance needs an explicit `--config /etc/binwave/<instance>.yaml` — this is exactly the difference between the template `binwave@.service` and the plain `binwave.service` (section 6).

## 6. Installing binwave

Two equivalent, equally compact options — choose by what is already the practice on the server.

### Option A: binary + systemd

Here there is one source, so the simpler unit without an instance name fits — `binwave.service`, with the default config `/etc/binwave/config.yaml` (if there is more than one source, use the template `binwave@.service` with an explicit `/etc/binwave/<instance>.yaml` for each).

`/usr/local/bin/binwave` (step 4) and `binwave.yaml` (step 5) are already in place; from the repository clone on the host you also need the unit file:

```bash
useradd --system --no-create-home --shell /usr/sbin/nologin binwave

install -m 0644 deploy/systemd/binwave.service /etc/systemd/system/
install -d -o root -g binwave -m 0750 /etc/binwave
install -m 0640 -o root -g binwave binwave.yaml /etc/binwave/config.yaml   # the file from step 5

systemctl daemon-reload
```

Do not start it right away — check first (step 7).

### Option B: Docker

```bash
make docker-build IMAGE=binwave:local
```

The config is mounted as a file at `/etc/binwave/config.yaml`; secrets come through environment variables, which the config substitutes itself as `${ENV}`. A compose example is `deploy/docker/compose.example.yaml`. The image is distroless, non-root, without a shell — CLI commands (`replication status` and the like) run through `docker exec` with the same binary inside the container.

Both options have the same contract: a foreground process, logs only to stdout, `SIGTERM` — graceful shutdown, exit code `78` — configuration error.

## 7. Checking before the start

Without changing anything in the databases — the same thing the service does at start and right after it:

```bash
binwave check --print     # config validation, secrets masked
binwave check --connect   # + connections, source requirements, privileges, table checks
```

Exit code: `0` — all is well, `78` — the configuration does not fit these databases, `1` — a database is unavailable.

## 8. Starting and watching

```bash
systemctl enable --now binwave
journalctl -u binwave -f
```

- `binwave replication status` — the state (`ready`/`paused`/`failed`), GTID position, lag, the number of tables and of quarantined ones;
- `binwave tables list` — what went to quarantine and why; `release --accept-data-gap <db.table>` returns it as it is (a table without a key or with a cascading foreign key is checked again before its next row and goes back to quarantine while the cause is there), `resync <db.table>` reads the table again (the service must be running);
- `curl 127.0.0.1:9187/status` — the same in JSON; row values never appear there (only on the control socket);
- `curl 127.0.0.1:9187/metrics` — Prometheus; the key metrics at start: `binwave_lag_seconds`, `binwave_degraded`, `binwave_quarantined_tables`, `binwave_replication_failed`, `binwave_autoresync_total`/`binwave_autoresync_failed_total`; collection and the dashboard are below.

Readiness (`READY=1`, `binwave_ready=1`) means that both databases accepted the connection, the requirements and privileges are checked, and the source accepted the binlog stream — it does not yet mean that all tables are validated: that check runs right after and may put some table into quarantine or (with `on_replication_error: stop`) stop the service with code 78 — watch the first minutes of the log and `tables list`.

### Metrics and dashboard

`/metrics` is served without authentication on `observability.listen` (by default `127.0.0.1:9187`, local only). You can collect them with what you already have: Prometheus or PMM 2/3.

**PMM.** The host with binwave already runs `pmm-agent`: it collects the metrics locally itself, so the default address does not need to change.

```bash
pmm-admin add external --service-name=binwave --listen-port=9187 --metrics-path=/metrics
pmm-admin list    # binwave is in the list, status Running
```

**Prometheus** on another host must reach the port: set `observability.listen: 0.0.0.0:9187` and close the port with a firewall for everyone except Prometheus (it has no authentication).

```yaml
scrape_configs:
  - job_name: binwave
    scrape_interval: 15s
    static_configs:
      - targets: ['replica-host:9187']
```

**Dashboard** — `deploy/grafana/binwave.json`: Grafana → Dashboards → New → Import → upload the file, choose the data source (in PMM — `Metrics`, in Prometheus — your own). In PMM the `instance` label is the agent ID, so at the top of the dashboard, in "Instance by", choose `node_name` (the host name) and save the dashboard (Save dashboard): the choice becomes the default. Tested on Grafana 12; on older versions (PMM 2 ships with Grafana 9) the import works, and minor panel settings that do not exist there yet are ignored. What to look at:

- **State** — whether the service is replicating, `degraded` / `failed` / pause, quarantined tables, lag.
- **Throughput** and **ClickHouse writes** — speed, batch fill, inserts in flight against `buffer.insert_parallelism`: shows where the bottleneck is — ClickHouse or reading the stream.
- **Initial load and resyncs** — the share of loaded tables and the breakdown by state; a growing `waiting_for_stream` means they are waiting for the stream, not for reading.
- **Data integrity and source** — replaced values, rows dropped by quarantine, binlog reconnects, skipped transactions and DDL.

During the initial load a large lag is normal: the stream waits behind the snapshot reads. For that time, lag alerts should be silenced by `binwave_initial_load_covered_tables > 0`.

## 9. If something went wrong

- **A table in quarantine** is not an outage: the others keep replicating. `tables list` shows the reason and `recoverable: true/false`; with `autoresync: true`, recoverable reasons (a generated column, a default expression, a type that MySQL recalculates) resolve themselves — this is also visible in the metrics (`binwave_autoresync_total`) and in the log with details, even if nobody did any operations by hand.
- **`failed`** (`binwave_replication_failed=1`, `/readyz` → 503) — a state that does not happen when the source and ClickHouse agree; the service is alive, `replication status` shows the failed transaction and the reason, `replication resume` retries or skips (`--skip-transactions`/`--gtid-next`).
- **Exit code 78** — configuration or source requirements; systemd does not restart (`RestartPreventExitStatus=78`), the message in the log names the reason.
- To stop the service without losing the position — `binwave replication pause`; to come back to it later — `resume` from the same place (the source must still keep the required binlogs; watch `binwave_paused`/`paused_since` if the pause turns out to be long).

## 10. Next

The full end-to-end test run (`make test-integration test-e2e`) does not belong here — it is for the code, not for a specific installation. For the installation itself, the readiness guide is a stable `binwave_lag_seconds`, no growth in `binwave_quarantined_tables`, and matching sample data between the source and ClickHouse (a manual comparison: there is no dedicated command for it yet).
