package config

import (
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite golden files")

func noEnv(string) (string, bool) { return "", false }

func mapEnv(m map[string]string) EnvLookup {
	return func(name string) (string, bool) {
		v, ok := m[name]
		return v, ok
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustParse(t *testing.T, yamlText string, env EnvLookup) *Config {
	t.Helper()
	cfg, err := Parse([]byte(yamlText), env)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	return cfg
}

const minimal = `
source: {mysql: {host: h, user: u, server_id: 1}}
target: {clickhouse: {addresses: ["h:9000"], database: replica}}
tables: {include: ["shop.*"]}
`

// TestMinimalEffectiveConfig pins every default through a golden file of the effective config.
func TestMinimalEffectiveConfig(t *testing.T) {
	cfg, err := Parse(readFile(t, "testdata/minimal.yaml"), noEnv)
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}

	got, err := cfg.RedactedYAML()
	if err != nil {
		t.Fatal(err)
	}

	golden := filepath.Join("testdata", "minimal.effective.golden.yaml")
	if *update {
		if err := os.WriteFile(golden, got, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if want := readFile(t, golden); string(got) != string(want) {
		t.Errorf("effective config mismatch (run go test ./internal/config -update)\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestDefaults(t *testing.T) {
	cfg := mustParse(t, minimal, noEnv)

	checks := []struct {
		name      string
		got, want any
	}{
		{"mysql port", cfg.Source.MySQL.Port, DefaultMySQLPort},
		{"mysql tls", cfg.Source.MySQL.TLS.Mode, TLSPreferred},
		{"clickhouse user", cfg.Target.ClickHouse.User, "default"},
		{"clickhouse tls", cfg.Target.ClickHouse.TLS.Mode, TLSDisabled},
		{"initial position", cfg.Replication.InitialPosition, StartFromNow},
		{"checkpoint store", cfg.Checkpoint.Store, CheckpointClickHouse},
		{"checkpoint table", cfg.Checkpoint.Table, "binwave_state"},
		{"target database (empty: the same name as in the source)", cfg.Tables.TargetDatabase, ""},
		{"max rows", cfg.Buffer.MaxRows, 100_000},
		{"max bytes", cfg.Buffer.MaxBytes, 64 * MiB},
		{"flush interval", cfg.Buffer.FlushInterval, DefaultBufferFlush},
		{"catchup flush interval", cfg.Buffer.CatchupFlushInterval, DefaultBufferCatchupFlush},
		{"max memory", cfg.Buffer.MaxMemory, GiB},
		{"ddl mode", cfg.DDL.Mode, DDLFull},
		{"ddl dry run", cfg.DDL.DryRun, false},
		{"listen", cfg.Observability.Listen, "127.0.0.1:9187"},
		{"log level", cfg.Observability.LogLevel, "info"},
		{"log format", cfg.Observability.LogFormat, LogText},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestExplicitValuesOverrideDefaults(t *testing.T) {
	cfg := mustParse(t, `
source: {mysql: {host: h, user: u, server_id: 1, port: 3307, tls: {mode: required}}}
target: {clickhouse: {addresses: ["h:9000"], database: replica, user: ch}}
tables: {include: ["shop.*"], target_database: analytics}
buffer: {max_rows: 10, max_bytes: 1KiB, flush_interval: 250ms, max_memory: 2MiB}
ddl: {mode: additive, dry_run: true}
observability: {listen: "0.0.0.0:9999", log_level: debug, log_format: text}
`, noEnv)

	if cfg.Source.MySQL.Port != 3307 || cfg.Source.MySQL.TLS.Mode != TLSRequired {
		t.Errorf("mysql = %+v", cfg.Source.MySQL)
	}
	if cfg.Tables.TargetDatabase != "analytics" {
		t.Errorf("target database = %q", cfg.Tables.TargetDatabase)
	}
	if cfg.Buffer != (Buffer{MaxRows: 10, MaxBytes: KiB, FlushInterval: Duration(250 * time.Millisecond), CatchupFlushInterval: DefaultBufferCatchupFlush, MaxMemory: 2 * MiB, InsertParallelism: DefaultBufferInsertParallelism}) {
		t.Errorf("buffer = %+v", cfg.Buffer)
	}
	if cfg.DDL != (DDL{Mode: DDLAdditive, DryRun: true, OnMissingStructure: OnMissingStop}) {
		t.Errorf("ddl = %+v", cfg.DDL)
	}
	if cfg.Observability != (Observability{Listen: "0.0.0.0:9999", LogLevel: "debug", LogFormat: LogText}) {
		t.Errorf("observability = %+v", cfg.Observability)
	}
}

func TestDatabaseMapDefaultsToEmptyAndParsesExplicitEntries(t *testing.T) {
	cfg := mustParse(t, minimal, noEnv)
	if len(cfg.Tables.DatabaseMap) != 0 {
		t.Errorf("database map = %v, want empty by default", cfg.Tables.DatabaseMap)
	}

	cfg = mustParse(t, `
source: {mysql: {host: h, user: u, server_id: 1}}
target: {clickhouse: {addresses: ["h:9000"], database: replica}}
tables: {include: ["shop.*"], database_map: {shop: shop_mirror}}
`, noEnv)
	if want := map[string]string{"shop": "shop_mirror"}; !reflect.DeepEqual(cfg.Tables.DatabaseMap, want) {
		t.Errorf("database map = %v, want %v", cfg.Tables.DatabaseMap, want)
	}
}

func TestExampleConfigIsValid(t *testing.T) {
	env := mapEnv(map[string]string{
		"BINWAVE_MYSQL_PASSWORD":      "x",
		"BINWAVE_CLICKHOUSE_PASSWORD": "y",
	})
	cfg, err := Parse(readFile(t, "../../deploy/config.example.yaml"), env)
	if err != nil {
		t.Fatalf("deploy/config.example.yaml is invalid: %v", err)
	}
	if got := cfg.Tables.Overrides["shop.users"].Columns["is_vip"].Type; got != "Bool" {
		t.Errorf("override type = %q, want Bool", got)
	}
}

func TestDemoConfigIsValid(t *testing.T) {
	if _, err := Parse(readFile(t, "../../deploy/docker/demo/config.yaml"), noEnv); err != nil {
		t.Fatalf("deploy/docker/demo/config.yaml is invalid: %v", err)
	}
}

func TestFileCheckpointPathFromStateDirectory(t *testing.T) {
	const doc = minimal + "checkpoint: {store: file}\n"

	cfg := mustParse(t, doc, mapEnv(map[string]string{StateDirectoryEnv: "/var/lib/binwave/main:/other"}))
	if want := "/var/lib/binwave/main/checkpoint.json"; cfg.Checkpoint.FilePath != want {
		t.Errorf("file_path = %q, want %q", cfg.Checkpoint.FilePath, want)
	}

	_, err := Parse([]byte(doc), noEnv)
	assertProblem(t, err, "checkpoint.file_path: is required")
}

func TestRetryDefaultsAndOverrides(t *testing.T) {
	if got := time.Duration(mustParse(t, minimal, noEnv).Retry.OverloadBudget); got != 10*time.Minute {
		t.Errorf("default overload budget = %v, want 10m", got)
	}
	cfg := mustParse(t, minimal+"retry: {overload_budget: 90s}\n", noEnv)
	if got := time.Duration(cfg.Retry.OverloadBudget); got != 90*time.Second {
		t.Errorf("overload budget = %v, want 90s", got)
	}
}

func TestIntervalDefaultsAndOverrides(t *testing.T) {
	cfg := mustParse(t, minimal, noEnv)
	got := []time.Duration{
		time.Duration(cfg.Retry.InitialDelay), time.Duration(cfg.Retry.MaxDelay),
		time.Duration(cfg.Retry.AttemptTimeout), time.Duration(cfg.Source.MySQL.HeartbeatPeriod),
	}
	want := []time.Duration{500 * time.Millisecond, 30 * time.Second, 2 * time.Minute, 5 * time.Second}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("default %d = %v, want %v", i, got[i], want[i])
		}
	}

	doc := "source: {mysql: {host: h, user: u, server_id: 1, heartbeat_period: 1s}}\n" +
		"target: {clickhouse: {addresses: [\"h:9000\"], database: replica}}\n" +
		"tables: {include: [\"shop.*\"]}\n" +
		"retry: {initial_delay: 100ms, max_delay: 1s, attempt_timeout: 15s}\n"
	c := mustParse(t, doc, noEnv)
	if time.Duration(c.Retry.InitialDelay) != 100*time.Millisecond || time.Duration(c.Retry.MaxDelay) != time.Second ||
		time.Duration(c.Retry.AttemptTimeout) != 15*time.Second || time.Duration(c.Source.MySQL.HeartbeatPeriod) != time.Second {
		t.Errorf("overrides not applied: %+v %+v", c.Retry, c.Source.MySQL)
	}
}

func TestStartupBudgetAndCascadePolicyDefaults(t *testing.T) {
	cfg := mustParse(t, minimal, noEnv)
	if got := time.Duration(cfg.Retry.StartupBudget); got != 10*time.Minute {
		t.Errorf("default startup budget = %v, want 10m", got)
	}
	if cfg.DDL.OnMissingStructure != OnMissingStop {
		t.Errorf("default missing structure policy = %q, want stop: nothing is lost by default", cfg.DDL.OnMissingStructure)
	}
	if cfg.Tables.OnReplicationError != OnErrorQuarantine {
		t.Errorf("default replication error policy = %q, want quarantine: one table must not stop the others", cfg.Tables.OnReplicationError)
	}
	if cfg.Tables.ForeignKeyCascades != CascadesError {
		t.Errorf("default cascade policy = %q, want error: a cascade silently leaves stale child rows, so it must be acknowledged", cfg.Tables.ForeignKeyCascades)
	}

	custom := mustParse(t, minimal+"retry: {startup_budget: 2m}\n", noEnv)
	if got := time.Duration(custom.Retry.StartupBudget); got != 2*time.Minute {
		t.Errorf("startup budget = %v, want 2m", got)
	}
	for _, policy := range []string{"error", "warn", "ignore"} {
		doc := "source: {mysql: {host: h, user: u, server_id: 1}}\ntarget: {clickhouse: {addresses: [\"h:9000\"], database: replica}}\ntables: {include: [\"shop.*\"], foreign_key_cascades: " + policy + "}\n"
		if got := mustParse(t, doc, noEnv).Tables.ForeignKeyCascades; string(got) != policy {
			t.Errorf("policy %q parsed as %q", policy, got)
		}
	}
}

func TestEnvExpansion(t *testing.T) {
	env := mapEnv(map[string]string{
		"HOST":     "db.internal",
		"PORT":     "3307",
		"PASSWORD": `p@ss"word: {x} $y`,
		"NUMERIC":  "12345",
	})
	cfg := mustParse(t, `
source:
  mysql:
    host: ${HOST}
    port: ${PORT}
    user: u-${HOST}-$${LITERAL}
    password: "${PASSWORD}"
    server_id: 1
target:
  clickhouse:
    addresses: ["${HOST}:9000"]
    database: replica
    password: "${NUMERIC}"
tables: {include: ["shop.*"]}
`, env)

	m := cfg.Source.MySQL
	if m.Host != "db.internal" || m.Port != 3307 {
		t.Errorf("host/port = %q/%d", m.Host, m.Port)
	}
	if m.User != "u-db.internal-${LITERAL}" {
		t.Errorf("user = %q, want escaped literal kept", m.User)
	}
	if m.Password != `p@ss"word: {x} $y` {
		t.Errorf("password = %q, special characters must survive expansion", m.Password)
	}
	if cfg.Target.ClickHouse.Password != "12345" {
		t.Errorf("quoted numeric password = %q", cfg.Target.ClickHouse.Password)
	}
	if cfg.Target.ClickHouse.Addresses[0] != "db.internal:9000" {
		t.Errorf("address = %q", cfg.Target.ClickHouse.Addresses[0])
	}
}

func TestEnvExpansionErrors(t *testing.T) {
	tests := map[string]string{
		"unset variable": "source:\n  mysql:\n    host: ${NOPE}\n",
		"invalid name":   "source:\n  mysql:\n    host: \"${1BAD}\"\n",
	}
	for name, doc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(doc), noEnv)
			if err == nil || !strings.Contains(err.Error(), "expand config") {
				t.Errorf("Parse() error = %v, want expansion error", err)
			}
		})
	}
}

func TestEnvNotExpandedInKeys(t *testing.T) {
	doc := strings.Replace(minimal, `tables: {include: ["shop.*"]}`, `tables: {include: ["shop.*"], overrides: {"${X}.t": {}}}`, 1)

	_, err := Parse([]byte(doc), mapEnv(map[string]string{"X": "shop"}))
	assertProblem(t, err, `tables.overrides.${X}.t: key must be "database.table"`)
}

func TestUnknownKeysRejected(t *testing.T) {
	doc := strings.Replace(minimal, `include: ["shop.*"]`, `include: ["shop.*"], exlude: ["shop.tmp"]`, 1)

	_, err := Parse([]byte(doc), noEnv)
	if err == nil || !strings.Contains(err.Error(), "exlude") {
		t.Fatalf("Parse() error = %v, want unknown field error", err)
	}
}

func TestParseErrors(t *testing.T) {
	tests := map[string]string{
		"empty":        "",
		"not yaml":     "source: [",
		"bad size":     minimal + "buffer: {max_bytes: 10XB}\n",
		"bad duration": minimal + "buffer: {flush_interval: soon}\n",
		"wrong type":   "source: {mysql: {port: abc}}",
	}
	for name, doc := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := Parse([]byte(doc), noEnv); err == nil {
				t.Error("Parse() succeeded, want error")
			}
		})
	}
}

func TestValidationProblems(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		want string
	}{
		{"missing host", `source: {mysql: {user: u, server_id: 1}}`, "source.mysql.host: is required"},
		{"missing user", `source: {mysql: {host: h, server_id: 1}}`, "source.mysql.user: is required"},
		{"missing server id", `source: {mysql: {host: h, user: u}}`, "source.mysql.server_id: is required"},
		{"bad port", `source: {mysql: {host: h, user: u, server_id: 1, port: 70000}}`, "source.mysql.port: must be between 1 and 65535"},
		{"bad tls mode", `source: {mysql: {host: h, user: u, server_id: 1, tls: {mode: maybe}}}`, `source.mysql.tls.mode: must be one of disabled, preferred, required, verify-full, got "maybe"`},
		{"cert without key", `source: {mysql: {host: h, user: u, server_id: 1, tls: {cert_file: c}}}`, "source.mysql.tls: cert_file and key_file must be set together"},
		{"ca with tls disabled", `target: {clickhouse: {addresses: ["h:1"], database: d, tls: {mode: disabled, ca_file: ca}}}`, "target.clickhouse.tls: ca_file/cert_file are set but mode is disabled"},
		{"no addresses", `target: {clickhouse: {database: d}}`, "target.clickhouse.addresses: at least one address is required"},
		{"bad address", `target: {clickhouse: {addresses: ["nohost"], database: d}}`, `target.clickhouse.addresses[0]: invalid address "nohost"`},
		{"missing database", `target: {clickhouse: {addresses: ["h:1"]}}`, "target.clickhouse.database: is required"},
		{"on_drop no longer exists", `ddl: {on_drop: skip}`, "on_drop"},
		{"mode off no longer exists", `ddl: {mode: "off"}`, "ddl.mode: must be one of additive, full"},
		{"retry budget negative", `retry: {overload_budget: -1s}`, "retry.overload_budget: must be > 0"},
		{"startup budget negative", `retry: {startup_budget: -1s}`, "retry.startup_budget: must be > 0"},
		{"initial delay zero-ish", `retry: {initial_delay: -1s}`, "retry.initial_delay: must be > 0"},
		{"max delay below initial", `retry: {initial_delay: 2s, max_delay: 1s}`, "retry.max_delay: must be >= retry.initial_delay"},
		{"attempt timeout negative", `retry: {attempt_timeout: -1s}`, "retry.attempt_timeout: must be > 0"},
		{"heartbeat too short", `source: {mysql: {host: h, user: u, server_id: 1, heartbeat_period: 10ms}}`, "source.mysql.heartbeat_period: must be at least 100ms"},
		{"relative control socket", `control: {socket: run/x.sock}`, "control.socket: must be an absolute path"},
		{"control socket too long", "control: {socket: /" + strings.Repeat("a", 120) + "}", "control.socket: is 121 bytes long"},
		{"bad missing structure policy", `ddl: {on_missing_structure: wait}`, "ddl.on_missing_structure: must be one of stop, quarantine"},
		{"bad replication error policy", `tables: {include: ["a.*"], on_replication_error: ignore}`, "tables.on_replication_error: must be one of quarantine, stop"},
		{"bad cascade policy", `tables: {include: ["a.*"], foreign_key_cascades: sometimes}`, "tables.foreign_key_cascades: must be one of error, warn, ignore"},
		{"cluster not supported yet", `target: {clickhouse: {addresses: ["h:1"], database: d, cluster: main}}`, "target.clickhouse.cluster: ON CLUSTER and replicated engines are not supported yet"},
		{"clickhouse preferred tls", `target: {clickhouse: {addresses: ["h:1"], database: d, tls: {mode: preferred}}}`, "target.clickhouse.tls.mode: preferred is not supported for ClickHouse"},
		{"bad initial position", `replication: {initial_position: yesterday}`, "replication.initial_position: must be one of snapshot, now, gtid"},
		{"snapshot parallelism negative", `replication: {initial_position: snapshot, snapshot: {parallelism: -1}}`, "replication.snapshot.parallelism: must be at least 1, got -1"},
		{"snapshot rate limit negative", `replication: {snapshot: {max_rows_per_second: -1}}`, "replication.snapshot.max_rows_per_second: must not be negative, got -1"},
		{"chunk size negative", `replication: {snapshot: {chunk_size: -5}}`, "replication.snapshot.chunk_size: must be at least 1 row, got -5"},
		{"insert parallelism negative", `buffer: {insert_parallelism: -1}`, "buffer.insert_parallelism: must be at least 1, got -1"},
		{"catchup flush interval negative", `buffer: {catchup_flush_interval: -1s}`, "buffer.catchup_flush_interval: must be > 0"},
		{"catchup flush interval below the ordinary one", `buffer: {flush_interval: 2s, catchup_flush_interval: 1s}`, "buffer.catchup_flush_interval: must be >= buffer.flush_interval (2s), got 1s"},
		{"insert parallelism too high", `buffer: {insert_parallelism: 100}`, "buffer.insert_parallelism: must not be above 64, got 100"},
		{"chunk max bytes negative", `replication: {snapshot: {chunk_max_bytes: -1}}`, `invalid size "-1": must not be negative`},
		{"gtid missing", `replication: {initial_position: gtid}`, "replication.gtid: is required when initial_position is gtid"},
		{"gtid unexpected", `replication: {gtid: "uuid:1-5"}`, "replication.gtid: is only allowed when initial_position is gtid"},
		{"bad store", `checkpoint: {store: redis}`, "checkpoint.store: must be one of clickhouse, file"},
		{"bad checkpoint table", `checkpoint: {table: "a.b"}`, `checkpoint.table: must be a plain identifier, got "a.b"`},
		{"no include", `tables: {exclude: ["a.b"]}`, "tables.include: at least one pattern is required"},
		{"pattern without dot", `tables: {include: ["shop"]}`, `tables.include[0]: pattern must be "database.table"`},
		{"broken glob", `tables: {include: ["shop.[a"]}`, `tables.include[0]: invalid pattern "shop.[a"`},
		{"bad exclude", `tables: {include: ["a.*"], exclude: ["x.y.z"]}`, `tables.exclude[0]: pattern must be "database.table"`},
		{"bad target db", `tables: {include: ["a.*"], target_database: "my-db"}`, `tables.target_database: must be a plain identifier`},
		{"database map with target database", `tables: {include: ["a.*"], target_database: analytics, database_map: {shop: shop_mirror}}`, "tables.database_map: cannot be set together with tables.target_database"},
		{"bad database map key", `tables: {include: ["a.*"], database_map: {"my-db": mirror}}`, `tables.database_map: key must be a plain database name, got "my-db"`},
		{"bad database map value", `tables: {include: ["a.*"], database_map: {shop: "my-db"}}`, `tables.database_map.shop: must be a plain identifier, got "my-db"`},
		{"bad override key", `tables: {include: ["a.*"], overrides: {users: {}}}`, `tables.overrides.users: key must be "database.table"`},
		{"bad target table", `tables: {include: ["a.*"], overrides: {a.b: {target_table: "x y"}}}`, `tables.overrides.a.b.target_table: must be a plain identifier`},
		{"excluded and overridden", `tables: {include: ["a.*"], overrides: {a.b: {exclude_columns: [c], columns: {c: {type: String}}}}}`, "tables.overrides.a.b.columns.c: column is both excluded and overridden"},
		{"order by excluded", `tables: {include: ["a.*"], overrides: {a.b: {exclude_columns: [c], order_by: [c]}}}`, `tables.overrides.a.b.order_by: column "c" is excluded`},
		{"empty override type", `tables: {include: ["a.*"], overrides: {a.b: {columns: {c: {type: ""}}}}}`, "tables.overrides.a.b.columns.c.type: is required"},
		{"negative rows", `buffer: {max_rows: -1}`, "buffer.max_rows: must be > 0"},
		{"negative flush", `buffer: {flush_interval: -1s}`, "buffer.flush_interval: must be > 0"},
		{"memory below batch", `buffer: {max_bytes: 2GiB, max_memory: 1GiB}`, "buffer.max_memory: must be >= buffer.max_bytes (2GiB), got 1GiB"},
		{"bad ddl mode", `ddl: {mode: drop_everything}`, "ddl.mode: must be one of additive, full"},
		{"bad listen", `observability: {listen: "9187"}`, `observability.listen: invalid address "9187"`},
		{"bad log level", `observability: {log_level: loud}`, `observability.log_level: must be one of debug, info, warn, error, got "loud"`},
		{"bad log format", `observability: {log_format: xml}`, "observability.log_format: must be one of json, text"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.doc), noEnv)
			assertProblem(t, err, tt.want)
		})
	}
}

func TestValidationReportsAllProblemsAtOnce(t *testing.T) {
	_, err := Parse([]byte("ddl: {mode: nope}\n"), noEnv)

	var verr *ValidationError
	if !errors.As(err, &verr) {
		t.Fatalf("error = %v, want *ValidationError", err)
	}
	// host, user, server_id, addresses, database, include, ddl.mode
	if len(verr.Problems) < 7 {
		t.Errorf("got %d problems, want all of them reported:\n%v", len(verr.Problems), err)
	}
}

func TestRedactedHidesPasswords(t *testing.T) {
	cfg, err := Parse(readFile(t, "testdata/minimal.yaml"), noEnv)
	if err != nil {
		t.Fatal(err)
	}

	out, err := cfg.RedactedYAML()
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-mysql", "secret-ch"} {
		if strings.Contains(string(out), secret) {
			t.Errorf("redacted output leaks %q", secret)
		}
	}
	if cfg.Source.MySQL.Password != "secret-mysql" {
		t.Error("Redacted() must not modify the original config")
	}
	if empty := (&Config{}).Redacted(); empty.Source.MySQL.Password != "" {
		t.Error("empty password must stay empty so its absence stays visible")
	}
}

func assertProblem(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("got no error, want problem %q", want)
	}
	if !strings.Contains(err.Error(), want) {
		t.Errorf("error does not contain %q:\n%v", want, err)
	}
}

func TestAutostartAndControlSocket(t *testing.T) {
	cfg := mustParse(t, minimal, noEnv)
	if !cfg.Replication.AutostartEnabled() {
		t.Error("replication must start by itself by default")
	}
	if cfg.Control.Socket != DefaultControlSocket {
		t.Errorf("control socket = %q, want %s", cfg.Control.Socket, DefaultControlSocket)
	}

	off := mustParse(t, minimal+"replication: {autostart: false}\n", noEnv)
	if off.Replication.AutostartEnabled() {
		t.Error("autostart: false was ignored")
	}

	withState := mustParse(t, minimal, func(name string) (string, bool) {
		if name == StateDirectoryEnv {
			return "/var/lib/binwave", true
		}
		return "", false
	})
	if withState.Control.Socket != DefaultControlSocket {
		t.Errorf("control socket = %q with a state directory, want %s: the socket is not state", withState.Control.Socket, DefaultControlSocket)
	}

	custom := mustParse(t, minimal+"control: {socket: /run/binwave/x.sock}\n", noEnv)
	if custom.Control.Socket != "/run/binwave/x.sock" {
		t.Errorf("control socket = %q", custom.Control.Socket)
	}
}

// TestTheControlSocketIsReadWithoutTheSecrets: the commands that talk to a running service read the
// socket path only; the secrets of the rest of the file are not in their environment.
func TestTheControlSocketIsReadWithoutTheSecrets(t *testing.T) {
	secret := "source:\n  mysql:\n    password: ${MYSQL_PASSWORD}\n    unknown_key: 1\n"
	env := func(name string) (string, bool) {
		if name == "INSTANCE" {
			return "a", true
		}
		return "", false
	}
	for name, tt := range map[string]struct{ yaml, want string }{
		"default":            {secret, DefaultControlSocket},
		"set":                {secret + "control: {socket: /run/binwave/a/binwave.sock}\n", "/run/binwave/a/binwave.sock"},
		"from an env var":    {"control:\n  socket: /run/binwave/${INSTANCE}/binwave.sock\n", "/run/binwave/a/binwave.sock"},
		"empty is a default": {"control: {socket: \"\"}\n", DefaultControlSocket},
	} {
		got, err := ParseControlSocket([]byte(tt.yaml), env)
		if err != nil || got != tt.want {
			t.Errorf("%s: socket = %q, %v, want %q", name, got, err, tt.want)
		}
	}

	_, err := ParseControlSocket([]byte("control: {socket: binwave.sock}\n"), env)
	assertProblem(t, err, "control.socket: must be an absolute path")
	_, err = ParseControlSocket([]byte("control:\n  socket: /run/${NOPE}/binwave.sock\n"), env)
	assertProblem(t, err, "NOPE is not set")
	_, err = ParseControlSocket([]byte("control: [\n"), env)
	assertProblem(t, err, "parse config")
}
