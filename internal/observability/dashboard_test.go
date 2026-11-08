package observability

import (
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// dashboardPath is the Grafana dashboard shipped with the repository.
const dashboardPath = "../../deploy/grafana/binwave.json"

// metricName matches a metric of binwave or of the Go and process collectors in a PromQL
// expression; label names and values inside braces never start with these prefixes.
var metricName = regexp.MustCompile(`\b(?:binwave|go|process)_[a-z_]+\b`)

// onlyOurs keeps the go_* and process_* series scraped together with binwave's own.
const onlyOurs = `and on (instance, job) binwave_build_info{$instance_label=~"$instance"}`

// TestTheDashboardAsksOnlyForMetricsTheServiceExports: a renamed or removed metric leaves its panel
// empty without any error in Grafana, so the dashboard is checked against the registry here.
func TestTheDashboardAsksOnlyForMetricsTheServiceExports(t *testing.T) {
	raw, err := os.ReadFile(dashboardPath)
	if err != nil {
		t.Fatal(err)
	}
	var dash dashboard
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatalf("parse %s: %v", dashboardPath, err)
	}

	exported := exportedMetrics(t)
	queried := map[string]bool{}
	for _, p := range dash.panels() {
		for _, q := range p.Targets {
			if q.Datasource.UID != "${datasource}" {
				t.Errorf("panel %q: data source %q, want the ${datasource} variable (PMM and Prometheus name theirs differently)", p.Title, q.Datasource.UID)
			}
			if !strings.Contains(q.Expr, `$instance_label=~"$instance"`) {
				t.Errorf("panel %q: %s does not filter by the instance variables", p.Title, q.Expr)
			}
			for _, name := range metricName.FindAllString(q.Expr, -1) {
				queried[name] = true
				if !strings.HasPrefix(name, "binwave_") && !strings.Contains(q.Expr, onlyOurs) {
					t.Errorf("panel %q: %s is exported by every Go program (on a PMM node, by each exporter): add %q", p.Title, name, onlyOurs)
				}
				if !exported[name] {
					t.Errorf("panel %q asks for %s, which the service does not export", p.Title, name)
				}
			}
		}
	}
	if len(queried) == 0 {
		t.Fatal("the dashboard queries no metrics: the panels were not parsed")
	}

	// The label that tells instances apart defaults to instance, the one Prometheus always sets; PMM
	// replaces it with the ID of its agent, so node_name and service_name are offered too.
	label, ok := dash.variable("instance_label")
	if !ok {
		t.Fatal("the dashboard has no instance_label variable")
	}
	if label.Current.Value != "instance" || label.Query != "instance,node_name,service_name" {
		t.Errorf("instance_label: default %q, choices %q; want instance of instance,node_name,service_name", label.Current.Value, label.Query)
	}
	if v, ok := dash.variable("instance"); !ok || !strings.Contains(v.Definition, "$instance_label") {
		t.Errorf("the instance variable does not list the values of the chosen label: %+v", v)
	}

	// Every binwave metric is on the dashboard, so a new one is not forgotten there.
	var missing []string
	for name := range exported {
		if strings.HasPrefix(name, "binwave_") && !queried[name] && !notOnTheDashboard[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("metrics missing from %s (add a panel or list them in notOnTheDashboard): %v", dashboardPath, missing)
	}
}

// notOnTheDashboard are metrics deliberately left off the dashboard.
var notOnTheDashboard = map[string]bool{
	"binwave_position_version":        true, // the ReplacingMergeTree version of the saved position; its age says more
	"binwave_tables":                  true, // a number that changes only with the configuration
	"binwave_insert_duration_seconds": true, // queried through its _bucket, _sum and _count series
}

// exportedMetrics lists every series name the registry serves, including the histogram's
// _bucket, _sum and _count and the vectors, which appear only once they have a label value.
func exportedMetrics(t *testing.T) map[string]bool {
	t.Helper()
	h, _ := newTestHub()
	h.Retry("shop.orders", "overload", nil)
	h.adjustments.WithLabelValues("zero_date").Inc()

	families, err := h.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, f := range families {
		name := f.GetName()
		names[name] = true
		if f.GetType().String() == "HISTOGRAM" {
			for _, suffix := range []string{"_bucket", "_sum", "_count"} {
				names[name+suffix] = true
			}
		}
	}
	return names
}

type dashboard struct {
	Panels     []panel `json:"panels"`
	Templating struct {
		List []variable `json:"list"`
	} `json:"templating"`
}

type variable struct {
	Name       string `json:"name"`
	Definition string `json:"definition"`
	Query      any    `json:"query"` // a string for a custom variable, an object for a query one
	Current    struct {
		Value any `json:"value"`
	} `json:"current"`
}

func (d dashboard) variable(name string) (variable, bool) {
	for _, v := range d.Templating.List {
		if v.Name == name {
			return v, true
		}
	}
	return variable{}, false
}

type panel struct {
	Title   string  `json:"title"`
	Panels  []panel `json:"panels"` // the panels of a collapsed row
	Targets []struct {
		Expr       string `json:"expr"`
		Datasource struct {
			UID string `json:"uid"`
		} `json:"datasource"`
	} `json:"targets"`
}

// panels flattens the rows of the dashboard.
func (d dashboard) panels() []panel {
	var out []panel
	var walk func([]panel)
	walk = func(ps []panel) {
		for _, p := range ps {
			out = append(out, p)
			walk(p.Panels)
		}
	}
	walk(d.Panels)
	return out
}
