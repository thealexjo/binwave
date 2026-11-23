package pipeline

import (
	"regexp"
	"testing"
)

func TestARandomUUIDIsAVersion4UUIDInTheFormClickHouseShows(t *testing.T) {
	form := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for range 1000 {
		u := randomUUID()
		if !form.MatchString(u) {
			t.Fatalf("randomUUID() = %q", u)
		}
		if seen[u] {
			t.Fatalf("randomUUID() gave %s twice", u)
		}
		seen[u] = true
	}
}
