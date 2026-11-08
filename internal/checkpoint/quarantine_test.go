package checkpoint

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestQuarantineRoundTripsAndIsCanonical(t *testing.T) {
	at := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	a := Quarantined{Table: "shop.a", Since: "u:1-3", Reason: "conversion error: value 1", At: at}
	b := Quarantined{Table: "shop.b", Since: "u:1-4", Reason: "schema mismatch", At: at}

	if got := EncodeQuarantine([]Quarantined{b, a}); got != EncodeQuarantine([]Quarantined{a, b}) {
		t.Error("the same set encodes differently depending on the order, so it would look changed on every save")
	}
	decoded, err := DecodeQuarantine(EncodeQuarantine([]Quarantined{b, a}))
	if err != nil || !reflect.DeepEqual(decoded, []Quarantined{a, b}) {
		t.Errorf("round trip = %+v, %v", decoded, err)
	}
	if EncodeQuarantine(nil) != "" || EncodeQuarantine([]Quarantined{}) != "" {
		t.Error("no tables must encode as the empty string, the value of a state without quarantine")
	}
	if got, err := DecodeQuarantine(""); err != nil || got != nil {
		t.Errorf("DecodeQuarantine(\"\") = %v, %v", got, err)
	}
}

func TestDamagedQuarantineIsAnError(t *testing.T) {
	if _, err := DecodeQuarantine("{not json"); err == nil || !strings.Contains(err.Error(), "damaged") {
		t.Errorf("error = %v, want one that says the list is damaged: dropping it would silently un-quarantine tables", err)
	}
}
