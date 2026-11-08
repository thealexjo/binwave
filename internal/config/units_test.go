package config

import "testing"

func TestParseByteSize(t *testing.T) {
	tests := []struct {
		in      string
		want    ByteSize
		wantErr bool
	}{
		{in: "0", want: 0},
		{in: "1024", want: 1024},
		{in: "10B", want: 10},
		{in: "64MiB", want: 64 << 20},
		{in: "1GiB", want: 1 << 30},
		{in: "2TiB", want: 2 << 40},
		{in: "512KiB", want: 512 << 10},
		{in: "100MB", want: 100_000_000},
		{in: "1KB", want: 1000},
		{in: " 8 MiB ", want: 8 << 20},
		{in: "", wantErr: true},
		{in: "MiB", wantErr: true},
		{in: "1.5GiB", wantErr: true},
		{in: "-1MiB", wantErr: true},
		{in: "10XB", wantErr: true},
		{in: "9999999999TiB", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseByteSize(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseByteSize(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("ParseByteSize(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestByteSizeString(t *testing.T) {
	tests := map[ByteSize]string{
		0:           "0B",
		1000:        "1000B",
		KiB:         "1KiB",
		64 * MiB:    "64MiB",
		GiB:         "1GiB",
		GiB + 1:     "1073741825B",
		3 * GiB / 2: "1536MiB",
	}
	for in, want := range tests {
		if got := in.String(); got != want {
			t.Errorf("ByteSize(%d).String() = %q, want %q", int64(in), got, want)
		}
	}
}

func FuzzParseByteSizeRoundTrip(f *testing.F) {
	for _, seed := range []string{"0", "64MiB", "1GB", "12", "-1", "1.5KiB", "9223372036854775807B"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, in string) {
		size, err := ParseByteSize(in)
		if err != nil {
			return
		}
		if size < 0 {
			t.Fatalf("ParseByteSize(%q) = %d, negative", in, size)
		}
		again, err := ParseByteSize(size.String())
		if err != nil || again != size {
			t.Fatalf("round trip %q → %s → %d, %v", in, size, again, err)
		}
	})
}
