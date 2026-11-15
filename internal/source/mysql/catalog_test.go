package mysql

import (
	"slices"
	"testing"
)

func TestCompressedColumnsAreReadFromTheDefinition(t *testing.T) {
	def := "CREATE TABLE `docs` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `body` blob /*!50633 COLUMN_FORMAT COMPRESSED */,\n" +
		"  `odd``name` json /*!50633 COLUMN_FORMAT COMPRESSED WITH COMPRESSION_DICTIONARY `d` */ DEFAULT NULL,\n" +
		"  `plain` text COMMENT 'not COLUMN_FORMAT COMPRESSED',\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
		"/*!50100 PARTITION BY HASH (`id`)\nPARTITIONS 4 */"

	got := compressedColumns(def)

	if want := []string{"body", "odd`name"}; !slices.Equal(got, want) {
		t.Errorf("compressedColumns() = %q, want %q", got, want)
	}
}

// TestOnlyAPerconaServerBefore8035IsReadForCompressedColumns: MySQL has no compressed columns, and a
// fixed Percona Server writes them correctly, so neither pays for a SHOW CREATE TABLE of every
// partitioned table.
func TestOnlyAPerconaServerBefore8035IsReadForCompressedColumns(t *testing.T) {
	for _, c := range []struct {
		version, comment string
		want             bool
	}{
		{"8.0.28-20", "Percona Server (GPL), Release 20", true},
		{"8.0.34-26", "Percona Server (GPL), Release 26", true},
		{"8.0.35-27", "Percona Server (GPL), Release 27", false},
		{"8.4.3-3", "Percona Server (GPL), Release 3", false},
		{"8.0.28", "MySQL Community Server - GPL", false},
		{"8.0.23", "", false},
	} {
		s := serverOf(map[string]string{"version": c.version, "version_comment": c.comment})
		if got := s.mayMiswriteCompressedPartitions(); got != c.want {
			t.Errorf("%s (%s): %v, want %v", c.version, c.comment, got, c.want)
		}
	}
}
