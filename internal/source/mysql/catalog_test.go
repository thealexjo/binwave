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
