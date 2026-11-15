package schema

// Catalog is what the source's catalog (information_schema) says about the tables binwave is
// going to replicate, in a form that needs no database to inspect: the startup checks run over it.
type Catalog struct {
	Tables      []CatalogTable
	ForeignKeys []ForeignKey
}

// CatalogTable is one base table with its columns and unique indexes.
type CatalogTable struct {
	Database string
	Name     string
	// Columns are the column names in table order.
	Columns []string
	// PrimaryKey are the columns of the primary key, in key order; empty without one. An invisible
	// generated key (MySQL 8.0.30+, sql_generate_invisible_primary_key) counts as a primary key.
	PrimaryKey []string
	// UniqueKeys are the other unique indexes.
	UniqueKeys []UniqueKey
	// EstimatedRows is what the server believes the table holds (information_schema.TABLES.TABLE_ROWS):
	// a statistic of the storage engine, off by tens of percent, and enough to tell a table of a
	// hundred million rows from one of a hundred. The initial load reads the big ones first, so that
	// the whole load is not left waiting for one table that started last (see Pipeline.tablesToLoad).
	EstimatedRows int64
	// UnreliableColumns are the columns whose values the source server may write wrongly into the
	// binlog, by a known bug of its version: the compressed columns of a partitioned table on
	// Percona Server before 8.0.35 (PS-7806).
	UnreliableColumns []string
}

// FullName renders "database.table".
func (t CatalogTable) FullName() string { return t.Database + "." + t.Name }

// HasKey reports whether the binlog identifies rows of the table by a key: a primary key, or a
// unique index over NOT NULL columns. Other tables have no key in the binlog.
func (t CatalogTable) HasKey() bool {
	if len(t.PrimaryKey) > 0 {
		return true
	}
	for _, k := range t.UniqueKeys {
		if !k.Nullable {
			return true
		}
	}
	return false
}

// UniqueKey is a unique index other than the primary key.
type UniqueKey struct {
	Name    string
	Columns []string
	// Nullable is true when a column may hold NULL (or the index is over an expression): such a
	// key does not identify rows, since NULLs are never equal.
	Nullable bool
}

// ForeignKey is a foreign key constraint of a child table.
type ForeignKey struct {
	Database        string
	Table           string
	Name            string
	ReferencedTable string
	// UpdateRule and DeleteRule are the referential actions: CASCADE, SET NULL, SET DEFAULT,
	// RESTRICT or NO ACTION.
	UpdateRule string
	DeleteRule string
}

// Cascades reports whether the key changes rows of its table on its own: MySQL applies these
// changes inside the storage engine and never writes them to the binlog.
func (f ForeignKey) Cascades() bool {
	return changesRows(f.UpdateRule) || changesRows(f.DeleteRule)
}

func changesRows(rule string) bool {
	switch rule {
	case "CASCADE", "SET NULL", "SET DEFAULT":
		return true
	}
	return false
}
