package rds_postgres

import (
	"reflect"
)

const (
	IndexTypeBTREE  indexType = "BTREE"
	IndexTypeGIN    indexType = "GIN"
	IndexTypeGIST   indexType = "GIST"
	IndexTypeSPGIST indexType = "SPGIST"
	IndexTypeBRIN   indexType = "BRIN"
	IndexTypeHash   indexType = "HASH"
)

type Row interface {
	Table() *Table
	Columns() *map[string]Column
	SetMetadata()
}

type Table struct {
	Name           string
	Columns        *map[string]Column
	PrimaryKeyName string
}

type Column struct {
	Name       string
	Type       reflect.Type
	Nullable   bool
	PrimaryKey bool
	Value      interface{}
}

type JoinColumnPair struct {
	LeftColumnName  string
	RightColumnName string
}

type JoinResultantColumns struct {
	Columns map[string]reflect.Type
}

type indexType string
