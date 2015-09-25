package sql

import (
	"strings"

	db "github.com/theduke/go-dukedb"
)

type Dialect interface {
	HasTransactions() bool

	HasLastInsertID() bool

	ReplacementCharacter() string
	Quote(string) string
	QuoteValue(string) string
	QuoteMany([]string) string

	ColumnType(*db.FieldInfo) string

	// Based on an arbitrary variable, return a representation the sql db can handle.
	Value(interface{}) interface{}

	// Builds up the table information from model information and stores
	// the table data indo ModelInfo.BackendData and the field info into
	// FieldInfo.BackendData
	BuildTableInfo(*db.ModelInfo) *TableInfo

	CreateTableStatement(info *TableInfo, ifExists bool) string
	DropTableStatement(table string, ifExists bool) string

	ColumnStatement(*ColumnInfo) string
	TableConstraintStatements(*TableInfo) []string

	CreateIndexStatement(name, table string, columns []string) string
	DropIndexStatement(name string, ifExists bool) string

	AddColumnStatement(table string, info *ColumnInfo) string
	AlterColumnTypeStatement(table string, info *ColumnInfo) string
	AlterColumnNameStatement(table, oldName, newName string) string
	DropColumnStatement(table, name string) string

	InsertMapStatement(table *TableInfo, data map[string]interface{}) (string, []interface{})

	WhereStatement(*SelectSpec) (string, []interface{})
	UpdateByMapStatement(where *SelectSpec, data map[string]interface{}) (string, []interface{})

	DeleteStatement(*SelectSpec) (string, []interface{})
	SelectStatement(*SelectSpec) (string, []interface{})
}

type BaseDialect struct {
}

func (d BaseDialect) ReplacementCharacter() string {
	return "?"
}

func (d BaseDialect) Quote(id string) string {
	return "\"" + strings.Replace(id, "\"", "", -1) + "\""
}

func (d BaseDialect) QuoteValue(value string) string {
	return `"` + value + `"`
}

func (d BaseDialect) QuoteMany(ids []string) string {
	quoted := make([]string, 0)
	for _, id := range ids {
		quoted = append(quoted, d.Quote(id))
	}

	return strings.Join(quoted, ", ")
}

func (d BaseDialect) Value(v interface{}) interface{} {
	return v
}
