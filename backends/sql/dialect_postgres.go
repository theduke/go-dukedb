package sql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	db "github.com/theduke/go-dukedb"
)

type PostgresDialect struct {
	BaseDialect
}

// Ensure that PostgresDialect implementes Dialect interface.
var _ Dialect = (*PostgresDialect)(nil)

func (d PostgresDialect) HasLastInsertID() bool {
	return false
}

func (d PostgresDialect) HasTransactions() bool {
	return true
}

func (d PostgresDialect) ReplacementCharacter() string {
	return "${}$"
}

func (d PostgresDialect) FixReplacementChar(query string) string {
	index := 0
	for {
		pos := strings.Index(query, "${}$")
		if pos == -1 {
			break
		}

		index += 1
		query = strings.Replace(query, "${}$", "$"+strconv.Itoa(index), 1)
	}

	return query
}

func (d PostgresDialect) ColumnType(info *db.FieldInfo) string {
	switch info.Type.Kind() {
	case reflect.Bool:
		return "boolean"

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
		if info.AutoIncrement {
			return "serial"
		}
		return "integer"

	case reflect.Int64, reflect.Uint64:
		if info.AutoIncrement {
			return "bigserial"
		}
		return "bigint"

	case reflect.Float32, reflect.Float64:
		return "numeric"

	case reflect.String:
		if info.Max > 0 && info.Max < 65532 {
			return fmt.Sprintf("varchar(%d)", info.Max)
		}
		return "text"

	case reflect.Struct:
		typ := info.Type.PkgPath() + "." + info.Type.Name()
		if typ == "time.Time" {
			return "timestamp with time zone"
		}

	case reflect.Map:
		return "hstore"

	case reflect.Slice:
		if info.StructType == "byte" {
			return "bytea"
		}
	}
	panic(fmt.Sprintf("Field %v has unsupported type %v (postgres)", info.Name, info.Type))
}

func (d PostgresDialect) BuildTableInfo(modelInfo *db.ModelInfo) *TableInfo {
	return BuildTableInfo(d, modelInfo)
}

func (d PostgresDialect) CreateTableStatement(info *TableInfo, ifExists bool) string {
	return CreateTableStatement(d, info, ifExists)
}

func (d PostgresDialect) DropTableStatement(table string, ifExists bool) string {
	return DropTableStatement(d, table, ifExists)
}

func (d PostgresDialect) ColumnStatement(info *ColumnInfo) string {
	return ColumnStatement(d, info)
}

func (d PostgresDialect) TableConstraintStatements(info *TableInfo) []string {
	stmts := make([]string, 0)

	if info.UniqueFields != nil {
		for _, fields := range info.UniqueFields {
			stmts = append(stmts, "UNIQUE("+strings.Join(fields, ", ")+")")
		}
	}

	return stmts
}

func (d PostgresDialect) CreateIndexStatement(name, table string, columnNames []string) string {
	return CreateIndexStatement(d, name, table, columnNames)
}

func (d PostgresDialect) DropIndexStatement(name string, ifExists bool) string {
	return DropIndexStatement(d, name, ifExists)
}

func (d PostgresDialect) AddColumnStatement(table string, info *ColumnInfo) string {
	return AddColumnStatement(d, table, info)
}

func (d PostgresDialect) AlterColumnTypeStatement(table string, info *ColumnInfo) string {
	stmt := "ALTER TABLE " + d.Quote(table)
	stmt += " ALTER COLUMN " + d.Quote(info.Name) + " TYPE " + info.Type
	return stmt
}

func (d PostgresDialect) AlterColumnNameStatement(table, oldName, newName string) string {
	stmt := "ALTER TABLE " + d.Quote(table)
	stmt += " RENAME " + d.Quote(oldName) + " TO " + d.Quote(newName)
	return stmt
}

func (d PostgresDialect) DropColumnStatement(table, name string) string {
	return DropColumnStatement(d, table, name)
}

func (d PostgresDialect) InsertMapStatement(tableInfo *TableInfo, data map[string]interface{}) (string, []interface{}) {
	stmt, args := InsertMapStatement(d, tableInfo.Name, data)

	// Add RETURNING statement for autovalue fields.
	returnColumns := make([]string, 0)
	for name := range tableInfo.Columns {
		column := tableInfo.Columns[name]
		if column.AutoIncrement {
			returnColumns = append(returnColumns, column.Name)
		}
	}
	if len(returnColumns) > 0 {
		stmt += " RETURNING " + d.QuoteMany(returnColumns)
	}

	return d.FixReplacementChar(stmt), args
}

func (d PostgresDialect) WhereStatement(spec *SelectSpec) (string, []interface{}) {
	return WhereStatement(spec)
}

func (d PostgresDialect) UpdateByMapStatement(spec *SelectSpec, data map[string]interface{}) (string, []interface{}) {
	stmt, args := UpdateByMapStatement(d, spec, data)
	return d.FixReplacementChar(stmt), args
}

func (d PostgresDialect) DeleteStatement(spec *SelectSpec) (string, []interface{}) {
	stmt, args := DeleteStatement(d, spec)
	return d.FixReplacementChar(stmt), args
}

func (d PostgresDialect) SelectStatement(spec *SelectSpec) (string, []interface{}) {
	stmt, args := SelectStatement(d, spec)
	return d.FixReplacementChar(stmt), args
}
