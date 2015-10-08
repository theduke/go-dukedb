package sql

import (
	"fmt"
	"reflect"
	"strings"

	db "github.com/theduke/go-dukedb"
)

type MysqlDialect struct {
	BaseDialect
}

// Ensure that MysqlDialect implementes Dialect interface.
var _ Dialect = (*MysqlDialect)(nil)

func (d MysqlDialect) HasLastInsertID() bool {
	return true
}

func (d MysqlDialect) HasTransactions() bool {
	return true
}

func (d MysqlDialect) Quote(id string) string {
	return "`" + strings.Replace(id, "`", "", -1) + "`"
}

func (d MysqlDialect) ReplacementCharacter() string {
	return "?"
}

func (d MysqlDialect) ColumnType(info *db.FieldInfo) string {
	switch info.Type.Kind() {
	case reflect.Bool:
		return "boolean"

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "int"

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
		return "int unsigned"

	case reflect.Int64:
		return "bigint"

	case reflect.Uint64:
		return "bigint unsigned"

	case reflect.Float32, reflect.Float64:
		return "double"

	case reflect.String:
		if info.Max > 0 && info.Max < 65532 {
			return fmt.Sprintf("varchar(%v)", info.Max)
		}
		return "longtext"

	case reflect.Struct:
		typ := info.Type.PkgPath() + "." + info.Type.Name()
		if typ == "time.Time" {
			return "DATETIME"
		}

	case reflect.Slice:
		if info.Max > 0 && info.Max < 65532 {
			return fmt.Sprintf("varbinary(%d)", info.Max)
		}
		return "longblob"
	}
	panic(fmt.Sprintf("invalid field type %s for field %v for mysql", info.Type, info.Name))
}

func (d MysqlDialect) BuildTableInfo(modelInfo *db.ModelInfo) *TableInfo {
	return BuildTableInfo(d, modelInfo)
}

func (d MysqlDialect) CreateTableStatement(info *TableInfo, ifExists bool) string {
	return CreateTableStatement(d, info, ifExists)
}

func (d MysqlDialect) DropTableStatement(table string, ifExists bool) string {
	return DropTableStatement(d, table, ifExists)
}

func (d MysqlDialect) ColumnStatement(info *ColumnInfo) string {
	stmt := ColumnStatement(d, info)
	if info.AutoIncrement {
		stmt += " AUTO_INCREMENT"
	}
	return stmt
}

func (d MysqlDialect) TableConstraintStatements(info *TableInfo) []string {
	stmts := make([]string, 0)

	if info.UniqueFields != nil {
		for _, fields := range info.UniqueFields {
			stmts = append(stmts, "UNIQUE("+strings.Join(fields, ", ")+")")
		}
	}

	return stmts
}

func (d MysqlDialect) AlterAutoIncrementIndexStatement(info *TableInfo, column string, index int) string {
	return fmt.Sprintf("ALTER TABLE %v AUTO_INCREMENT = %v", info.Name, index)
}

func (d MysqlDialect) CreateIndexStatement(name, table string, columnNames []string) string {
	return CreateIndexStatement(d, name, table, columnNames)
}

func (d MysqlDialect) DropIndexStatement(name string, ifExists bool) string {
	return DropIndexStatement(d, name, ifExists)
}

func (d MysqlDialect) AddColumnStatement(table string, info *ColumnInfo) string {
	return AddColumnStatement(d, table, info)
}

func (d MysqlDialect) AlterColumnTypeStatement(table string, info *ColumnInfo) string {
	panic("NOT IMPLEMENTED")
}

func (d MysqlDialect) AlterColumnNameStatement(table, oldName, newName string) string {
	panic("NOT IMPLEMENTED")
}

func (d MysqlDialect) DropColumnStatement(table, name string) string {
	return DropColumnStatement(d, table, name)
}

func (d MysqlDialect) InsertMapStatement(tableInfo *TableInfo, data map[string]interface{}) (string, []interface{}) {
	return InsertMapStatement(d, tableInfo.Name, data)
}

func (d MysqlDialect) WhereStatement(spec *SelectSpec) (string, []interface{}) {
	return WhereStatement(spec)
}

func (d MysqlDialect) UpdateByMapStatement(spec *SelectSpec, data map[string]interface{}) (string, []interface{}) {
	return UpdateByMapStatement(d, spec, data)
}

func (d MysqlDialect) DeleteStatement(spec *SelectSpec) (string, []interface{}) {
	return DeleteStatement(d, spec)
}

func (d MysqlDialect) SelectStatement(spec *SelectSpec) (string, []interface{}) {
	return SelectStatement(d, spec)
}
