package sql

import(
	"reflect"
	"fmt"
	"strings"

	db "github.com/theduke/go-dukedb"
)

type Sqlite3Dialect struct {
	BaseDialect
}

// Ensure that Sqlite3Dialect implementes Dialect interface.
var _ Dialect = (*Sqlite3Dialect)(nil)

func (d Sqlite3Dialect) HasLastInsertID() bool {
	return true
}

func (d Sqlite3Dialect) HasTransactions() bool {
	return true
}

func (d Sqlite3Dialect) Quote(id string) string {
	return "\"" + strings.Replace(id, "\"", "", -1) + "\""
}

func (d Sqlite3Dialect) ReplacementCharacter() string {
	return "$$"
}

func (d Sqlite3Dialect) ColumnType(info *db.FieldInfo) string {
	switch info.Type {
	case reflect.Bool:
		return "integer"

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr, reflect.Int64, reflect.Uint64:
		return "integer"

	case reflect.Float32, reflect.Float64:
		return "float"

	case reflect.String:
		return "text"

	case reflect.Struct:
		if info.StructType == "time.Time" {
			return "datetime"
		}

	case reflect.Slice:
		return "blob"
	}
	panic(fmt.Sprintf("invalid field type %s for field %v for mysql", info.Type, info.Name))
}

func (d Sqlite3Dialect) BuildTableInfo(modelInfo *db.ModelInfo) *TableInfo {
	return BuildTableInfo(d, modelInfo)
}

func (d Sqlite3Dialect) CreateTableStatement(info *TableInfo, ifExists bool) string {
	return CreateTableStatement(d, info, ifExists)
}

func (d Sqlite3Dialect) DropTableStatement(table string, ifExists bool) string {
	return DropTableStatement(d, table, ifExists)
}

func (d Sqlite3Dialect) TableConstraintStatements(info *TableInfo) []string {
	stmts := make([]string, 0)

	if info.UniqueFields != nil {
		for _, fields := range info.UniqueFields {
			stmts = append(stmts, "UNIQUE(" + strings.Join(fields, ", ") + ")")
		}
	}

	return stmts
}

func (d Sqlite3Dialect) CreateIndexStatement(name, table string, columnNames []string) string {
	return CreateIndexStatement(d, name, table, columnNames)
}

func (d Sqlite3Dialect) DropIndexStatement(name string, ifExists bool) string {
	return DropIndexStatement(d, name, ifExists)
}

func (d Sqlite3Dialect) ColumnStatement(info *ColumnInfo) string {
	stmt := d.Quote(info.Name) + " " + info.Type
	if info.NotNull {
		stmt += " NOT NULL"
	}
	if info.PrimaryKey {
		stmt += " PRIMARY KEY"
	}
	if info.AutoIncrement {
		stmt += " AUTOINCREMENT"
	}
	if info.Constraints != "" {
		stmt += " CHECK(" + info.Constraints + ")"
	}
	if info.Default != "" {
		stmt += " DEFAULT " + info.Default
	}
	if info.Unique {
		stmt += " UNIQUE"
	}
	if info.ForeignKey != "" {
		stmt += " REFERENCES " + d.Quote(info.ForeignKeyTable)
		stmt += " (" + d.Quote(info.ForeignKey) + ")"
		if info.ForeignKeyOnUpdate != "" {
			stmt += " ON UPDATE " + info.ForeignKeyOnUpdate
		}
		if info.ForeignKeyOnDelete != "" {
			stmt += " ON DELETE " + info.ForeignKeyOnDelete
		}
	}

	return stmt
}

func (d Sqlite3Dialect) AddColumnStatement(table string, info *ColumnInfo) string {
	return AddColumnStatement(d, table, info)
}

func (d Sqlite3Dialect) AlterColumnTypeStatement(table string, info *ColumnInfo) string {
	panic("Sqlite3 backend does not support altering column type")
}

func (d Sqlite3Dialect) AlterColumnNameStatement(table, oldName, newName string) string {
	panic("Sqlite3 backend does not support altering column name")
}

func (d Sqlite3Dialect) DropColumnStatement(table, name string) string {
	return DropColumnStatement(d, table, name)
}

func (d Sqlite3Dialect) InsertMapStatement(tableInfo *TableInfo, data map[string]interface{}) (string, []interface{}) {
	return InsertMapStatement(d, tableInfo.Name, data)
}

func (d Sqlite3Dialect) WhereStatement(spec *SelectSpec) (string, []interface{}) {
	return WhereStatement(spec)
}

func (d Sqlite3Dialect) UpdateByMapStatement(spec *SelectSpec, data map[string]interface{}) (string, []interface{}) {
	return UpdateByMapStatement(d, spec, data)
}

func (d Sqlite3Dialect) DeleteStatement(spec *SelectSpec) (string, []interface{}) {
	return DeleteStatement(d, spec)
}

func (d Sqlite3Dialect) SelectStatement(spec *SelectSpec) (string, []interface{}) {
	return SelectStatement(d, spec)
}
