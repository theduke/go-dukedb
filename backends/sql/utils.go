package sql

import(
	"strings"
	"strconv"

	db "github.com/theduke/go-dukedb"
)

func BuildTableInfo(d Dialect, modelInfo *db.ModelInfo) *TableInfo {
	tableInfo := &TableInfo{
		Name: modelInfo.BackendName,
		Columns: make(map[string]*ColumnInfo),
	}

	uniqueFields := make([][]string, 0)

	for name := range modelInfo.FieldInfo {
		fieldInfo := modelInfo.FieldInfo[name]

		if fieldInfo.Ignore || fieldInfo.IsRelation() {
			continue
		}

		info := &ColumnInfo{
			Name: fieldInfo.BackendName,
			Type: d.ColumnType(fieldInfo),

			Unique: fieldInfo.Unique,
			PrimaryKey: fieldInfo.PrimaryKey,
			AutoIncrement: fieldInfo.AutoIncrement,
			NotNull: fieldInfo.NotNull,
			Default: fieldInfo.Default,

			Index: fieldInfo.Index,	
			//Constraints: ??
		}

		// Handle unique clauses with multiple fields.
		if fieldInfo.UniqueWith != nil && len(fieldInfo.UniqueWith) > 0 {
			unique := make([]string, 0)
			copy(unique, fieldInfo.UniqueWith)
			unique = append(unique, fieldInfo.BackendName)
			uniqueFields = append(uniqueFields, unique)
		}

		tableInfo.Columns[info.Name] = info
	}

	if len(uniqueFields) > 0 {
		tableInfo.UniqueFields = uniqueFields
	}

	return tableInfo
}

func CreateTableStatement(d Dialect, info *TableInfo, ifExists bool) string {
	stmt := "CREATE TABLE"

	if ifExists {
		stmt += " IF NOT EXISTS "
	}

	stmt += " " + d.Quote(info.Name) + " "

	stmt += "("

	statements := make([]string, 0)

	for name := range info.Columns {
		columnInfo := info.Columns[name]
		stmt := d.ColumnStatement(columnInfo)
		statements = append(statements, stmt)
	}

	statements = append(statements, d.TableConstraintStatements(info)...)

	stmt += strings.Join(statements, ", ")

	stmt += ")"

	return stmt
}

func DropTableStatement(d Dialect, table string, ifExists bool) string {
	stmt := "DROP TABLE" 
	if ifExists {
		stmt += " IF EXISTS"
	}

	stmt += " " + d.Quote(table)

	return stmt
}

func CreateIndexStatement(d Dialect, name, table string, columnNames []string) string {
	stmt := "CREATE INDEX " + d.Quote(name) + " ON " + d.Quote(table)

	columns := make([]string, 0)
	for _, name := range columnNames {
		columns = append(columns, d.Quote(name))
	}

	stmt += "(" + strings.Join(columns, ", ") + ")"

	return stmt
}

func DropIndexStatement(d Dialect, name string, ifExists bool) string {
	return "DROP INDEX " + d.Quote(name)
}

func AddColumnStatement(d Dialect, table string, info *ColumnInfo) string {
	return "ALTER TABLE " + d.Quote(table) + " ADD COLUMN " + d.ColumnStatement(info)
}

func DropColumnStatement(d Dialect, table, name string) string {
	return "ALTER TABLE " + d.Quote(table) + " DROP COLUMN " + d.Quote(name)
}

func InsertMapStatement(d Dialect, table string, data map[string]interface{}) (string, []interface{}) {
	stmt := "INSERT INTO " + d.Quote(table) + " "

	columns := make([]string, 0)
	replacements := make([]string, 0)
	vals := make([]interface{}, 0)

	for key := range data {
		columns = append(columns, d.Quote(key))
		replacements = append(replacements, d.ReplacementCharacter())
		vals = append(vals, d.Value(data[key]))
	}

	stmt += "(" + strings.Join(columns, ", ") + ") "
	stmt += "VALUES (" + strings.Join(replacements, ", ") + ")"

	return stmt, vals
}

func WhereStatement(spec *SelectSpec) (string, []interface{}) {
	stmt := ""

	if spec.Where != "" {
		stmt += " WHERE " + spec.Where
	}
	if spec.Orders != "" {
		stmt += " ORDER BY " + spec.Orders
	}
	if spec.Limit != 0 {
		stmt += " LIMIT " + strconv.Itoa(spec.Limit)
	}
	if spec.Offset != 0 {
		stmt += " OFFSET " + strconv.Itoa(spec.Offset)
	}

	return stmt, spec.WhereArgs
}

func UpdateByMapStatement(d Dialect, spec *SelectSpec, data map[string]interface{}) (string, []interface{}) {
	stmt := "UPDATE " + d.Quote(spec.Table) + " SET "

	columns := make([]string, 0)
	vals := make([]interface{}, 0)
	for key := range data {
		columns = append(columns, d.Quote(key) + "=" + d.ReplacementCharacter())
		vals = append(vals, d.Value(data[key]))
	}

	stmt += strings.Join(columns, ", ")

	where, whereArgs := d.WhereStatement(spec)
	if where != "" {
		stmt += where
		if whereArgs != nil {
			vals = append(vals, whereArgs...)
		}
	}

	return stmt, vals
}

func DeleteStatement(d Dialect, spec *SelectSpec) (string, []interface{}) {
	stmt := "DELETE FROM " + d.Quote(spec.Table)
	where, whereArgs := d.WhereStatement(spec)
	if where != "" {
		stmt += where
	}
	return stmt, whereArgs
}

func SelectStatement(d Dialect, spec *SelectSpec) (string, []interface{}) {
	stmt := "SELECT"

	if !spec.HasColumns() {
		spec.Columns = []string{"*"}
	} else {
		/*
		for index, column := range spec.Columns {
			spec.Columns[index] = d.Quote(column)
		}
		*/
	}

	quotedTable := d.Quote(spec.Table)

	// Handle joins.
	joinClauses := make([]string, 0)
	if spec.Joins != nil {
		for _, join := range spec.Joins {
			quotedJoinTable := d.Quote(join.Table)

			// Add join clause.
			clause := ""
			if join.Type == db.InnerJoin {
				clause = "INNER JOIN"
			}
			if join.Type == db.LeftJoin {
				clause = "LEFT JOIN"
			}
			if join.Type == db.RightJoin {
				clause = "RIGHT JOIN"
			}
			if join.Type == db.CrossJoin {
				clause = "CROSS JOIN"
			}

			clause += " " + quotedJoinTable + " AS " + join.Table
			clause += " ON (" + quotedJoinTable + "." + d.Quote(join.JoinColumn) + "=" + quotedTable + "." + d.Quote(join.ForeignKeyColumn) + ")"

			if join.HasColumns() {
				for _, column := range join.Columns {
					spec.Columns = append(spec.Columns, quotedJoinTable + "." + d.Quote(column))
				}
			} else {
				spec.Columns = append(spec.Columns, quotedJoinTable + ".*")
			}
			joinClauses = append(joinClauses, clause)
		}
	}

	stmt += " " + strings.Join(spec.Columns, ", ")

	stmt += " FROM " + quotedTable

	if len(joinClauses) > 0 {
		stmt += " " + strings.Join(joinClauses, " ")
	}

	where, whereArgs := d.WhereStatement(spec)
	if where != "" {
		stmt += where
	}
	
	return stmt, whereArgs
}
