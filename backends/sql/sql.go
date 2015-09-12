package sql

type ColumnInfo struct{
	Name string
	Type string

	Unique bool
	AutoIncrement bool
	PrimaryKey bool
	NotNull bool
	Default string
	
	Index string
		
	ForeignKey string
	ForeignKeyTable string
	ForeignKeyOnUpdate string
	ForeignKeyOnDelete string

	Constraints string
}

type TableInfo struct{
	Name string
	Columns map[string]*ColumnInfo
	UniqueFields [][]string

	Constraints string
}

func (t TableInfo) HasForeignKeyToTable(table string) bool {
	for columnName := range t.Columns {
		column := t.Columns[columnName]
		if column.ForeignKeyTable == table {
			return true
		}
	}

	return false
}

type Join struct {
	Type string
	Table string
	JoinColumn string
	ForeignKeyColumn string

	Columns []string

	Where string
	WhereArgs []interface{}
}

func (j Join) HasColumns() bool {
	return j.Columns != nil && len(j.Columns) > 0 && j.Columns[0] != "*"
}

type SelectSpec struct {
	Table string
	Columns []string
	Where string
	WhereArgs []interface{}
	Orders string
	Limit int
	Offset int

	Joins []Join
}

func (s SelectSpec) HasColumns() bool {
	return s.Columns != nil && len(s.Columns) > 0 && s.Columns[0] != "*"
}