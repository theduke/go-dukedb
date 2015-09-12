package sql

import (
	"reflect"
	"fmt"
	"strconv"
	"log"

	"database/sql"

	db "github.com/theduke/go-dukedb"
)


type Backend struct {
	db.BaseBackend

	Db *sql.DB
	Tx *sql.Tx

	dialect Dialect

	TableInfo map[string]*TableInfo

	MigrationHandler *db.MigrationHandler
}

// Ensure that Backend implements the db.Backend interfaces at compile time.
var _ db.Backend = (*Backend)(nil)
var _ db.MigrationBackend = (*Backend)(nil)
var _ db.TransactionBackend = (*Backend)(nil)

func New(driver, driverOptions string) (*Backend, db.DbError) {
	b := Backend{}

	switch driver {
	case "postgres":
			b.dialect = &PostgresDialect{}
	case "mysql":
		b.dialect = &MysqlDialect{}
	case "sqlite3":
		b.dialect = &Sqlite3Dialect{}
	default:
		panic("Unsupported sql driver: " + driver)
	}

	DB, err := sql.Open(driver, driverOptions)
	if err != nil {
		return nil, db.Error{
			Code: "connection_error",
			Message: err.Error(),
		}
	}

	b.Db = DB

	b.ModelInfo = make(map[string]*db.ModelInfo)
	b.TableInfo = make(map[string]*TableInfo)
	b.MigrationHandler = db.NewMigrationHandler(&b)

	b.RegisterModel(&MigrationAttempt{})

	return &b, nil
}

func (b Backend) GetName() string {
	return "sql"
}

func (b *Backend) SetDebug(d bool) {
	b.Debug = d
}

func (b Backend) Copy() db.Backend {
	copied := Backend{
		Db: b.Db,
		dialect: b.dialect,
		TableInfo: b.TableInfo,	
		MigrationHandler: b.MigrationHandler,
	}
	copied.ModelInfo = b.ModelInfo
	copied.SetDebug(b.GetDebug())
	return &copied
}

func (b *Backend) RegisterModel(m db.Model) error {
	b.BaseBackend.RegisterModel(m)

	info := b.ModelInfo[m.Collection()]
	tableInfo := b.dialect.BuildTableInfo(info)
	b.TableInfo[info.BackendName] = tableInfo

	return nil
}

func (b Backend) BuildRelationshipInfo() {
	db.BuildAllRelationInfo(b.ModelInfo)

	// Build m2m tables.
	for model := range b.ModelInfo {
		modelInfo := b.ModelInfo[model]
		tableInfo := b.TableInfo[modelInfo.BackendName]

		for field := range modelInfo.FieldInfo {
			fieldInfo := modelInfo.FieldInfo[field]
			if !fieldInfo.M2M {
				continue
			}

			relatedInfo := b.ModelInfo[fieldInfo.RelationItem.Collection()]
			relatedTableInfo := b.TableInfo[relatedInfo.BackendName]

			column := tableInfo.Columns[modelInfo.GetPkField().BackendName]
			pkColumn := &ColumnInfo{
				Name: modelInfo.BackendName + "_" + column.Name,
				Type: column.Type,
				NotNull: true,
				ForeignKey: column.Name,
				ForeignKeyTable: tableInfo.Name,
			}

			column = relatedTableInfo.Columns[relatedInfo.GetPkField().BackendName]
			foreignColumn := &ColumnInfo{
				Name: relatedInfo.BackendName + "_" + column.Name,
				Type: column.Type,
				NotNull: true,
				ForeignKey: column.Name,
				ForeignKeyTable: relatedTableInfo.Name,
			}

			if fieldInfo.M2M {
				table := &TableInfo{
					Name: fieldInfo.M2MCollection,
					Columns: map[string]*ColumnInfo{
						pkColumn.Name: pkColumn,
						foreignColumn.Name: foreignColumn,
					},
					UniqueFields: [][]string{[]string{pkColumn.Name, foreignColumn.Name}},
				}
				b.TableInfo[table.Name] = table
			}
		}
	}
}

func (b Backend) SqlExec(query string, args ...interface{}) (sql.Result, error) {
	if b.Debug {
		log.Printf("%v | %+v", query, args)
	}

	if b.Tx != nil {
		return b.Tx.Exec(query, args...)
	} else {
		return b.Db.Exec(query, args...)
	}
}

func (b Backend) SqlQuery(query string, args ...interface{}) (*sql.Rows, error) {
	if b.Debug{
		log.Printf("%v | %+v", query, args)
	}

	if b.Tx != nil {
		return b.Tx.Query(query, args...)
	} else {
		return b.Db.Query(query, args...)
	}
}

func (b Backend) sqlScanRow(query string, args []interface{}, vars ...interface{}) db.DbError {
	rows, err := b.SqlQuery(query, args...)
	if err != nil {
		return db.Error{
			Code: "sql_error",
			Message: err.Error(),
			Data: err,
		}	
	}

	defer rows.Close()
	rows.Next()
	rows.Scan(vars...)
	if err := rows.Err(); err != nil {
		return db.Error{
			Code: "sql_rows_error",
			Message: err.Error(),
		}
	}	

	return nil
} 

func (b Backend) CreateCollection(name string) db.DbError {
	info := b.GetModelInfo(name)
	if info == nil {
		return db.Error{
			Code: "unknown_model",
			Message: fmt.Sprintf("Model %v not registered with GORM backend", name),
		}
	}

	tableInfo := b.TableInfo[info.BackendName]
	stmt := b.dialect.CreateTableStatement(tableInfo, true)	
	if _, err := b.SqlExec(stmt); err != nil {
		return db.Error{
			Code: "create_table_failed",
			Message: err.Error(),
		}
	}

	// Create m2m tables.
	for name := range info.FieldInfo {
		field := info.FieldInfo[name]
		if field.M2M {
			table := b.TableInfo[field.M2MCollection]
			stmt := b.dialect.CreateTableStatement(table, true)	
			if _, err := b.SqlExec(stmt); err != nil {
				return db.Error{
					Code: "create_m2m_table_failed",
					Message: err.Error(),
				}
			}
		}
	}

	return nil
}

func (b Backend) DropTable(name string, ifExists bool) db.DbError {
	stmt := b.dialect.DropTableStatement(name, ifExists)	
	if _, err := b.SqlExec(stmt); err != nil {
		return db.Error{
			Code: "drop_table_failed",
			Message: err.Error(),
		}
	}

	return nil
}

func (b Backend) DropCollection(name string) db.DbError {
	info := b.GetModelInfo(name)
	if info == nil {
		return db.Error{
			Code: "unknown_model",
			Message: fmt.Sprintf("Model %v not registered with GORM backend", name),
		}
	}

	// First delete all m2m tables.
	for fieldName := range info.FieldInfo {
		field := info.FieldInfo[fieldName]
		if field.M2M {
			b.DropTable(field.M2MCollection, true)	
		}
	}

	return b.DropTable(info.BackendName, true)
}

func (b Backend) DropAllCollections() db.DbError {
	for name := range b.ModelInfo {
		info := b.ModelInfo[name]

		// First delete tables with a foreign key to this table.
		for tableName := range b.TableInfo {
			t := b.TableInfo[tableName]

			if !t.HasForeignKeyToTable(info.BackendName) {
				continue
			}

ColumnLoop:
			for columnName := range t.Columns {
				column := t.Columns[columnName]


				if column.ForeignKey != "" && column.ForeignKeyTable != info.BackendName {
					// Try to find the collection for the table name.
					// If the collection is found, use DropCollection to properly remove
					// m2m tables.
					for collection := range b.ModelInfo {
						if b.ModelInfo[collection].BackendName == column.ForeignKeyTable {
							if err := b.DropCollection(collection); err != nil {
								return err
							}
							continue ColumnLoop
						}
					}

					if err := b.DropTable(column.ForeignKeyTable, true); err != nil {
						return err
					}
				}
			}
		}

		if err := b.DropCollection(name); err != nil {
			return err
		}
	}

	return nil
}


func (b *Backend) Q(model string) *db.Query {
	q := db.Q(model)
	q.Backend = b
	return q
}

func (b Backend) filterManyToSql(info *db.ModelInfo, filters []db.Filter, connector string) (string, []interface{}) {
	sql := "("
	args := make([]interface{}, 0)	

	count := len(filters)
	for i := 0; i < count; i++ {
		subSql, subArgs := b.filterToSql(info, filters[i])

		sql += subSql
		args = append(args, subArgs...)

		if i < count -1 {
			sql += " " + connector + " "
		}
	}

	sql += ")"

	return sql, args
}

func (b Backend) filterToSql(info *db.ModelInfo, filter db.Filter) (string, []interface{}) {
	filterType := reflect.TypeOf(filter).Elem().Name()
	filterName := filter.Type()

	sql := ""
	args := make([]interface{}, 0)

	if filterType == "FieldCondition" {
		// fieldCOnditions can easily be handled generically.
		cond := filter.(*db.FieldCondition)

		// TODO: return error from func.
		operator, _ := db.FilterToSqlCondition(filterName)

		// Allow to query with struct field name.
		if fieldInfo, ok := info.FieldInfo[cond.Field]; ok {
			cond.Field = fieldInfo.BackendName
		}

		sql = cond.Field + " " + operator 
		if filterName == "in" {
			sql += " (" + b.dialect.ReplacementCharacter() + ")"
		} else {
			sql += " " + b.dialect.ReplacementCharacter()
		}

		args = append(args, cond.Value)

		return sql, args
	}

	if filterName == "and" {
		and := filter.(*db.AndCondition)
		sql, args = b.filterManyToSql(info, and.Filters, "AND")
	} else if filterName == "or" {
		or := filter.(*db.OrCondition)
		sql, args = b.filterManyToSql(info, or.Filters, "OR")
	} else if filterName == "not" {
		not := filter.(*db.NotCondition)
		sql, args = b.filterManyToSql(info, not.Filters, "AND")
		sql = "NOT (" + sql + ")"
	} else {
		panic(fmt.Sprintf("SQL: Unhandled filter type '%v'", filterType))
	}

	return sql, args
}

func (b Backend) selectByQuery(q *db.Query) (*SelectSpec, db.DbError) {
	info := b.GetModelInfo(q.Model)
	if info == nil {
		return nil, db.Error{
			Code: "unknown_model",
			Message: fmt.Sprintf("Model '%v' not registered with sql backend", q.Model),
		}
	}

	// Handle filters.
	where := ""
	whereArgs := make([]interface{}, 0)

	if q.Filters != nil && len(q.Filters) > 0 {
		if len(q.Filters) == 1 {
			where, whereArgs = b.filterToSql(info, q.Filters[0])
		} else {
			filter := db.And(q.Filters...)
			where, whereArgs = b.filterToSql(info, filter)
		}
	}

	// Handle field specificiaton.
	columns := []string{"*"}

	if len(q.FieldSpec) > 0 {
		columns = make([]string, 0)
		for _, field := range q.FieldSpec {
			if fieldInfo, ok := info.FieldInfo[field]; ok {
				columns = append(columns, fieldInfo.BackendName)
			} else {
				columns = append(columns, field)
			}
		}
	}

	// Ordering.
	orders := ""
	if q.Orders != nil {

		count := len(q.Orders)
		for i := 0; i < count; i++ {
			orders += q.Orders[i].String()
			if i < count - 1 {
				orders += ", "
			}
		}
	}

	// Handle joins.
	var joins []Join
	if q.Joins != nil {
		for _, join := range q.Joins {
			if join.JoinType == "" || join.Model == "" || join.JoinFieldName == "" || join.ForeignFieldName == "" {
				continue
			}

			joinSpec := Join{
				Table: join.Model,
				Type: join.JoinType,
				JoinColumn: join.JoinFieldName,
				ForeignKeyColumn: join.ForeignFieldName,
			}
			joins = append(joins, joinSpec)
		}
	}

	return &SelectSpec{
		Table: info.BackendName,
		Columns: columns,
		Where: where,
		WhereArgs: whereArgs,
		Orders: orders,
		Limit: q.LimitNum,
		Offset: q.OffsetNum,
		Joins: joins,
	}, nil
}

func (b *Backend) BuildRelationQuery(q *db.RelationQuery) (*db.Query, db.DbError) {
	return db.BuildRelationQuery(b, nil, q)
}

func (b Backend) QuerySql(sql string, args []interface{}) ([]map[string]interface{}, db.DbError) {
	rows, err := b.SqlQuery(sql, args...)
	if err != nil {
		return nil, db.Error{
			Code: "sql_error",
			Message: err.Error(),
		}
	}
	defer rows.Close()

	cols, err  := rows.Columns()
	if err != nil {
		return nil, db.Error{
			Code: "sql_rows_error",
			Message: err.Error(),
		}
	}

	vals := make([]interface{}, len(cols))
	valPointers := make([]interface{}, len(cols))
	for i, _ := range cols {
		valPointers[i] = &vals[i]
	}

	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		if err := rows.Scan(valPointers...); err != nil {
			return nil, db.Error{
				Code: "sql_scan_error",
				Message: err.Error(),
			}
		}

		data := make(map[string]interface{})
		for i, col := range cols {
			data[col] = vals[i]
		}
		result = append(result, data)
	}
	if err := rows.Err(); err != nil {
		return nil, db.Error{
			Code: "sql_rows_error",
			Message: err.Error(),
		}
	}

	return result, nil
}

func (b Backend) querySqlModels(info *db.ModelInfo, sql string, args []interface{}) ([]db.Model, db.DbError) {
	rows, err := b.SqlQuery(sql, args...)
	if err != nil {
		return nil, db.Error{
			Code: "sql_error",
			Message: err.Error(),
		}
	}
	defer rows.Close()

	cols, err  := rows.Columns()
	if err != nil {
		return nil, db.Error{
			Code: "sql_rows_error",
			Message: err.Error(),
		}
	}

	models := make([]db.Model, 0)

	for rows.Next() {
		model, _ := b.NewModel(info.Collection)
		modelVal := reflect.ValueOf(model).Elem()

		vals := make([]interface{}, 0)
		for _, col := range cols {
			fieldName := info.MapFieldName(col)
			if fieldName == "" {
				fmt.Printf("skipping column %v\n", col)
				var x interface{}
				vals = append(vals, &x)
			} else {
				vals = append(vals, modelVal.FieldByName(fieldName).Addr().Interface())
			}
		}

		if err := rows.Scan(vals...); err != nil {
			return nil, db.Error{
				Code: "sql_scan_error",
				Message: err.Error(),
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, db.Error{
			Code: "sql_rows_error",
			Message: err.Error(),
		}
	}

	modelSlice, err := db.InterfaceToModelSlice(models)
	if err != nil {
		return nil, db.Error{
			Code: "model_conversion_error",
			Message: err.Error(),
		}
	}
	return modelSlice, nil
}

// Perform a query.	
func (b Backend) Query(q *db.Query) ([]db.Model, db.DbError) {
	info := b.GetModelInfo(q.Model)
	if info == nil {
		return nil, db.Error{
			Code: "unknown_model",
			Message: fmt.Sprintf("Model %v was not registered with sql backend", q.Model),
		}
	}

	spec, err := b.selectByQuery(q)
	if err != nil {
		return nil, err
	}
	// Todo: fix joins!
	stmt, args := b.dialect.SelectStatement(spec)

	result, err := b.QuerySql(stmt, args)
	if err != nil {
		return nil, err
	}

	slice, err := db.BuildModelSliceFromMap(info, result)
	if err != nil {
		return nil, err
	}

	models, _ := db.InterfaceToModelSlice(slice)

	// Do joins.
	if len(q.Joins) > 0 {
		if err := db.BackendDoJoins(&b, q.Model, models, q.Joins); err != nil {
			return nil, db.Error{
				Code: "join_error",
				Message: err.Error(),
			}
		}
	}

	return models, nil
}

func (b Backend) QueryOne(q *db.Query) (db.Model, db.DbError) {
	return db.BackendQueryOne(&b, q)
}

func (b Backend) Count(q *db.Query) (int, db.DbError) {
	info := b.GetModelInfo(q.Model)
	if info == nil {
		return -1, db.Error{
			Code: "unknown_model",
			Message: fmt.Sprintf("Model %v was not registered with sql backend", q.Model),
		}
	}

	q.FieldSpec = []string{"COUNT(*) as count"}

	spec, err := b.selectByQuery(q)
	if err != nil {
		return -1, err
	}
	stmt, args := b.dialect.SelectStatement(spec)

	var count int
	if err := b.sqlScanRow(stmt, args, &count); err != nil {
		return -1, err
	}
		
	return count, nil
}

func (b Backend) Last(q *db.Query) (db.Model, db.DbError) {
	return db.BackendLast(&b, q)
}

// Find first model with primary key ID.
func (b Backend) FindOne(modelType string, id string) (db.Model, db.DbError) {
	return db.BackendFindOne(&b, modelType, id)	
}

func (b Backend) FindBy(modelType, field string, value interface{}) ([]db.Model, db.DbError) {
	return b.Q(modelType).Filter(field, value).Find()
}

func (b Backend) FindOneBy(modelType, field string, value interface{}) (db.Model, db.DbError) {
	return b.Q(modelType).Filter(field, value).First()
}

// Auto-persist related models.

	 
func (b *Backend) Create(m db.Model) db.DbError {
	info := b.GetModelInfo(m.Collection())
	if info == nil {
		return db.Error{
			Code: "unknown_model",
			Message: fmt.Sprintf("Model %v was not registered with sql backend", m.Collection()),
		}
	}

	// Persist relationships before create.
	err := db.BackendPersistRelations(b, info, m)
	if err != nil {
		return err
	}

	data, err := db.ModelToMap(info, m, false)
	if err != nil {
		return err
	}

	tableInfo := b.TableInfo[info.BackendName]
	sql, args := b.dialect.InsertMapStatement(tableInfo, data)


	if b.dialect.HasLastInsertID() {
		result, err2 := b.SqlExec(sql, args...)
		if err2 != nil {
			return db.Error{
				Code: "sql_insert_error",
				Message: err2.Error(),
			}
		}

		id, err2 := result.LastInsertId()
		if err2 == nil && id != 0 {
			strId := strconv.FormatInt(id, 10)
			m.SetID(strId)
		}
	} else {
		rows, err := b.QuerySql(sql, args)
		if err != nil {
			return err
		}
		if len(rows) == 1 {
			data := rows[0]
			db.UpdateModelFromData(info, m, data)	
		}
	}

	// Persist relations again.
	err = db.BackendPersistRelations(b, info, m)
	if err != nil {
		return err
	}

	return nil
}

func (b Backend) selectForModel(info *db.ModelInfo, m db.Model) (*SelectSpec, db.DbError) {
	q := b.Q(m.Collection())

	and := db.And()
	for name := range info.FieldInfo {
		field := info.FieldInfo[name]
		if field.PrimaryKey {
			val, _ := db.GetStructFieldValue(m, name)
			and.Add(db.Eq(field.BackendName, val))
		}
	}

	q.FilterQ(and)

	return b.selectByQuery(q)
}

func (b *Backend) Update(m db.Model) db.DbError {
	info := b.GetModelInfo(m.Collection())
	if info == nil {
		return db.Error{
			Code: "unknown_model",
			Message: fmt.Sprintf("Model %v was not registered with sql backend", m.Collection()),
		}
	}

	data, err := db.ModelToMap(info, m, false)
	if err != nil {
		return err
	}

	// Persist relations.
	err = db.BackendPersistRelations(b, info, m)
	if err != nil {
		return err
	}

	err = b.UpdateByMap(m, data)
	if err != nil {
		return err
	}

	// Persist relations again.
	err = db.BackendPersistRelations(b, info, m)
	if err != nil {
		return err
	}

	return nil
}

func (b Backend) UpdateByMap(m db.Model, rawData map[string]interface{}) db.DbError {
	info := b.GetModelInfo(m.Collection())
	if info == nil {
		return db.Error{
			Code: "unknown_model",
			Message: fmt.Sprintf("Model %v was not registered with sql backend", m.Collection()),
		}
	}

	// Allow to update by struct field name.
	data := make(map[string]interface{})
	for key := range rawData {
		if field, ok := info.FieldInfo[key]; ok {
			data[field.BackendName] = rawData[key]
		} else {
			data[key] = rawData[key]
		}
	}

	spec, err := b.selectForModel(info, m)
	if err != nil {
		return err
	}
	stmt, args := b.dialect.UpdateByMapStatement(spec, data)

	_, err2 := b.SqlExec(stmt, args...)
	if err2 != nil {
		return db.Error{
			Code: "sql_update_error",
			Message: err2.Error(),
		}
	}

	return nil
}

func (b Backend) Delete(m db.Model) db.DbError {
	info := b.GetModelInfo(m.Collection())
	if info == nil {
		return db.Error{
			Code: "unknown_model",
			Message: fmt.Sprintf("Model %v was not registered with sql backend", m.Collection()),
		}
	}

	spec, err := b.selectForModel(info, m)
	if err != nil {
		return err
	}
	stmt, args := b.dialect.DeleteStatement(spec)

	_, err2 := b.SqlExec(stmt, args...)
	if err2 != nil {
		return db.Error{
			Code: "sql_delete_error",
			Message: err2.Error(),
		}
	}

	return nil
}

func (b Backend) DeleteMany(q *db.Query) db.DbError {
	spec, err := b.selectByQuery(q)
	if err != nil {
		return err
	}

	stmt, args := b.dialect.DeleteStatement(spec)

	_, err2 := b.SqlExec(stmt, args...)
	if err2 != nil {
		return db.Error{
			Code: "sql_delete_error",
			Message: err2.Error(),
		}
	}

	return nil
}

/**
 * M2M
 */


func (b Backend) M2M(obj db.Model, name string) (db.M2MCollection, db.DbError) {
	info := b.GetModelInfo(obj.Collection())
	fieldInfo, hasField := info.FieldInfo[name]

	if !hasField {
		return nil, db.Error{
			Code: "unknown_field",
			Message: fmt.Sprintf("The model %v has no field %v", obj.Collection(), name),
		}
	}

	if !fieldInfo.M2M {
		return nil, db.Error{
			Code: "no_m2m_field",
			Message: fmt.Sprintf("The %v on model %v is not m2m", name, obj.Collection()),
		}
	}

	relationInfo := b.GetModelInfo(fieldInfo.RelationItem.Collection())
	if relationInfo == nil {
		return nil, db.Error{
			Code: "unknown_relation_model",
			Message: fmt.Sprintf("Model '%v' not registered with sql backend", fieldInfo.RelationItem.Collection()),
		}
	}

	col := &M2MCollection{
		Backend: &b,
		Model: obj,
		ModelInfo: info,
		RelationInfo: relationInfo,

		Name: name,

		table: fieldInfo.M2MCollection,

		modelField: info.PkField,
		modelColumnName: info.GetPkField().BackendName,
		joinTableModelColumn: info.BackendName + "_" + info.GetPkField().BackendName,

		relationField: relationInfo.PkField,
		relationColumnName: relationInfo.GetPkField().BackendName,
		joinTableRelationColumn: relationInfo.BackendName + "_" + relationInfo.GetPkField().BackendName,
	}

	query, err := col.BuildQuery()
	if err != nil {
		return nil, err
	}
	col.Query = query
	
	items, err := b.Query(col.Query)
	if err != nil {
		return nil, err
	}
	col.Items = items

	return col, nil
}

type M2MCollection struct {
	db.BaseM2MCollection

	Backend *Backend

	Model db.Model
	ModelInfo *db.ModelInfo
	RelationInfo *db.ModelInfo

	Name string

	table string
	
	modelField string
	modelColumnName string
	joinTableModelColumn string

	relationField string
	relationColumnName string
	joinTableRelationColumn string

	Query *db.Query
}

// Ensure that M2MCollection implements the db.M2MCollection interface at compile time.
var _ db.M2MCollection = (*M2MCollection)(nil)

func (c M2MCollection) BuildQuery() (*db.Query, db.DbError) {
	pk, _ := db.GetStructFieldValue(c.Model, c.modelField)
	return c.Backend.Q(c.ModelInfo.Collection).Filter(c.modelField, pk).Related(c.Name).Build()
}

func (c *M2MCollection) Add(items ...db.Model) db.DbError {
	modelId, _ := db.GetStructFieldValue(c.Model, c.modelField)

	for _, item := range items {
		if !c.Contains(item) {
			relationId, _ := db.GetStructFieldValue(item, c.relationField)

			data := map[string]interface{}{
				c.joinTableModelColumn: modelId,
				c.joinTableRelationColumn: relationId,
			}
			stmt, args := c.Backend.dialect.InsertMapStatement(c.Backend.TableInfo[c.table], data)

			_, err := c.Backend.SqlExec(stmt, args...)
			if err != nil {
				return db.Error{
					Code: "sql_insert_error",
					Message: err.Error(),
				}
			}

			c.Items = append(c.Items, item)
		}
	}
	return nil
}

func (c *M2MCollection) Delete(items ...db.Model) db.DbError {
	modelId, _ := db.GetStructFieldValue(c.Model, c.modelField)

	filters := db.Or()

	for _, item := range items {
		relationId, _ := db.GetStructFieldValue(item, c.relationField)

		filter := db.And(db.Eq(c.modelColumnName, modelId), db.Eq(c.relationColumnName, relationId))
		filters.Add(filter)
	}

	where, whereArgs := c.Backend.filterToSql(c.ModelInfo, filters)
	spec := &SelectSpec{
		Table: c.table,
		Where: where,
		WhereArgs: whereArgs,
	}

	stmt, args := c.Backend.dialect.DeleteStatement(spec)
	_, err := c.Backend.SqlExec(stmt, args)
	if err != nil {
		return db.Error{
			Code: "sql_delete_error",
			Message: err.Error(),
		}
	}

	for _, item := range items {
		for index, curItem := range c.Items {
			if curItem.GetID() == item.GetID() {
				c.Items = append(c.Items[:index], c.Items[index+1:]...)
				break
			}
		}
	}
	return nil
}

func (c *M2MCollection) Clear() db.DbError {
	modelId, _ := db.GetStructFieldValue(c.Model, c.modelField)
	filter := db.Eq(c.modelColumnName, modelId)
	where, whereArgs := c.Backend.filterToSql(c.ModelInfo, filter)
	spec := &SelectSpec{
		Table: c.table,
		Where: where,
		WhereArgs: whereArgs,
	}

	stmt, args := c.Backend.dialect.DeleteStatement(spec)
	_, err := c.Backend.SqlExec(stmt, args)
	if err != nil {
		return db.Error{
			Code: "sql_delete_error",
			Message: err.Error(),
		}
	}

	c.Items = make([]db.Model, 0)
	return nil
}

func (c *M2MCollection) Replace(items []db.Model) db.DbError {
	if err := c.Clear(); err != nil {
		return err
	}
	if err := c.Add(items...); err != nil {
		return err
	}
	c.Items = items
	return nil
}
