package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/theduke/go-apperror"

	db "github.com/theduke/go-dukedb"
)

type Backend struct {
	db.BaseBackend

	name string

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

func New(driver, driverOptions string) (*Backend, apperror.Error) {
	b := Backend{}
	b.SetName("sql")

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
		return nil, apperror.Wrap(err, "connection_error")
	}

	b.Db = DB

	b.SetAllModelInfo(make(map[string]*db.ModelInfo))
	b.TableInfo = make(map[string]*TableInfo)
	b.MigrationHandler = db.NewMigrationHandler(&b)

	b.RegisterModel(&MigrationAttempt{})

	return &b, nil
}

func (b *Backend) HasStringIDs() bool {
	return false
}

func (b *Backend) SetDebug(d bool) {
	b.Debug = d
}

func (b *Backend) Copy() db.Backend {
	copied := Backend{
		Db:               b.Db,
		dialect:          b.dialect,
		TableInfo:        b.TableInfo,
		MigrationHandler: b.MigrationHandler,
	}
	copied.SetAllModelInfo(b.AllModelInfo())
	copied.SetDebug(b.GetDebug())
	return &copied
}

func (b *Backend) CreateModel(collection string) (interface{}, apperror.Error) {
	return db.BackendCreateModel(b, collection)
}

func (b *Backend) MustCreateModel(collection string) interface{} {
	return db.BackendMustCreateModel(b, collection)
}

func (b *Backend) MergeModel(model db.Model) {
	db.BackendMergeModel(b, model)
}

func (b *Backend) ModelToMap(m interface{}, marshal bool) (map[string]interface{}, apperror.Error) {
	info, err := b.InfoForModel(m)
	if err != nil {
		return nil, err
	}

	return db.ModelToMap(info, m, false, marshal)
}

func (b *Backend) BuildRelationshipInfo() {
	b.BaseBackend.BuildRelationshipInfo()

	info := b.AllModelInfo()

	// First, build table info for all collections.
	for _, modelInfo := range info {
		tableInfo := b.dialect.BuildTableInfo(modelInfo)
		b.TableInfo[modelInfo.BackendName] = tableInfo
	}

	// Now build m2m tables.
	for model, modelInfo := range info {
		tableInfo := b.TableInfo[modelInfo.BackendName]

		for field := range modelInfo.FieldInfo {
			fieldInfo := modelInfo.FieldInfo[field]
			if !fieldInfo.M2M {
				continue
			}

			relatedInfo := info[fieldInfo.RelationCollection]
			relatedTableInfo := b.TableInfo[relatedInfo.BackendName]

			column := tableInfo.Columns[modelInfo.GetPkField().BackendName]
			pkColumn := &ColumnInfo{
				Name:            modelInfo.BackendName + "_" + column.Name,
				Type:            column.Type,
				NotNull:         true,
				ForeignKey:      column.Name,
				ForeignKeyTable: tableInfo.Name,
			}

			column = relatedTableInfo.Columns[relatedInfo.GetPkField().BackendName]
			foreignColumn := &ColumnInfo{
				Name:            relatedInfo.BackendName + "_" + column.Name,
				Type:            column.Type,
				NotNull:         true,
				ForeignKey:      column.Name,
				ForeignKeyTable: relatedTableInfo.Name,
			}

			if fieldInfo.M2M {
				if fieldInfo.M2MCollection == "" {
					panic(fmt.Sprintf("Model field %v.%v has empty m2mcollection", model, field))
				}

				table := &TableInfo{
					Name: fieldInfo.M2MCollection,
					Columns: map[string]*ColumnInfo{
						pkColumn.Name:      pkColumn,
						foreignColumn.Name: foreignColumn,
					},
					UniqueFields: [][]string{[]string{pkColumn.Name, foreignColumn.Name}},
				}
				b.TableInfo[table.Name] = table
			}
		}
	}
}

func (b *Backend) SqlExec(query string, args ...interface{}) (sql.Result, error) {
	if b.Debug {
		if b.Logger != nil {
			b.Logger.Debugf("%v | %+v", query, args)
		} else {
			fmt.Printf("%v | %v\n", query, args)
		}
	}

	if b.Tx != nil {
		return b.Tx.Exec(query, args...)
	} else {
		return b.Db.Exec(query, args...)
	}
}

func (b *Backend) SqlQuery(query string, args ...interface{}) (*sql.Rows, error) {
	if b.Debug {
		if b.Logger != nil {
			b.Logger.Debugf("%v | %+v", query, args)
		} else {
			fmt.Printf("%v | %v\n", query, args)
		}
	}

	if b.Tx != nil {
		return b.Tx.Query(query, args...)
	} else {
		return b.Db.Query(query, args...)
	}
}

func (b *Backend) sqlScanRow(query string, args []interface{}, vars ...interface{}) apperror.Error {
	rows, err := b.SqlQuery(query, args...)
	if err != nil {
		return apperror.Wrap(err, "sql_error")
	}

	defer rows.Close()
	rows.Next()
	rows.Scan(vars...)
	if err := rows.Err(); err != nil {
		return apperror.Wrap(err, "sql_rows_error")
	}

	return nil
}

func (b *Backend) CreateCollection(collection string) apperror.Error {
	info, err := b.InfoForCollection(collection)
	if err != nil {
		return err
	}

	tableInfo := b.TableInfo[info.BackendName]
	if tableInfo == nil {
		return apperror.New(
			"missing_table_info",
			fmt.Sprintf("There is no table info for the collection %v. Did you forget to call backend.BuildRelationshipInfo() after backend.RegisterModel()?", collection))
	}
	stmt := b.dialect.CreateTableStatement(tableInfo, true)
	if _, err := b.SqlExec(stmt); err != nil {
		return apperror.Wrap(err, "create_table_failed")
	}

	// Change the serial start value of auto-incrementing primary keys to start at 1 instead
	// of zero.
	pkField := info.GetField(info.PkField)
	if pkField.AutoIncrement {
		stmt = b.dialect.AlterAutoIncrementIndexStatement(tableInfo, pkField.BackendName, 1)
		if stmt != "" {
			if _, err := b.SqlExec(stmt); err != nil {
				return apperror.Wrap(err, "alter_sequence_error", "Table was created, but sequence could not be altered to start at 1")
			}
		}
	}

	// Create indexes.
	for name, column := range tableInfo.Columns {
		if column.Index == "" {
			continue
		}

		stmt := b.dialect.CreateIndexStatement(column.Index, tableInfo.Name, []string{name})
		if _, err := b.SqlExec(stmt); err != nil {
			msg := fmt.Sprintf("Could not create index %v on %v.%v", column.Index, tableInfo.Name, column.Name)
			return apperror.Wrap(err, "create_index_failed", msg)
		}
	}

	// Create m2m tables.
	for name := range info.FieldInfo {
		field := info.FieldInfo[name]
		if field.M2M {
			table := b.TableInfo[field.M2MCollection]
			stmt := b.dialect.CreateTableStatement(table, true)
			if _, err := b.SqlExec(stmt); err != nil {
				return apperror.Wrap(err, "create_m2m_table_failed")
			}
		}
	}

	return nil
}

func (b *Backend) CreateCollections(names ...string) apperror.Error {
	for _, name := range names {
		if err := b.CreateCollection(name); err != nil {
			return apperror.Wrap(err, "create_collection_error",
				fmt.Sprintf("Could not create collection %v: %v", name, err))
		}
	}

	return nil
}

func (b *Backend) DropTable(name string, ifExists bool) apperror.Error {
	stmt := b.dialect.DropTableStatement(name, ifExists)
	if _, err := b.SqlExec(stmt); err != nil {
		return apperror.Wrap(err, "drop_table_failed")
	}

	return nil
}

func (b *Backend) DropCollection(collection string) apperror.Error {
	info, err := b.InfoForCollection(collection)
	if err != nil {
		return err
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

func (b *Backend) DropAllCollections() apperror.Error {
	info := b.AllModelInfo()
	for name := range info {
		info := info[name]

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
					info := b.AllModelInfo()
					for collection := range info {
						if info[collection].BackendName == column.ForeignKeyTable {
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

func (b *Backend) Q(collection string) db.Query {
	q := db.Q(collection)
	q.SetBackend(b)
	return q
}

func (b *Backend) filterManyToSql(info *db.ModelInfo, filters []db.Filter, connector string) (string, []interface{}) {
	sql := "("
	args := make([]interface{}, 0)

	count := len(filters)
	for i := 0; i < count; i++ {
		subSql, subArgs := b.filterToSql(info, filters[i])

		sql += subSql
		args = append(args, subArgs...)

		if i < count-1 {
			sql += " " + connector + " "
		}
	}

	sql += ")"

	return sql, args
}

func (b *Backend) filterToSql(info *db.ModelInfo, filter db.Filter) (string, []interface{}) {
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
			slice := reflect.ValueOf(cond.Value)
			if slice.Type().Kind() != reflect.Slice {
				panic("Non-slice condition value for IN query")
			}

			replacements := make([]string, 0)
			for i := 0; i < slice.Len(); i++ {
				replacements = append(replacements, b.dialect.ReplacementCharacter())
				args = append(args, slice.Index(i).Interface())
			}

			sql += " (" + strings.Join(replacements, ",") + ")"
		} else {
			sql += " " + b.dialect.ReplacementCharacter()
			args = append(args, cond.Value)
		}

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

func (b *Backend) selectByQuery(q db.Query) (*SelectSpec, apperror.Error) {
	info, err := b.InfoForCollection(q.GetCollection())
	if err != nil {
		return nil, err
	}

	// Handle filters.
	where := ""
	whereArgs := make([]interface{}, 0)

	filters := q.GetFilters()
	filterLen := len(filters)

	if filters != nil && filterLen > 0 {
		if filterLen == 1 {
			where, whereArgs = b.filterToSql(info, filters[0])
		} else {
			filter := db.And(filters...)
			where, whereArgs = b.filterToSql(info, filter)
		}
	}

	// Handle field specificiaton.
	columns := []string{"*"}

	if len(q.GetFields()) > 0 {
		columns = make([]string, 0)
		for _, field := range q.GetFields() {
			if fieldInfo, ok := info.FieldInfo[field]; ok {
				columns = append(columns, fieldInfo.BackendName)
			} else {
				columns = append(columns, field)
			}
		}
	}

	// Ordering.
	sqlOrder := ""
	orders := q.GetOrders()
	if orders != nil {
		count := len(orders)
		for i := 0; i < count; i++ {
			sqlOrder += orders[i].String()
			if i < count-1 {
				sqlOrder += ", "
			}
		}
	}

	// Handle joins.
	var sqlJoins []Join
	joins := q.GetJoins()
	if joins != nil {
		for _, join := range joins {
			if join.GetJoinType() == "" || join.GetCollection() == "" || join.GetJoinFieldName() == "" || join.GetForeignFieldName() == "" {
				continue
			}

			joinSpec := Join{
				Table:            join.GetCollection(),
				Type:             join.GetJoinType(),
				JoinColumn:       join.GetJoinFieldName(),
				ForeignKeyColumn: join.GetForeignFieldName(),
			}
			sqlJoins = append(sqlJoins, joinSpec)
		}
	}

	return &SelectSpec{
		Table:     info.BackendName,
		Columns:   columns,
		Where:     where,
		WhereArgs: whereArgs,
		Orders:    sqlOrder,
		Limit:     q.GetLimit(),
		Offset:    q.GetOffset(),
		Joins:     sqlJoins,
	}, nil
}

func (b *Backend) BuildRelationQuery(q db.RelationQuery) (db.Query, apperror.Error) {
	return db.BuildRelationQuery(b, nil, q)
}

func (b *Backend) QuerySql(sql string, args []interface{}) ([]map[string]interface{}, apperror.Error) {
	rows, err := b.SqlQuery(sql, args...)
	if err != nil {
		return nil, apperror.Wrap(err, "sql_error")
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, apperror.Wrap(err, "sql_rows_error")
	}

	vals := make([]interface{}, len(cols))
	valPointers := make([]interface{}, len(cols))
	for i, _ := range cols {
		valPointers[i] = &vals[i]
	}

	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		if err := rows.Scan(valPointers...); err != nil {
			return nil, apperror.Wrap(err, "sql_scan_error")
		}

		data := make(map[string]interface{})
		for i, col := range cols {
			val := vals[i]
			if bytes, ok := val.([]byte); ok {
				val = string(bytes)
			}

			data[col] = val
		}
		result = append(result, data)
	}
	if err := rows.Err(); err != nil {
		return nil, apperror.Wrap(err, "sql_rows_error")
	}

	return result, nil
}

func (b *Backend) querySqlModels(info *db.ModelInfo, sql string, args []interface{}) ([]interface{}, apperror.Error) {
	rows, err := b.SqlQuery(sql, args...)
	if err != nil {
		return nil, apperror.Wrap(err, "sql_error")
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, apperror.Wrap(err, "sql_rows_error")
	}

	models := make([]interface{}, 0)

	for rows.Next() {
		model, _ := b.CreateModel(info.Collection)
		modelVal := reflect.ValueOf(model).Elem()

		vals := make([]interface{}, 0)
		for _, col := range cols {
			fieldName := info.MapFieldName(col)
			if fieldName == "" {
				var x interface{}
				vals = append(vals, &x)
			} else {
				vals = append(vals, modelVal.FieldByName(fieldName).Addr().Interface())
			}
		}

		if err := rows.Scan(vals...); err != nil {
			return nil, apperror.Wrap(err, "sql_scan_error")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, apperror.Wrap(err, "sql_rows_error")
	}

	return models, nil
}

func (b *Backend) doQuery(q db.Query) ([]interface{}, apperror.Error) {
	collection := q.GetCollection()
	info, err := b.InfoForCollection(collection)
	if err != nil {
		return nil, err
	}

	// Normalize the query.
	if err := db.NormalizeQuery(info, q); err != nil {
		return nil, err
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

	models := make([]interface{}, 0)
	for _, data := range result {
		model, err := db.BuildModelFromMap(info, data)
		if err != nil {
			return nil, apperror.Wrap(err, "map_to_model_error",
				fmt.Sprintf("Could not convert map to model: %v", err))
		}
		models = append(models, model)
	}

	// Do joins.
	if len(q.GetJoins()) > 0 {
		if err := db.BackendDoJoins(b, q.GetCollection(), models, q.GetJoins()); err != nil {
			return nil, apperror.Wrap(err, "join_error")
		}
	}

	if q.GetJoinType() == "m2m" {

		// For m2m, a custom join result assigner has to be used,
		// because the resulting models do not contain the m2m fields
		// neccessary for mapping the join result.
		q.SetJoinResultAssigner(func(objs, joinedModels []interface{}, joinQ db.RelationQuery) {
			targetField := joinQ.GetRelationName()
			joinedField := joinQ.GetJoinFieldName()

			joinField := joinQ.GetForeignFieldName()
			parts := strings.Split(joinField, ".")
			joinField = parts[len(parts)-1]

			parentCollection := joinQ.GetBaseQuery().GetCollection()
			parentInfo := b.ModelInfo(parentCollection)
			joinedFieldType := parentInfo.GetField(joinedField).Type

			mapper := make(map[interface{}][]interface{})
			for index, row := range result {
				val := row[joinField]

				// Need to convert the value to the proper type.
				convertedVal, err := db.Convert(val, joinedFieldType)
				if err != nil {
					panic(err)
				}

				mapper[convertedVal] = append(mapper[convertedVal], joinedModels[index])
			}

			for _, model := range objs {
				val, err := db.GetStructFieldValue(model, joinedField)
				if err != nil {
					panic("Join result assignment error: " + err.Error())
				}

				if joins, ok := mapper[val]; ok && len(joins) > 0 {
					db.SetStructModelField(model, targetField, joins)
				}
			}
		})
	}

	return models, nil
}

// Perform a query.
func (b *Backend) Query(q db.Query, targetSlice ...interface{}) ([]interface{}, apperror.Error) {
	res, err := b.doQuery(q)
	return db.BackendQuery(b, q, targetSlice, res, err)
}

func (b *Backend) QueryOne(q db.Query, targetModel ...interface{}) (interface{}, apperror.Error) {
	return db.BackendQueryOne(b, q, targetModel)
}

func (b *Backend) Count(q db.Query) (int, apperror.Error) {
	_, err := b.InfoForCollection(q.GetCollection())
	if err != nil {
		return 0, err
	}

	newQ := b.Q(q.GetCollection()).Fields("COUNT(*) as count")
	newQ.SetFilters(q.GetFilters()...)

	spec, err := b.selectByQuery(newQ)
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

func (b *Backend) Last(q db.Query, targetModel ...interface{}) (interface{}, apperror.Error) {
	return db.BackendLast(b, q, targetModel)
}

// Find first model with primary key ID.
func (b *Backend) FindOne(modelType string, id interface{}, targetModel ...interface{}) (interface{}, apperror.Error) {
	return db.BackendFindOne(b, modelType, id, targetModel)
}

func (b *Backend) FindBy(modelType, field string, value interface{}, targetModel ...interface{}) ([]interface{}, apperror.Error) {
	return db.BackendFindBy(b, modelType, field, value, targetModel)
}

func (b *Backend) FindOneBy(modelType, field string, value interface{}, targetModel ...interface{}) (interface{}, apperror.Error) {
	return db.BackendFindOneBy(b, modelType, field, value, targetModel)
}

// Auto-persist related models.

func (b *Backend) Create(m interface{}) apperror.Error {
	return db.BackendCreate(b, m, func(info *db.ModelInfo, m interface{}) apperror.Error {
		data, err := db.ModelToMap(info, m, true, false)
		if err != nil {
			return err
		}

		// Remove primary key from data if it is the zero value.
		pkField := info.GetField(info.PkField).BackendName
		if pk, ok := data[pkField]; ok {
			if db.IsZero(pk) {
				delete(data, pkField)
			}
		}

		if len(data) == 0 {
			return apperror.New("empty_model_data", "Can't create a model with empty data")
		}

		tableInfo := b.TableInfo[info.BackendName]
		sql, args := b.dialect.InsertMapStatement(tableInfo, data)

		if b.dialect.HasLastInsertID() {
			result, err2 := b.SqlExec(sql, args...)
			if err2 != nil {
				return apperror.Wrap(err2, "sql_insert_error")
			}

			id, err2 := result.LastInsertId()
			if err2 == nil && id != 0 {
				if err := b.SetModelID(m, id); err != nil {
					return err
				}
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

		return nil
	})
}

func (b *Backend) selectForModel(info *db.ModelInfo, m interface{}) (*SelectSpec, apperror.Error) {
	collection, err := db.GetModelCollection(m)
	if err != nil {
		return nil, err
	}

	q := b.Q(collection)

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

func (b *Backend) Update(m interface{}) apperror.Error {
	return db.BackendUpdate(b, m, func(info *db.ModelInfo, m interface{}) apperror.Error {
		data, err := db.ModelToMap(info, m, true, false)
		if err != nil {
			return err
		}

		err = b.UpdateByMap(m, data)
		if err != nil {
			return err
		}

		return nil
	})
}

func (b *Backend) UpdateByMap(m interface{}, rawData map[string]interface{}) apperror.Error {
	info, err := b.InfoForModel(m)
	if err != nil {
		return err
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
		return apperror.Wrap(err2, "sql_update_error")
	}

	return nil
}

func (b *Backend) Delete(m interface{}) apperror.Error {
	return db.BackendDelete(b, m, func(info *db.ModelInfo, m interface{}) apperror.Error {
		spec, err := b.selectForModel(info, m)
		if err != nil {
			return err
		}
		stmt, args := b.dialect.DeleteStatement(spec)

		_, err2 := b.SqlExec(stmt, args...)
		if err2 != nil {
			return apperror.Wrap(err2, "sql_delete_error")
		}

		return nil
	})
}

func (b *Backend) DeleteMany(q db.Query) apperror.Error {
	spec, err := b.selectByQuery(q)
	if err != nil {
		return err
	}

	stmt, args := b.dialect.DeleteStatement(spec)

	_, err2 := b.SqlExec(stmt, args...)
	if err2 != nil {
		return apperror.Wrap(err2, "sql_delete_error")
	}

	return nil
}

/**
 * Related.
 */

func (b *Backend) Related(model interface{}, name string) (db.RelationQuery, apperror.Error) {
	return db.BackendRelated(b, model, name)
}

/**
 * M2M
 */

func (b *Backend) M2M(obj interface{}, name string) (db.M2MCollection, apperror.Error) {
	info, err := b.InfoForModel(obj)
	if err != nil {
		return nil, err
	}

	fieldInfo, hasField := info.FieldInfo[name]

	if !hasField {
		return nil, &apperror.Err{
			Code:    "unknown_field",
			Message: fmt.Sprintf("The model %v has no field %v", info.Collection, name),
		}
	}

	if !fieldInfo.M2M {
		return nil, &apperror.Err{
			Code:    "no_m2m_field",
			Message: fmt.Sprintf("The %v on model %v is not m2m", name, info.Collection),
		}
	}

	relationInfo := b.ModelInfo(fieldInfo.RelationCollection)
	if relationInfo == nil {
		return nil, &apperror.Err{
			Code:    "unknown_relation_model",
			Message: fmt.Sprintf("Model '%v' not registered with sql backend", fieldInfo.RelationCollection),
		}
	}

	col := &M2MCollection{
		BaseM2MCollection: db.BaseM2MCollection{
			Backend: b,
		},

		Backend:      b,
		Model:        obj,
		ModelInfo:    info,
		RelationInfo: relationInfo,

		Name: name,

		table: fieldInfo.M2MCollection,

		modelField:           info.PkField,
		modelColumnName:      info.GetPkField().BackendName,
		joinTableModelColumn: info.BackendName + "_" + info.GetPkField().BackendName,

		relationField:           relationInfo.PkField,
		relationColumnName:      relationInfo.GetPkField().BackendName,
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

	Model        interface{}
	ModelInfo    *db.ModelInfo
	RelationInfo *db.ModelInfo

	Name string

	table string

	modelField           string
	modelColumnName      string
	joinTableModelColumn string

	relationField           string
	relationColumnName      string
	joinTableRelationColumn string

	Query db.Query
}

// Ensure that M2MCollection implements the db.M2MCollection interface at compile time.
var _ db.M2MCollection = (*M2MCollection)(nil)

func (c M2MCollection) BuildQuery() (db.Query, apperror.Error) {
	pk, _ := db.GetStructFieldValue(c.Model, c.modelField)
	return c.Backend.Q(c.ModelInfo.Collection).Filter(c.modelField, pk).Related(c.Name).Build()
}

func (c *M2MCollection) Add(items ...interface{}) apperror.Error {
	modelId, _ := db.GetStructFieldValue(c.Model, c.modelField)

	for _, item := range items {
		if !c.Contains(item) {
			relationId, _ := db.GetStructFieldValue(item, c.relationField)

			data := map[string]interface{}{
				c.joinTableModelColumn:    modelId,
				c.joinTableRelationColumn: relationId,
			}
			stmt, args := c.Backend.dialect.InsertMapStatement(c.Backend.TableInfo[c.table], data)

			_, err := c.Backend.SqlExec(stmt, args...)
			if err != nil {
				return apperror.Wrap(err, "sql_insert_error")
			}

			c.Items = append(c.Items, item)
		}
	}
	return nil
}

func (c *M2MCollection) Delete(items ...interface{}) apperror.Error {
	modelId, _ := db.GetStructFieldValue(c.Model, c.modelField)

	filters := db.Or()

	for _, item := range items {
		relationId, _ := db.GetStructFieldValue(item, c.relationField)

		filter := db.And(db.Eq(c.modelColumnName, modelId), db.Eq(c.joinTableModelColumn, relationId))
		filters.Add(filter)
	}

	where, whereArgs := c.Backend.filterToSql(c.ModelInfo, filters)
	spec := &SelectSpec{
		Table:     c.table,
		Where:     where,
		WhereArgs: whereArgs,
	}

	stmt, args := c.Backend.dialect.DeleteStatement(spec)
	_, err := c.Backend.SqlExec(stmt, args)
	if err != nil {
		return apperror.Wrap(err, "sql_delete_error")
	}

	for _, item := range items {
		for index, curItem := range c.Items {
			curItemId := c.Backend.MustModelID(curItem)
			itemId := c.Backend.MustModelID(item)

			if curItemId == itemId {
				c.Items = append(c.Items[:index], c.Items[index+1:]...)
				break
			}
		}
	}
	return nil
}

func (c *M2MCollection) Clear() apperror.Error {
	modelId, _ := db.GetStructFieldValue(c.Model, c.modelField)
	filter := db.Eq(c.joinTableModelColumn, modelId)
	where, whereArgs := c.Backend.filterToSql(c.ModelInfo, filter)
	spec := &SelectSpec{
		Table:     c.table,
		Where:     where,
		WhereArgs: whereArgs,
	}

	stmt, args := c.Backend.dialect.DeleteStatement(spec)
	_, err := c.Backend.SqlExec(stmt, args...)
	if err != nil {
		return apperror.Wrap(err, "sql_delete_error")
	}

	c.Items = make([]interface{}, 0)
	return nil
}

func (c *M2MCollection) Replace(items []interface{}) apperror.Error {
	if err := c.Clear(); err != nil {
		return err
	}
	if err := c.Add(items...); err != nil {
		return err
	}
	c.Items = items
	return nil
}
