package gorm

import (
	"reflect"
	"fmt"

	"github.com/jinzhu/gorm"

	db "github.com/theduke/dukedb"
)


type Backend struct {
	db.BaseBackend
	Db *gorm.DB

	MigrationHandler *db.MigrationHandler
}

func New(gorm *gorm.DB) *Backend {
	b := Backend{
		Db: gorm,
	}

	b.ModelInfo = make(map[string]*db.ModelInfo)
	b.MigrationHandler = db.NewMigrationHandler(&b)

	return &b
}

func (b Backend) GetName() string {
	return "gorm"
}

func (b Backend) Copy() db.Backend {
	copied := Backend{
		Db: b.Db,
	}
	copied.ModelInfo = b.ModelInfo
	copied.SetDebug(b.Debug())
	return &copied
}


func (b *Backend) Q(model string) *db.Query {
	q := db.Q(model)
	q.Backend = b
	return q
}

func filterManyToSql(filters []db.Filter, connector string) (string, []interface{}) {
	sql := "("
	args := make([]interface{}, 0)	

	count := len(filters)
	for i := 0; i < count; i++ {
		subSql, subArgs := filterToSql(filters[i])

		sql += subSql
		args = append(args, subArgs...)

		if i < count -1 {
			sql += " " + connector + " "
		}
	}

	sql += ")"

	return sql, args
}

func filterToSql(filter db.Filter) (string, []interface{}) {
	filterType := reflect.TypeOf(filter).Elem().Name()
	filterName := filter.Type()

	sql := ""
	args := make([]interface{}, 0)

	if filterType == "FieldCondition" {
		// fieldCOnditions can easily be handled generically.
		cond := filter.(*db.FieldCondition)

		operator := db.FilterToCondition(filterName)

		sql = cond.Field + " " + operator 
		if filterName == "in" {
			sql += " (?)"
		} else {
			sql += " ?"
		}

		args = append(args, cond.Value)

		return sql, args
	}

	if filterName == "and" {
		and := filter.(*db.AndCondition)
		sql, args = filterManyToSql(and.Filters, "AND")
	} else if filterName == "or" {
		or := filter.(*db.OrCondition)
		sql, args = filterManyToSql(or.Filters, "OR")
	} else if filterName == "not" {
		not := filter.(*db.NotCondition)
		sql, args = filterToSql(not.Filter)
		sql = "NOT (" + sql + ")"
	} else {
		panic(fmt.Sprintf("GORM: Unhandled filter type '%v'", filterType))
	}

	return sql, args
}

func (b Backend) buildQuery(q *db.Query) (*gorm.DB, db.DbError) {
	gormQ := b.Db
	if b.Debug() {
		gormQ = gormQ.Debug()
	}

	// Handle filters.
	for _, filter := range q.Filters {
		sql, args := filterToSql(filter)	
		gormQ = gormQ.Where(sql, args...)
	}

	// Handle joins.
	/*
	if len(q.Joins) > 0 {
		for _, joinQ := range q.Joins {
			args := make([]interface{}, 0)

			// Join has filters!
			// Construct sql and arguments for gorm.Preload("name", "where", "args"...)	
			if len(joinQ.Filters) > 0 {
				sql, queryArgs := filterManyToSql(joinQ.Filters, "AND")
				args = append(args, interface{}(sql))
				args = append(args, queryArgs...)
			}

			if joinQ.JoinField != "" || joinQ.JoinedField != "" {
				panic("gorm_backend_does_not_support_join_keys")
			}

			gormQ = gormQ.Preload(joinQ.JoinTargetField, args...)
		}
	}
	*/

	// Handle field specificiaton.
	if len(q.FieldSpec) > 0 {
		gormQ = gormQ.Select(q.FieldSpec)
	}

	// Ordering.
	if q.Orders != nil {
		sql := ""

		count := len(q.Orders)
		for i := 0; i < count; i++ {
			sql += q.Orders[i].String()
			if i < count - 1 {
				sql += ", "
			}
		}

		gormQ = gormQ.Order(sql)
	}

	// Limit & Offset.

	if q.LimitNum != 0 {
		gormQ = gormQ.Limit(q.LimitNum)
	}
	if q.OffsetNum != 0 {
		gormQ = gormQ.Offset(q.OffsetNum)
	}


	return gormQ, nil
}

// Perform a query.	
func (b Backend) Query(q *db.Query) ([]db.Model, db.DbError) {
	slice, err := b.NewModelSlice(q.Model)
	if err != nil {
		return nil, err
	}

	gormQ, err := b.buildQuery(q)
	if err != nil {
		return nil, err
	}

	if err := gormQ.Find(slice).Error; err != nil {
		return nil, db.Error{Code: "db_error", Message: err.Error()}
	}

	models := db.InterfaceToModelSlice(slice)

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
	res, err := b.Query(q)
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, nil
	}

	m := res[0].(db.Model)
	return m, nil
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

// Convenience methods.
	 
func (b Backend) Create(m db.Model) db.DbError {
	if err := b.Db.Create(m).Error; err != nil {
		return db.Error{Code: "gorm_error", Message: err.Error()}
	}

	return nil
}

func (b Backend) Update(m db.Model) db.DbError {
	if err := b.Db.Save(m).Error; err != nil {
		return db.Error{Code: "gorm_error", Message: err.Error()}
	}

	return nil
}

func (b Backend) Delete(m db.Model) db.DbError {
	if err := b.Db.Delete(m).Error; err != nil {
		return db.Error{Code: "gorm_error", Message: err.Error()}
	}

	return nil
}
