package dukedb

import (
	"fmt"

	"github.com/theduke/go-apperror"
)

/**
 * Query.
 */

type Query struct {
	// backend can optionally hold the backend where the model resides.
	// This must be set for the convenience functions like .Find() to work.
	backend Backend

	// name is an optional identifier for the query (for profiling, caching, etc).
	name string

	// statement is the SelectStatement.
	statement SelectStatement

	// joinResultAssigner can hold a function that will take care of assigning the results
	// of a join query to the parent models. This is needed for m2m joins, since models
	// obtained by executing the query will not hold the neccessary fields for mapping
	// the query result to the parent objects.
	// For example, the SQL backend will use a closure to keep track of the raw query
	// result and assign based on it.
	joinResultAssigner JoinAssigner
}

func Q(collection string) *Query {
	return &Query{
		statement: &selectStmt{
			collection: collection,
		},
	}
}

func (q *Query) GetStatement() SelectStatement {
	return q.statement
}

func (q *Query) GetCollection() string {
	return q.statement.Collection()
}

func (q *Query) GetName() string {
	return q.name
}

func (q *Query) SetName(x string) {
	q.statement.SetName(x)
	q.name = x
}

func (q *Query) GetJoinResultAssigner() JoinAssigner {
	return q.joinResultAssigner
}

func (q *Query) SetJoinResultAssigner(x JoinAssigner) {
	q.joinResultAssigner = x
}

/**
 * Limit methods.
 */

func (q *Query) Limit(limit int) *Query {
	q.statement.SetLimit(limit)
	return q
}

func (q *Query) GetLimit() int {
	return q.statement.Limit()
}

/**
 * Offset methods.
 */

func (q *Query) Offset(offset int) *Query {
	q.statement.SetOffset(offset)
	return q
}

func (q *Query) GetOffset() int {
	return q.statement.Offset()
}

/**
 * Fields methods.
 */

func (q *Query) Field(fields ...string) *Query {
	q.statement.SetFields(make([]Expression, 0))
	for _, field := range fields {
		// Use a named expression to allow join queries without extra work.
		// Named queries will construct a SQL query with '"collection"."field" AS "collection.field"'
		expr := NameExpr(q.GetCollection()+"."+field, ColFieldIdExpr(q.GetCollection(), field))
		q.statement.AddField(expr)
	}
	return q
}

func (q *Query) FieldExpr(exprs ...Expression) *Query {
	q.statement.AddField(exprs...)
	return q
}

func (q *Query) SetFields(fields []string) *Query {
	q.statement.SetFields(nil)
	return q.Field(fields...)
}

func (q *Query) SetFieldExpressions(expressions []Expression) *Query {
	q.statement.SetFields(expressions)
	return q
}

/**
 * Sort methods.
 */

func (q *Query) Sort(field string, asc bool) *Query {
	q.statement.AddSort(SortExpr(ColFieldIdExpr(q.GetCollection(), field), asc))
	return q
}

func (q *Query) SortExpr(expr SortExpression) *Query {
	q.statement.AddSort(expr)
	return q
}

func (q *Query) SetSorts(exprs []SortExpression) *Query {
	q.statement.SetSorts(exprs)
	return q
}

/**
 * Filter methods.
 */

func (q *Query) FilterExpr(expressions ...Expression) *Query {
	for _, expr := range expressions {
		q.statement.FilterAnd(expr)
	}
	return q
}

func (q *Query) SetFilters(expressions ...Expression) *Query {
	if len(expressions) == 0 {
		// No Filter.
		q.statement.SetFilter(nil)
	} else if len(expressions) == 1 {
		// Single filter.
		q.statement.SetFilter(expressions[0])
	} else {
		// Multiple filters, so create an AndExpr wrapper.
		q.statement.SetFilter(AndExpr(expressions...))
	}
	return q
}

func (q *Query) Filter(field string, val interface{}) *Query {
	return q.FilterExpr(Eq(q.GetCollection(), field, val))
}

func (q *Query) FilterCond(field string, condition string, val interface{}) *Query {
	operator := MapOperator(condition)
	if operator == "" {
		panic(fmt.Sprintf("Unknown operator: '%v'", operator))
	}
	return q.FilterExpr(FieldValFilter(q.GetCollection(), field, operator, val))
}

func (q *Query) AndExpr(filters ...Expression) *Query {
	return q.FilterExpr(filters...)
}

func (q *Query) And(field string, val interface{}) *Query {
	return q.Filter(field, val)
}

func (q *Query) AndCond(field, condition string, val interface{}) *Query {
	return q.FilterCond(field, condition, val)
}

func (q *Query) OrExpr(filters ...Expression) *Query {
	for _, f := range filters {
		q.statement.FilterOr(f)
	}
	return q
}

func (q *Query) Or(field string, val interface{}) *Query {
	return q.OrExpr(Eq(q.GetCollection(), field, val))
}

func (q *Query) OrCond(field string, condition string, val interface{}) *Query {
	operator := MapOperator(condition)
	if operator == "" {
		panic(fmt.Sprintf("Unknown operator: '%v'", operator))
	}
	return q.OrExpr(FieldValFilter(q.GetCollection(), field, operator, val))
}

func (q *Query) NotExpr(filters ...Expression) *Query {
	for _, f := range filters {
		q.FilterExpr(NotExpr(f))
	}
	return q
}

func (q *Query) Not(field string, val interface{}) *Query {
	return q.FilterExpr(NotExpr(Eq(q.GetCollection(), field, val)))
}

func (q *Query) NotCond(field string, condition string, val interface{}) *Query {
	operator := MapOperator(condition)
	if operator == "" {
		panic(fmt.Sprintf("Unknown operator: '%v'", operator))
	}
	return q.NotExpr(FieldValFilter(q.GetCollection(), field, operator, val))
}

/**
 * Joins.
 */

func (q *Query) JoinQ(jqs ...*RelationQuery) *Query {
	for _, jq := range jqs {
		q.statement.AddJoin(jq.GetStatement())
	}
	return q
}

func (q *Query) Join(fieldName string, joinType ...string) *Query {
	typ := JOIN_LEFT
	if len(joinType) > 0 {
		if len(joinType) > 1 {
			panic("Called Query.Join() with more than one joinType")
		}
		typ = joinType[0]
	}
	q.statement.AddJoin(JoinStmt(fieldName, typ, nil))
	return q
}

// Retrieve a join query for the specified field.
// Returns a *RelationQuery, or nil if not found.
// Supports nested Joins like 'Parent.Tags'.
func (q *Query) GetJoin(field string) *RelationQuery {
	stmt := q.statement.GetJoin(field)
	if stmt == nil {
		return nil
	}

	return &RelationQuery{
		baseQuery: q,
		statement: stmt,
	}
}

func (q *Query) GetJoins() []*RelationQuery {
	jqs := make([]*RelationQuery, 0)
	for _, stmt := range q.statement.Joins() {
		q := &RelationQuery{
			baseQuery: q,
			statement: stmt,
		}
		jqs = append(jqs, q)
	}
	return jqs
}

/**
 * Related.
 */

func (q *Query) Related(name string) *RelationQuery {
	return RelQ(q, name, JOIN_INNER)
}

func (q *Query) RelatedCustom(name, collection, joinKey, foreignKey, typ string) *RelationQuery {
	return RelQCustom(q, name, collection, joinKey, foreignKey, typ)
}

/**
 * Backend functions.
 */

func (q *Query) GetBackend() Backend {
	return q.backend
}

func (q *Query) SetBackend(x Backend) {
	q.backend = x
}

func (q *Query) Find(targetSlice ...interface{}) ([]interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Calling .Find() on query without backend")
	}

	return q.backend.Query(q, targetSlice...)
}

func (q *Query) First(targetModel ...interface{}) (interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Calling .First() on query without backend")
	}

	return q.backend.QueryOne(q, targetModel...)
}

func (q *Query) Last(targetModel ...interface{}) (interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Calling .Last() on query without backend")
	}
	return q.backend.Last(q, targetModel...)
}

func (q *Query) Count() (int, apperror.Error) {
	if q.backend == nil {
		panic("Calling .Count() on query without backend")
	}
	return q.backend.Count(q)
}

func (q *Query) Delete() apperror.Error {
	if q.backend == nil {
		panic("Calling .Delete() on query without backend")
	}
	return q.backend.DeleteQ(q)
}

/**
 * RelationQuery.
 */

type RelationQuery struct {
	Query

	baseQuery *Query
	statement JoinStatement
}

func RelQ(q *Query, name string, joinType string) *RelationQuery {
	stmt := JoinStmt(name, joinType, nil)
	stmt.SetParentSelect(q.GetStatement())

	relQ := &RelationQuery{
		baseQuery: q,
		statement: stmt,
	}
	relQ.Query.statement = stmt.SelectStatement()
	relQ.SetBackend(q.GetBackend())

	return relQ
}

func RelQCustom(q *Query, name, collection, joinKey, foreignKey, typ string) *RelationQuery {
	filter := FilterExpr(
		ColFieldIdExpr(q.GetCollection(), joinKey),
		OPERATOR_EQ,
		ColFieldIdExpr(collection, foreignKey))

	stmt := JoinStmt(name, typ, filter)
	stmt.SetParentSelect(q.GetStatement())

	relQ := &RelationQuery{
		baseQuery: q,
		statement: stmt,
	}
	relQ.Query.statement = stmt.SelectStatement()
	relQ.SetBackend(q.GetBackend())

	return relQ
}

// RelationQuery specific methods.

func (q *RelationQuery) GetStatement() JoinStatement {
	return q.statement
}

func (q *RelationQuery) GetBaseQuery() *Query {
	return q.baseQuery
}

func (q *RelationQuery) SetBaseQuery(bq *Query) {
	q.baseQuery = bq
}

func (q *RelationQuery) GetRelationName() string {
	return q.statement.RelationName()
}

func (q *RelationQuery) SetRelationName(name string) {
	q.statement.SetRelationName(name)
}

func (q *RelationQuery) GetJoinType() string {
	return q.statement.JoinType()
}

func (q *RelationQuery) SetJoinType(typ string) *RelationQuery {
	q.statement.SetJoinType(typ)
	return q
}

func (q *RelationQuery) Build() (*Query, apperror.Error) {
	if q.backend == nil {
		panic("Callind .Find() on a query without backend")
	}
	return q.backend.BuildRelationQuery(q)
}

// Backend methods.

func (q *RelationQuery) Find(targetSlice ...interface{}) ([]interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Callind .Find() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.Find(targetSlice...)
}

func (q *RelationQuery) First(targetModel ...interface{}) (interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Calling .First() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.First(targetModel...)
}

func (q *RelationQuery) Last(targetModel ...interface{}) (interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Calling .Last() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.Last(targetModel...)
}

func (q *RelationQuery) Count() (int, apperror.Error) {
	if q.backend == nil {
		panic("Calling .Count() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return 0, err
	}
	return newQ.Count()
}

func (q *RelationQuery) Delete() apperror.Error {
	if q.backend == nil {
		panic("Calling .Delete() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return err
	}
	return newQ.Delete()
}

// Query methods.

/**
 * Limit methods.
 */

func (q *RelationQuery) Limit(l int) *RelationQuery {
	q.Query.Limit(l)
	return q
}

/**
 * Offset methods.
 */

func (q *RelationQuery) Offset(o int) *RelationQuery {
	q.Query.Offset(o)
	return q
}

/**
 * Fields methods.
 */

func (q *RelationQuery) Field(fields ...string) *RelationQuery {
	q.Query.Field(fields...)
	return q
}

func (q *RelationQuery) FieldExpr(exprs ...Expression) *RelationQuery {
	q.Query.FieldExpr(exprs...)
	return q
}

func (q *RelationQuery) SetFields(fields []string) *RelationQuery {
	q.Query.SetFields(fields)
	return q
}

func (q *RelationQuery) SetFieldExpressions(expressions []Expression) *RelationQuery {
	q.Query.SetFieldExpressions(expressions)
	return q
}

/*
func (q *RelationQuery) LimitFields(fields ...string) *RelationQuery {
	q.Query.LimitFields(fields...)
	return q
}
*/

/**
 * Sort methods.
 */

func (q *RelationQuery) Sort(name string, asc bool) *RelationQuery {
	q.Query.Sort(name, asc)
	return q
}

func (q *RelationQuery) SortExpr(expr SortExpression) *RelationQuery {
	q.Query.SortExpr(expr)
	return q
}

func (q *RelationQuery) SetSorts(exprs []SortExpression) *RelationQuery {
	q.Query.SetSorts(exprs)
	return q
}

/**
 * Filter methods.
 */

func (q *RelationQuery) FilterExpr(expressions ...Expression) *RelationQuery {
	q.Query.FilterExpr(expressions...)
	return q
}

func (q *RelationQuery) SetFilters(expressions ...Expression) *RelationQuery {
	q.Query.SetFilters(expressions...)
	return q
}

func (q *RelationQuery) Filter(field string, val interface{}) *RelationQuery {
	q.Query.Filter(field, val)
	return q
}

func (q *RelationQuery) FilterCond(field string, condition string, val interface{}) *RelationQuery {
	q.Query.FilterCond(field, condition, val)
	return q
}

func (q *RelationQuery) AndExpr(filters ...Expression) *RelationQuery {
	q.Query.AndExpr(filters...)
	return q
}

func (q *RelationQuery) And(field string, val interface{}) *RelationQuery {
	q.Query.And(field, val)
	return q
}

func (q *RelationQuery) AndCond(field, condition string, val interface{}) *RelationQuery {
	q.Query.AndCond(field, condition, val)
	return q
}

func (q *RelationQuery) OrExpr(filters ...Expression) *RelationQuery {
	q.Query.OrExpr(filters...)
	return q
}

func (q *RelationQuery) Or(field string, val interface{}) *RelationQuery {
	q.Query.Or(field, val)
	return q
}

func (q *RelationQuery) OrCond(field string, condition string, val interface{}) *RelationQuery {
	q.Query.OrCond(field, condition, val)
	return q
}

func (q *RelationQuery) NotExpr(filters ...Expression) *RelationQuery {
	q.Query.NotExpr(filters...)
	return q
}

func (q *RelationQuery) Not(field string, val interface{}) *RelationQuery {
	q.Query.Not(field, val)
	return q
}

func (q *RelationQuery) NotCond(field string, condition string, val interface{}) *RelationQuery {
	q.Query.NotCond(field, condition, val)
	return q
}

/**
 * Joins.
 */

func (q *RelationQuery) JoinQ(jqs ...*RelationQuery) *RelationQuery {
	q.Query.JoinQ(jqs...)
	return q
}

func (q *RelationQuery) Join(fieldName string, joinType ...string) *RelationQuery {
	q.Query.Join(fieldName, joinType...)
	return q
}

func (q *RelationQuery) GetJoin(field string) *RelationQuery {
	q.Query.GetJoin(field)
	return q
}

func (q *RelationQuery) GetJoins() []*RelationQuery {
	return q.Query.GetJoins()
}
