package dukedb

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/theduke/go-apperror"
)

/**
 * DbQuery.
 */

type DbQuery struct {
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

// Ensure DbQuery implements Query.
var _ Query = (*DbQuery)(nil)

func Q(collection string) Query {
	return &DbQuery{
		statement: &SelectStatement{
			Collection: collection,
		},
	}
}

func (q *DbQuery) GetCollection() string {
	return q.statement.Collection
}

func (q *DbQuery) GetName() string {
	return q.name
}

func (q *DbQuery) SetName(x string) {
	q.name = x
}

func (q *DbQuery) GetJoinResultAssigner() JoinAssigner {
	return q.joinResultAssigner
}

func (q *DbQuery) SetJoinResultAssigner(x JoinAssigner) {
	q.joinResultAssigner = x
}

/**
 * Limit methods.
 */

func (q *DbQuery) Limit(l int) Query {
	q.statement.Limit = l
	return q
}

func (q *DbQuery) GetLimit() int {
	return q.statement.Limit
}

/**
 * Offset methods.
 */

func (q *DbQuery) Offset(o int) Query {
	q.statement.Offset = o
	return q
}

func (q *DbQuery) GetOffset() int {
	return q.statement.Offset
}

/**
 * Fields methods.
 */

func (q *DbQuery) Field(fields ...string) Query {
	q.statement.Fields = make([]Expression, 0)
	for _, field := range fields {
		q.statement.AddField(Identifier(field))
	}
	return q
}

func (q *DbQuery) FieldExpr(exprs ...Expression) Query {
	q.statement.AddField(exprs...)
	return q
}

func (q *DbQuery) SetFields(fields []string) Query {
	q.statement.Fields = nil
	return q.Field(fields...)
}

func (q *DbQuery) SetFieldExpressions(expressions []Expression) Query {
	q.statement.Fields = expressions
	return q
}

/**
 * Limit the query to specified fields.
 * If fields where already specified, they will be reduced.
 */
func (q *DbQuery) LimitFields(fields ...string) Query {
	if q.statement.Fields == nil {
		return q.Field(fields...)
	}

	allowMap := make(map[string]bool)
	for _, field := range fields {
		allowMap[field] = true
	}

	usedFields := make([]string, 0)
	for _, expr := range q.statement.Fields {
		usedFields = append(usedFields, expr.GetIdentifiers()...)
	}

	finalFields := make([]string, 0)
	for _, identifier := range usedFields {
		if _, ok := allowMap[field]; ok {
			finalFields = append(finalFields, field)
		}
	}

	return q.statement.SetFields(finalFields)
}

/**
 * Sort methods.
 */

func (q *DbQuery) Sort(name string, asc bool) Query {
	q.statement.AddSort(Sort(name, asc))
	return q
}

func (q *DbQuery) SortExpr(expr *SortExpression) Query {
	q.statement.AddSort(expr)
	return q
}

func (q *DbQuery) SetSorts(exprs []SortExpression) Query {
	q.statement.Sorts = exprs
	return q
}

/**
 * Filter methods.
 */

func (q *DbQuery) FilterExpr(expressions ...Expression) Query {
	for _, expr := range expressions {
		q.statement.FilterAnd(expr)
	}
	return q
}

func (q *DbQuery) SetFilters(expressions ...Expression) Query {
	q.statement.Filter = And(expressions...)
	return q
}

func (q *DbQuery) Filter(field string, val interface{}) Query {
	return q.FilterQ(Eq(field, val))
}

func (q *DbQuery) FilterCond(field string, condition string, val interface{}) Query {
	operator := MapOperator(condition)
	if operator == "" {
		panic(fmt.Sprintf("Unknown operator: '%v'", operator))
	}
	return q.FilterExpr(ValFilter(field, operator, val))
}

func (q *DbQuery) AndExpr(filters ...Expession) Query {
	return q.FilterExpr(filters...)
}

func (q *DbQuery) And(field string, val interface{}) Query {
	return q.Filter(field, val)
}

func (q *DbQuery) AndCond(field, condition string, val interface{}) Query {
	return q.FilterCond(field, condition, val)
}

func (q *DbQuery) OrExpr(filters ...Expression) Query {
	for _, f := range filters {
		q.statement.FilterOr(f)
	}
	return q
}

func (q *DbQuery) Or(field string, val interface{}) Query {
	return q.OrExpr(Eq(field, val))
}

func (q *DbQuery) OrCond(field string, condition string, val interface{}) Query {
	operator := MapOperator(condition)
	if operator == "" {
		panic(fmt.Sprintf("Unknown operator: '%v'", operator))
	}
	return q.OrExpr(ValFilter(field, operator, val))
}

func (q *DbQuery) NotExpr(filters ...Filter) Query {
	for _, f := range filters {
		q.FilterExpr(Not(f))
	}
	return q
}

func (q *DbQuery) Not(field string, val interface{}) Query {
	return q.FilterExpr(Not(Eq(field, val)))
}

func (q *DbQuery) NotCond(field string, condition string, val interface{}) Query {
	operator := MapOperator(condition)
	if operator == "" {
		panic(fmt.Sprintf("Unknown operator: '%v'", operator))
	}
	return q.NotExpr(ValFilter(field, operator, val))
}

/**
 * Joins.
 */

func (q *DbQuery) JoinQ(jqs ...RelationQuery) Query {
	for _, jq := range jqs {
		stmt := jq.GetStatement()
		stmt.Base = q.statement
		q.statement.AddJoin(stmt)
	}
	return q
}

func (q *DbQuery) Join(fieldName string) Query {
	q.statement.AddJoin(Join(fieldName, "", nil))
	return q
}

// Retrieve a join query for the specified field.
// Returns a *RelationQuery, or nil if not found.
// Supports nested Joins like 'Parent.Tags'.
func (q *DbQuery) GetJoin(field string) RelationQuery {
	stmt := q.statement.GetJoin(field)
	if stmt == nil {
		return nil
	}

	return &DbRelationQuery{
		baseQuery: q,
		statement: stmt,
	}
}

func (q *DbQuery) GetJoins() []RelationQuery {
	jqs := make([]RelationQuery, 0)
	for _, stmt := range q.statement.Joins {
		q := &DbRelationQuery{
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

func (q *DbQuery) Related(name string) RelationQuery {
	return RelQ(q, name)
}

func (q *DbQuery) RelatedCustom(name, collection, joinKey, foreignKey, typ string) RelationQuery {
	return RelQCustom(q, name, collection, joinKey, foreignKey, typ)
}

/**
 * Backend functions.
 */

func (q *DbQuery) GetBackend() Backend {
	return q.backend
}

func (q *DbQuery) SetBackend(x Backend) {
	q.backend = x
}

func (q *DbQuery) Find(targetSlice ...interface{}) ([]interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Calling .Find() on query without backend")
	}

	return q.backend.Query(q, targetSlice...)
}

func (q *DbQuery) First(targetModel ...interface{}) (interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Calling .First() on query without backend")
	}

	return q.backend.QueryOne(q, targetModel...)
}

func (q *DbQuery) Last(targetModel ...interface{}) (interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Calling .Last() on query without backend")
	}
	return q.backend.Last(q, targetModel...)
}

func (q *DbQuery) Count() (int, apperror.Error) {
	if q.backend == nil {
		panic("Calling .Count() on query without backend")
	}
	return q.backend.Count(q)
}

func (q *DbQuery) Delete() apperror.Error {
	if q.backend == nil {
		panic("Calling .Delete() on query without backend")
	}
	return q.backend.DeleteQ(q)
}

/**
 * RelationQuery.
 */

type DbRelationQuery struct {
	DbQuery

	baseQuery Query
	statement *JoinStatement
}

// Ensure DbRelationQuery implements RelationQuery.
var _ RelationQuery = (*DbRelationQuery)(nil)

func RelQ(q Query, name string) RelationQuery {
	stmt := Join(name, "", nil)
	stmt.Base = q.GetStatement()

	relQ := DbRelationQuery{
		baseQuery: q,
		statement: stmt,
	}
	relQ.SetBackend(q.GetBackend())

	return &relQ
}

func RelQCustom(q Query, name, collection, joinKey, foreignKey, typ string) RelationQuery {
	filter := &Filter{
		Field:    ColFieldIdentifier(collection, joinKey),
		Operator: OPERATOR_EQ,
		Clause:   ColFieldIdentifier(q.GetCollection(), foreignKey),
	}
	stmt := Join(name, typ, filter)
	stmt.Base = q.GetStatement()

	relQ := DbRelationQuery{
		baseQuery: q,
		statement: stmt,
	}
	relQ.SetBackend(q.GetBackend())

	return relQ
}

// RelationQuery specific methods.

func (q *DbRelationQuery) GetBaseQuery() Query {
	return q.baseQuery
}

func (q *DbRelationQuery) SetBaseQuery(bq Query) {
	q.baseQuery = bq
}

func (q *DbRelationQuery) GetRelationName() string {
	return q.statement.RelationName
}

func (q *DbRelationQuery) SetRelationName(name string) {
	q.statement.RelationName = name
}

func (q *DbRelationQuery) GetJoinType() string {
	return q.statement.JoinType
}

func (q *DbRelationQuery) SetJoinType(typ string) {
	return q.statement.JoinType = typ
}

func (q *DbRelationQuery) Build() (Query, apperror.Error) {
	if q.backend == nil {
		panic("Callind .Find() on a query without backend")
	}
	return q.backend.BuildRelationQuery(q)
}

// Backend methods.

func (q *DbRelationQuery) Find(targetSlice ...interface{}) ([]interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Callind .Find() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.Find(targetSlice...)
}

func (q *DbRelationQuery) First(targetModel ...interface{}) (interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Calling .First() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.First(targetModel...)
}

func (q *DbRelationQuery) Last(targetModel ...interface{}) (interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Calling .Last() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.Last(targetModel...)
}

func (q *DbRelationQuery) Count() (int, apperror.Error) {
	if q.backend == nil {
		panic("Calling .Count() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return 0, err
	}
	return newQ.Count()
}

func (q *DbRelationQuery) Delete() apperror.Error {
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

func (q *DbRelationQuery) Limit(l int) RelationQuery {
	q.DbQuery.Limit(l)
	return q
}

/**
 * Offset methods.
 */

func (q *DbRelationQuery) Offset(o int) RelationQuery {
	q.DbQuery.Offset(o)
	return q
}

/**
 * Fields methods.
 */

func (q *DbRelationQuery) Fields(fields ...string) RelationQuery {
	q.DbQuery.Fields(fields...)
	return q
}

func (q *DbRelationQuery) AddFields(fields ...string) RelationQuery {
	q.DbQuery.AddFields(fields...)
	return q
}

/**
 * Limit the query to specified fields.
 * If fields where already specified, they will be reduced.
 */
func (q *DbRelationQuery) LimitFields(fields ...string) RelationQuery {
	q.DbQuery.LimitFields(fields...)
	return q
}

/**
 * Order methods.
 */

func (q *DbRelationQuery) Order(name string, asc bool) RelationQuery {
	q.DbQuery.Order(name, asc)
	return q
}

func (q *DbRelationQuery) SetOrders(orders ...OrderSpec) RelationQuery {
	q.DbQuery.SetOrders(orders...)
	return q
}

/**
 * Filter methods.
 */

func (q *DbRelationQuery) FilterQ(f ...Filter) RelationQuery {
	q.DbQuery.FilterQ(f...)
	return q
}

func (q *DbRelationQuery) SetFilters(f ...Filter) RelationQuery {
	q.DbQuery.SetFilters(f...)
	return q
}

func (q *DbRelationQuery) Filter(field string, val interface{}) RelationQuery {
	q.DbQuery.FilterQ(Eq(field, val))
	return q
}

func (q *DbRelationQuery) FilterCond(field string, condition string, val interface{}) RelationQuery {
	q.DbQuery.FilterCond(field, condition, val)
	return q
}

func (q *DbRelationQuery) AndQ(filters ...Filter) RelationQuery {
	q.DbQuery.FilterQ(filters...)
	return q
}

func (q *DbRelationQuery) And(field string, val interface{}) RelationQuery {
	q.DbQuery.Filter(field, val)
	return q
}

func (q *DbRelationQuery) AndCond(field, condition string, val interface{}) RelationQuery {
	q.DbQuery.FilterCond(field, condition, val)
	return q
}

func (q *DbRelationQuery) OrQ(filters ...Filter) RelationQuery {
	q.DbQuery.OrQ(filters...)
	return q
}

func (q *DbRelationQuery) Or(field string, val interface{}) RelationQuery {
	q.DbQuery.OrQ(Eq(field, val))
	return q
}

func (q *DbRelationQuery) OrCond(field string, condition string, val interface{}) RelationQuery {
	q.DbQuery.OrCond(field, condition, val)
	return q
}

func (q *DbRelationQuery) NotQ(filters ...Filter) RelationQuery {
	q.DbQuery.NotQ(filters...)
	return q
}

func (q *DbRelationQuery) Not(field string, val interface{}) RelationQuery {
	q.DbQuery.Not(field, val)
	return q
}

func (q *DbRelationQuery) NotCond(field string, condition string, val interface{}) RelationQuery {
	q.DbQuery.NotCond(field, condition, val)
	return q
}

/**
 * Joins.
 */

func (q *DbRelationQuery) JoinQ(jq ...RelationQuery) RelationQuery {
	q.DbQuery.JoinQ(jq...)
	return q
}

func (q *DbRelationQuery) Join(fieldName string) RelationQuery {
	q.DbQuery.Join(fieldName)
	return q
}
