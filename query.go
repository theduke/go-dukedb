package dukedb

import (
	"fmt"
	"reflect"
	"strings"

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

// Ensure Query implements Query.
var _ Query = (*Query)(nil)

func Q(collection string) Query {
	return &Query{
		statement: &SelectStatement{
			Collection: collection,
		},
	}
}

func (q *Query) GetCollection() string {
	return q.statement.Collection
}

func (q *Query) GetName() string {
	return q.name
}

func (q *Query) SetName(x string) {
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

func (q *Query) Limit(l int) Query {
	q.statement.Limit = l
	return q
}

func (q *Query) GetLimit() int {
	return q.statement.Limit
}

/**
 * Offset methods.
 */

func (q *Query) Offset(o int) Query {
	q.statement.Offset = o
	return q
}

func (q *Query) GetOffset() int {
	return q.statement.Offset
}

/**
 * Fields methods.
 */

func (q *Query) Field(fields ...string) Query {
	q.statement.Fields = make([]Expression, 0)
	for _, field := range fields {
		q.statement.AddField(Identifier(field))
	}
	return q
}

func (q *Query) FieldExpr(exprs ...Expression) Query {
	q.statement.AddField(exprs...)
	return q
}

func (q *Query) SetFields(fields []string) Query {
	q.statement.Fields = nil
	return q.Field(fields...)
}

func (q *Query) SetFieldExpressions(expressions []Expression) Query {
	q.statement.Fields = expressions
	return q
}

/**
 * Limit the query to specified fields.
 * If fields where already specified, they will be reduced.
 */
func (q *Query) LimitFields(fields ...string) Query {
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

func (q *Query) Sort(name string, asc bool) Query {
	q.statement.AddSort(Sort(name, asc))
	return q
}

func (q *Query) SortExpr(expr *SortExpression) Query {
	q.statement.AddSort(expr)
	return q
}

func (q *Query) SetSorts(exprs []SortExpression) Query {
	q.statement.Sorts = exprs
	return q
}

/**
 * Filter methods.
 */

func (q *Query) FilterExpr(expressions ...Expression) Query {
	for _, expr := range expressions {
		q.statement.FilterAnd(expr)
	}
	return q
}

func (q *Query) SetFilters(expressions ...Expression) Query {
	q.statement.Filter = And(expressions...)
	return q
}

func (q *Query) Filter(field string, val interface{}) Query {
	return q.FilterQ(Eq(field, val))
}

func (q *Query) FilterCond(field string, condition string, val interface{}) Query {
	operator := MapOperator(condition)
	if operator == "" {
		panic(fmt.Sprintf("Unknown operator: '%v'", operator))
	}
	return q.FilterExpr(ValFilter(field, operator, val))
}

func (q *Query) AndExpr(filters ...Expession) Query {
	return q.FilterExpr(filters...)
}

func (q *Query) And(field string, val interface{}) Query {
	return q.Filter(field, val)
}

func (q *Query) AndCond(field, condition string, val interface{}) Query {
	return q.FilterCond(field, condition, val)
}

func (q *Query) OrExpr(filters ...Expression) Query {
	for _, f := range filters {
		q.statement.FilterOr(f)
	}
	return q
}

func (q *Query) Or(field string, val interface{}) Query {
	return q.OrExpr(Eq(field, val))
}

func (q *Query) OrCond(field string, condition string, val interface{}) Query {
	operator := MapOperator(condition)
	if operator == "" {
		panic(fmt.Sprintf("Unknown operator: '%v'", operator))
	}
	return q.OrExpr(ValFilter(field, operator, val))
}

func (q *Query) NotExpr(filters ...Filter) Query {
	for _, f := range filters {
		q.FilterExpr(Not(f))
	}
	return q
}

func (q *Query) Not(field string, val interface{}) Query {
	return q.FilterExpr(Not(Eq(field, val)))
}

func (q *Query) NotCond(field string, condition string, val interface{}) Query {
	operator := MapOperator(condition)
	if operator == "" {
		panic(fmt.Sprintf("Unknown operator: '%v'", operator))
	}
	return q.NotExpr(ValFilter(field, operator, val))
}

/**
 * Joins.
 */

func (q *Query) JoinQ(jqs ...RelationQuery) Query {
	for _, jq := range jqs {
		stmt := jq.GetStatement()
		stmt.Base = q.statement
		q.statement.AddJoin(stmt)
	}
	return q
}

func (q *Query) Join(fieldName string) Query {
	q.statement.AddJoin(Join(fieldName, "", nil))
	return q
}

// Retrieve a join query for the specified field.
// Returns a *RelationQuery, or nil if not found.
// Supports nested Joins like 'Parent.Tags'.
func (q *Query) GetJoin(field string) RelationQuery {
	stmt := q.statement.GetJoin(field)
	if stmt == nil {
		return nil
	}

	return &RelationQuery{
		baseQuery: q,
		statement: stmt,
	}
}

func (q *Query) GetJoins() []RelationQuery {
	jqs := make([]RelationQuery, 0)
	for _, stmt := range q.statement.Joins {
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

func (q *Query) Related(name string) RelationQuery {
	return RelQ(q, name)
}

func (q *Query) RelatedCustom(name, collection, joinKey, foreignKey, typ string) RelationQuery {
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

	baseQuery Query
	statement *JoinStatement
}

// Ensure RelationQuery implements RelationQuery.
var _ RelationQuery = (*RelationQuery)(nil)

func RelQ(q Query, name string) RelationQuery {
	stmt := Join(name, "", nil)
	stmt.Base = q.GetStatement()

	relQ := RelationQuery{
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

	relQ := RelationQuery{
		baseQuery: q,
		statement: stmt,
	}
	relQ.SetBackend(q.GetBackend())

	return relQ
}

// RelationQuery specific methods.

func (q *RelationQuery) GetBaseQuery() Query {
	return q.baseQuery
}

func (q *RelationQuery) SetBaseQuery(bq Query) {
	q.baseQuery = bq
}

func (q *RelationQuery) GetRelationName() string {
	return q.statement.RelationName
}

func (q *RelationQuery) SetRelationName(name string) {
	q.statement.RelationName = name
}

func (q *RelationQuery) GetJoinType() string {
	return q.statement.JoinType
}

func (q *RelationQuery) SetJoinType(typ string) {
	return q.statement.JoinType = typ
}

func (q *RelationQuery) Build() (Query, apperror.Error) {
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

func (q *RelationQuery) Limit(l int) RelationQuery {
	q.Query.Limit(l)
	return q
}

/**
 * Offset methods.
 */

func (q *RelationQuery) Offset(o int) RelationQuery {
	q.Query.Offset(o)
	return q
}

/**
 * Fields methods.
 */

func (q *RelationQuery) Fields(fields ...string) RelationQuery {
	q.Query.Fields(fields...)
	return q
}

func (q *RelationQuery) AddFields(fields ...string) RelationQuery {
	q.Query.AddFields(fields...)
	return q
}

/**
 * Limit the query to specified fields.
 * If fields where already specified, they will be reduced.
 */
func (q *RelationQuery) LimitFields(fields ...string) RelationQuery {
	q.Query.LimitFields(fields...)
	return q
}

/**
 * Order methods.
 */

func (q *RelationQuery) Order(name string, asc bool) RelationQuery {
	q.Query.Order(name, asc)
	return q
}

func (q *RelationQuery) SetOrders(orders ...OrderSpec) RelationQuery {
	q.Query.SetOrders(orders...)
	return q
}

/**
 * Filter methods.
 */

func (q *RelationQuery) FilterQ(f ...Filter) RelationQuery {
	q.Query.FilterQ(f...)
	return q
}

func (q *RelationQuery) SetFilters(f ...Filter) RelationQuery {
	q.Query.SetFilters(f...)
	return q
}

func (q *RelationQuery) Filter(field string, val interface{}) RelationQuery {
	q.Query.FilterQ(Eq(field, val))
	return q
}

func (q *RelationQuery) FilterCond(field string, condition string, val interface{}) RelationQuery {
	q.Query.FilterCond(field, condition, val)
	return q
}

func (q *RelationQuery) AndQ(filters ...Filter) RelationQuery {
	q.Query.FilterQ(filters...)
	return q
}

func (q *RelationQuery) And(field string, val interface{}) RelationQuery {
	q.Query.Filter(field, val)
	return q
}

func (q *RelationQuery) AndCond(field, condition string, val interface{}) RelationQuery {
	q.Query.FilterCond(field, condition, val)
	return q
}

func (q *RelationQuery) OrQ(filters ...Filter) RelationQuery {
	q.Query.OrQ(filters...)
	return q
}

func (q *RelationQuery) Or(field string, val interface{}) RelationQuery {
	q.Query.OrQ(Eq(field, val))
	return q
}

func (q *RelationQuery) OrCond(field string, condition string, val interface{}) RelationQuery {
	q.Query.OrCond(field, condition, val)
	return q
}

func (q *RelationQuery) NotQ(filters ...Filter) RelationQuery {
	q.Query.NotQ(filters...)
	return q
}

func (q *RelationQuery) Not(field string, val interface{}) RelationQuery {
	q.Query.Not(field, val)
	return q
}

func (q *RelationQuery) NotCond(field string, condition string, val interface{}) RelationQuery {
	q.Query.NotCond(field, condition, val)
	return q
}

/**
 * Joins.
 */

func (q *RelationQuery) JoinQ(jq ...RelationQuery) RelationQuery {
	q.Query.JoinQ(jq...)
	return q
}

func (q *RelationQuery) Join(fieldName string) RelationQuery {
	q.Query.Join(fieldName)
	return q
}
