package dukedb

import (
	"fmt"
	"reflect"

	"github.com/theduke/go-apperror"
	"github.com/theduke/go-utils"

	. "github.com/theduke/go-dukedb/expressions"
)

/**
 * Query.
 */

type Query struct {
	name string

	// backend holds the queries backend.
	backend Backend

	collection string
	modelInfo  *ModelInfo

	// Models attached to this query.
	models []interface{}

	// statement is the SelectStatement.
	statement *SelectStmt

	joins map[string]*RelationQuery

	// joinResultAssigner can hold a function that will take care of assigning the results
	// of a join query to the parent models. This is needed for m2m joins, since models
	// obtained by executing the query will not hold the neccessary fields for mapping
	// the query result to the parent objects.
	// For example, the SQL backend will use a closure to keep track of the raw query
	// result and assign based on it.
	joinResultAssigner JoinAssigner

	// errors holds errors that might have occurred while creating the query.
	// Must be checked by backends before executing the query.
	errors []apperror.Error
}

func newQuery(backend Backend, collection string) *Query {
	info := backend.ModelInfo(collection)
	return &Query{
		backend:    backend,
		modelInfo:  backend.ModelInfo(collection),
		collection: collection,
		statement:  NewSelectStmt(info.BackendName()),
		joins:      make(map[string]*RelationQuery),
	}
}

func (q *Query) GetStatement() *SelectStmt {
	return q.statement
}

func (q *Query) GetCollection() string {
	return q.statement.Collection()
}

func (q *Query) GetModels() []interface{} {
	return q.models
}

func (q *Query) SetModels(x []interface{}) {
	q.models = x
}

func (q *Query) GetName() string {
	return q.name
}

func (q *Query) SetName(name string) {
	q.name = name
}

func (q *Query) Name(x string) *Query {
	q.name = x
	return q
}

func (q *Query) GetJoinResultAssigner() JoinAssigner {
	return q.joinResultAssigner
}

func (q *Query) SetJoinResultAssigner(x JoinAssigner) {
	q.joinResultAssigner = x
}

func (q *Query) addError(code, msg string) {
	q.errors = append(q.errors, apperror.New(code, msg))
}

func (q *Query) GetErrors() []apperror.Error {
	return q.errors
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
	for _, field := range fields {
		// If field contains a . separator, check if a corresponding parent join exists.
		left, right := utils.StrSplitLeft(field, ".")
		if right != "" {
			// Possibly a nested field.
			parentJoin := q.GetJoin(left)
			if parentJoin == nil {
				// Parent join does not exist yet.
				// Auto-join if the relationship exists.
				relation := q.modelInfo.FindRelation(left)
				if relation != nil {
					q.Join(relation.Name())
					parentJoin = q.GetJoin(relation.Name())
				}
			}
			if parentJoin != nil {
				parentJoin.Field(right)
				return q
			}
		}

		// No parent join found, add the field.

		// Check if the field exists.
		attr := q.modelInfo.FindAttribute(field)
		var typ reflect.Type
		if attr != nil {
			// Field exists, so use it's backend name.
			field = attr.BackendName()
			typ = attr.Type()
		}

		// Still add the field, even if it is not found on the model info.
		// Maybe the backend still supports it!

		// Use a named expression to allow join queries without extra work.
		// Named queries will construct a SQL query with '"collection"."field" AS "collection.field"'
		fieldName := q.modelInfo.BackendName() + "." + field
		expr := NewFieldSelector(fieldName, q.modelInfo.BackendName(), field, typ)
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
	parent := StrBeforeLast(field, ".")
	if parent != field {
		// Possibly nested field, check if parent join exists.
		join := q.GetJoin(parent)
		if join != nil {
			attr := join.modelInfo.FindAttribute(StrAfterLast(field, "."))
			if attr != nil {
				// Joined collection contains a field with the given name, so properly add the sort
				// with the right backend names.
				field = attr.BackendName()
				// Even if the attr is not found, we do not return, but
			}
			q.statement.AddSort(NewSortExpr(NewColFieldIdExpr(join.modelInfo.BackendName(), field), asc))
			return q
		}
	}

	// No parent join found, so assume field is on the current collection.

	// Try to find attribute.
	attr := q.modelInfo.FindAttribute(field)
	if attr != nil {
		// Field found, so use its backend name.
		field = attr.BackendName()
	}
	// Even if the field was not found, still add it to the query, because
	// maybe the backend still supports it.
	q.statement.AddSort(NewSortExpr(NewColFieldIdExpr(q.GetCollection(), field), asc))
	return q
}

func (q *Query) SortExpr(expr *SortExpr) *Query {
	q.statement.AddSort(expr)
	return q
}

func (q *Query) SetSorts(exprs []*SortExpr) *Query {
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
		q.statement.SetFilter(NewAndExpr(expressions...))
	}
	return q
}

func (q *Query) FilterCond(field string, condition string, val interface{}) *Query {
	operator := MapOperator(condition)
	if operator == "" {
		q.addError("unknown_operator", fmt.Sprintf("Unknown operator %v", condition))
		return q
	}

	if attr := q.modelInfo.FindAttribute(field); attr != nil {
		field = attr.BackendName()
	}

	return q.FilterExpr(NewFieldValFilter(q.modelInfo.BackendName(), field, operator, val))
}

func (q *Query) Filter(field string, val interface{}) *Query {
	return q.FilterCond(field, OPERATOR_EQ, val)
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

func (q *Query) OrCond(field string, condition string, val interface{}) *Query {
	operator := MapOperator(condition)
	if operator == "" {
		panic(fmt.Sprintf("Unknown operator: '%v'", operator))
	}
	return q.OrExpr(NewFieldValFilter(q.GetCollection(), field, operator, val))
}

func (q *Query) Or(field string, val interface{}) *Query {
	return q.OrCond(field, OPERATOR_EQ, val)
}

func (q *Query) NotExpr(filters ...Expression) *Query {
	for _, f := range filters {
		q.FilterExpr(NewNotExpr(f))
	}
	return q
}

func (q *Query) Not(field string, val interface{}) *Query {
	return q.FilterExpr(NewNotExpr(Eq(q.GetCollection(), field, val)))
}

func (q *Query) NotCond(field string, condition string, val interface{}) *Query {
	operator := MapOperator(condition)
	if operator == "" {
		panic(fmt.Sprintf("Unknown operator: '%v'", operator))
	}
	return q.NotExpr(NewFieldValFilter(q.GetCollection(), field, operator, val))
}

/**
 * Joins.
 */

func (q *Query) JoinQ(jqs ...*RelationQuery) *Query {
	for _, jq := range jqs {
		relationName := jq.GetRelationName()

		left, right := utils.StrSplitLeft(relationName, ".")
		if right != "" {
			parentJoin := q.GetJoin(left)
			if parentJoin == nil {
				// Parent join does not exist yet, so join it.
				q.Join(left, jq.GetJoinType())
				parentJoin = q.GetJoin(left)
			}
			// parentJoin surely exists now, so join sub-join.
			jq.SetRelationName(right)
			parentJoin.JoinQ(jq)
			return q
		}

		// Not a nested join.

		// Check if join already exists.
		join := q.GetJoin(relationName)
		if join != nil {
			// Join already exists.
			return q
		}

		// Join does not exist yet.

		// Check if relation exists.
		relation := q.modelInfo.FindRelation(relationName)
		if relation == nil {
			// Relation does not exist, so add an error.
			q.addError("invalid_join_unknown_relation", fmt.Sprintf("The relation %v does not exist", relationName))
			// Do not return but add join even if the relation does not exist
			// to prevent errors.
		} else {
			jq.SetRelationName(relation.Name())
			jq.GetStatement().SetCollection(relation.Model().BackendName())
		}

		jq.SetBaseQuery(q)
		q.statement.AddJoin(jq.GetStatement())
		q.joins[jq.GetRelationName()] = jq
	}
	return q
}

func (q *Query) Join(relationName string, joinType ...string) *Query {
	typ := JOIN_LEFT
	if len(joinType) > 0 {
		if len(joinType) > 1 {
			panic("Called Query.Join() with more than one joinType")
		}
		typ = joinType[0]
	}

	join := RelQ(q, relationName, "", typ)
	q.JoinQ(join)

	return q
}

// Retrieve a join query for the specified field.
// Returns a *RelationQuery, or nil if not found.
// Supports nested Joins like 'Parent.Tags'.
func (q *Query) GetJoin(relationName string) *RelationQuery {
	// Check for nesting.
	left, right := utils.StrSplitLeft(relationName, ".")
	if right != "" {
		parentJoin, ok := q.joins[left]
		if ok {
			return parentJoin.GetJoin(right)
		}
	}

	// Not a nested join.
	return q.joins[relationName]
}

func (q *Query) HasJoin(relationName string) bool {
	join := q.GetJoin(relationName)
	return join != nil
}

func (q *Query) GetJoins() map[string]*RelationQuery {
	return q.joins
}

/**
 * Related.
 */

func (q *Query) Related(name string) *RelationQuery {
	relation := q.modelInfo.FindRelation(name)
	if relation == nil {
		return nil
	}

	relQ := RelQ(q, relation.Name(), relation.Model().BackendName(), JOIN_INNER)
	return relQ
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
	return q.backend.DeleteMany(q)
}

/**
 * RelationQuery.
 */

type RelationQuery struct {
	Query

	baseQuery    *Query
	relationName string
	statement    *JoinStmt
}

func RelQ(q *Query, name, backendName string, joinType string) *RelationQuery {
	stmt := NewJoinStmt(backendName, joinType, nil)
	stmt.SetName(name)

	relQ := &RelationQuery{
		Query: Query{
			joins: make(map[string]*RelationQuery),
		},
		baseQuery:    q,
		relationName: name,
		statement:    stmt,
	}
	// Set relQ.Query.statement to the join statements select, since all the
	// query methods operate on reQ.Query.statement.
	relQ.Query.statement = stmt.SelectStatement()
	relQ.SetBackend(q.GetBackend())

	return relQ
}

// Create a new relation query that connects two collections via two of their
// fields. These will normally be the respective primary keys.
func RelQCustom(q *Query, collection, joinKey, foreignKey, typ string) *RelationQuery {
	joinCondition := NewFilter(
		NewColFieldIdExpr(q.GetCollection(), joinKey),
		OPERATOR_EQ,
		NewColFieldIdExpr(collection, foreignKey))

	return RelQExpr(q, collection, typ, joinCondition)
}

// RelQExpr creates a new relation query for a collection with an arbitrary
// join condition.
func RelQExpr(q *Query, collection, typ string, joinCondition Expression) *RelationQuery {
	stmt := NewJoinStmt(collection, typ, joinCondition)

	relQ := &RelationQuery{
		baseQuery: q,
		statement: stmt,
	}

	// Set relQ.Query.statement to the join statements select, since all the
	// query methods operate on reQ.Query.statement.
	relQ.Query.statement = stmt.SelectStatement()
	relQ.SetBackend(q.GetBackend())

	return relQ
}

// RelationQuery specific methods.

func (q *RelationQuery) GetStatement() *JoinStmt {
	return q.statement
}

func (q *RelationQuery) GetBaseQuery() *Query {
	return q.baseQuery
}

func (q *RelationQuery) SetBaseQuery(bq *Query) {
	q.baseQuery = bq
}

func (q *RelationQuery) GetRelationName() string {
	return q.relationName
}

func (q *RelationQuery) SetRelationName(name string) {
	q.relationName = name
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

func (q *RelationQuery) SortExpr(expr *SortExpr) *RelationQuery {
	q.Query.SortExpr(expr)
	return q
}

func (q *RelationQuery) SetSorts(exprs []*SortExpr) *RelationQuery {
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
