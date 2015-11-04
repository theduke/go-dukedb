package dukedb

import (
	"fmt"
	"strconv"
	"strings"

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

	// Name of the collection.
	collection string

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

	rawResult []map[string]interface{}
}

func NewQuery(collection string, backend Backend) *Query {
	return &Query{
		backend:    backend,
		collection: collection,
		statement:  NewSelectStmt(collection),
		joins:      make(map[string]*RelationQuery),
	}
}

func (q *Query) GetStatement() *SelectStmt {
	return q.statement
}

func (q *Query) SetStatement(stmt *SelectStmt) {
	q.statement = stmt
}

func (q *Query) GetCollection() string {
	return q.collection
}

func (q *Query) SetCollection(collection string) {
	q.collection = collection
	q.statement.SetCollection(collection)
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
		q.statement.AddField(NewIdExpr(field))
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
	q.statement.AddSort(NewSortExpr(NewIdExpr(field), asc))
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
	left, right := utils.StrSplitLeft(field, ".")
	if right != "" {
		// Nested join.
		// If the join exists, add the filter to the join.
		join := q.joins[left]
		if join != nil {
			join.FilterCond(right, condition, val)
			return q
		}
	}

	return q.FilterExpr(NewFieldValFilter(q.collection, field, condition, val))
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
	left, right := utils.StrSplitLeft(field, ".")
	if right != "" {
		// Nested join.
		// If the join exists, add the filter to the join.
		join := q.joins[left]
		if join != nil {
			join.OrCond(right, condition, val)
			return q
		}
	}

	return q.OrExpr(NewFieldValFilter(q.collection, field, condition, val))
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
	return q.NotCond(field, OPERATOR_EQ, val)
}

func (q *Query) NotCond(field string, condition string, val interface{}) *Query {
	left, right := utils.StrSplitLeft(field, ".")
	if right != "" {
		// Nested join.
		// If the join exists, add the filter to the join.
		join := q.joins[left]
		if join != nil {
			join.NotCond(right, condition, val)
			return q
		}
	}

	return q.NotExpr(NewFieldValFilter(q.collection, field, condition, val))
}

/**
 * Joins.
 */

func (q *Query) JoinQ(jqs ...*RelationQuery) *Query {
	for _, jq := range jqs {
		name := jq.GetRelationName()
		if name == "" {
			name = "custom_join_" + strconv.Itoa(len(q.joins))
		}
		q.joins[name] = jq
	}
	return q
}

func (q *Query) Join(relationName string, joinType ...string) *Query {
	typ := JOIN_LEFT
	if len(joinType) > 0 {
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
			join := parentJoin.GetJoin(right)
			if join != nil {
				return join
			}
		}
	}

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
	relQ := RelQ(q, name, "", JOIN_INNER)
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

func (q *Query) Pluck() ([]map[string]interface{}, apperror.Error) {
	if q.backend == nil {
		panic("Calling .Pluck() on query without backend")
	}
	return q.backend.Pluck(q)
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
	relationName string
	baseQuery    *Query
	statement    *JoinStmt

	localField   string
	foreignField string
}

func RelQ(q *Query, relationName string, collection string, joinType string) *RelationQuery {
	joinStmt := NewJoinStmt(collection, joinType, nil)
	newQ := NewQuery(collection, q.backend)
	newQ.SetStatement(joinStmt.SelectStatement())

	relQ := &RelationQuery{
		Query:        *newQ,
		relationName: relationName,
		baseQuery:    q,
		statement:    joinStmt,
	}

	return relQ
}

// Create a new relation query that connects two collections via two of their
// fields. These will normally be the respective primary keys.
func RelQCustom(q *Query, collection, joinKey, foreignKey, typ string) *RelationQuery {
	joinCondition := NewFilter(
		NewColFieldIdExpr(q.GetCollection(), joinKey),
		OPERATOR_EQ,
		NewColFieldIdExpr(collection, foreignKey))

	jq := RelQExpr(q, collection, typ, joinCondition)
	jq.localField = foreignKey
	jq.foreignField = joinKey

	return jq
}

// RelQExpr creates a new relation query for a collection with an arbitrary
// join condition.
func RelQExpr(q *Query, collection, typ string, joinCondition Expression) *RelationQuery {
	joinStmt := NewJoinStmt(collection, typ, joinCondition)
	newQ := NewQuery(collection, q.backend)
	newQ.SetStatement(joinStmt.SelectStatement())

	relQ := &RelationQuery{
		Query:     *newQ,
		baseQuery: q,
		statement: joinStmt,
	}

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
		panic("Calling .Build() on a query without backend")
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

func (q *Query) Normalize() apperror.Error {
	if q.backend == nil {
		panic("Called .Normalize() on a query without a backend.")
	}

	info := q.backend.ModelInfos().Find(q.collection)
	if info == nil {
		return &apperror.Err{
			Public:  true,
			Code:    "unknown_collection",
			Message: fmt.Sprintf("Collection %v was not registered with the backend", q.collection),
		}
	}

	// Normalize joins.

	nestedJoins := make([]*RelationQuery, 0)
	// First, process all non-nested joins.
	for _, join := range q.GetJoins() {
		relationName := join.GetRelationName()
		if relationName == "" {
			// Ignore custom joins.
			continue
		}

		if strings.Contains(relationName, ".") {
			// Process nested joins later.
			nestedJoins = append(nestedJoins, join)
			continue
		}

		relation := info.FindRelation(relationName)
		if relation == nil {
			return &apperror.Err{
				Public:  true,
				Code:    "unknown_relation",
				Message: fmt.Sprintf("Collection '%v' does not have a relation '%v'", info.Collection(), relationName),
			}
		}

		if relation.Name() != relationName {
			join.SetRelationName(relationName)
			delete(q.joins, relationName)
			q.joins[relation.Name()] = join
		}
	}

	// Now, move nested joins to their parent.
	for _, join := range nestedJoins {
		relationName := join.GetRelationName()
		left, right := utils.StrSplitLeft(relationName, ".")

		relation := info.FindRelation(left)
		if relation == nil {
			return &apperror.Err{
				Public:  true,
				Code:    "unknown_relation",
				Message: fmt.Sprintf("Collection '%v' does not have a relation '%v'", info.Collection(), relationName),
			}
		}

		parentJoin := q.joins[relation.Name()]
		if parentJoin == nil {
			// Parent join does not exist, so auto join it.
			q.Join(relation.Name())
			parentJoin = q.GetJoin(relation.Name())
		}

		// Parent join definitely exists now.
		// Add the nested join.
		join.SetRelationName(right)
		parentJoin.JoinQ(join)
	}

	// Normalize the statement now.

	s := q.GetStatement()

	// Normalize fields.
	fields := make([]Expression, 0)
	for _, field := range s.Fields() {
		id, ok := field.(*IdentifierExpr)
		if !ok {
			// Custom field, so just accept it.
			fields = append(fields, field)
			continue
		}

		fieldName := id.Identifier()

		left, right := utils.StrSplitLeft(fieldName, ".")
		if right == "" {
			// Not a nested field.
			attr := info.FindAttribute(fieldName)
			if attr == nil {
				return &apperror.Err{
					Public:  true,
					Code:    "unknown_field",
					Message: fmt.Sprintf("The collection %v does not have a field %v", info.Collection(), fieldName),
				}
			}

			// Field exists, so add it to the statement.
			sel := NewFieldSelector(attr.Name(), info.BackendName(), attr.BackendName(), attr.Type())
			fields = append(fields, sel)
			continue
		}

		relation := info.FindRelation(left)
		if relation != nil {
			// Nested field. Check if parent join exists.
			join := q.GetJoin(relation.Name())
			if join == nil {
				// Parent not joined.
				// Auto-join it.
				q.Join(relation.Name())
				join = q.GetJoin(relation.Name())
			}

			// Add field to join.
			join.Field(right)
			continue
		}

		// Check if an embedded attribute exists.
		attr := info.FindAttribute(left)
		if attr != nil && attr.BackendEmbed() {
			// Embedded attribute.
			// Maybe the backend supports selecting fields from it, so add
			// it to the statement.
			sel := NewFieldSelector(attr.Name()+"."+right, info.BackendName(), attr.BackendName()+"."+right, attr.Type())
			fields = append(fields, sel)
			continue
		}

		// Unknown field!
		return &apperror.Err{
			Public:  true,
			Code:    "unknown_field",
			Message: fmt.Sprintf("Collection %v does not have a field %v", info.Collection(), fieldName),
		}
	}
	s.SetFields(fields)

	// Normalize Filters.
	if err := q.normalizeFilter(info, s.Filter()); err != nil {
		return err
	}

	// Normalize sorts.
	sorts := make([]*SortExpr, 0)
	for _, sort := range s.Sorts() {
		expr := sort.Expression()
		id, ok := expr.(*IdentifierExpr)
		if !ok {
			// Custom sort, just add it.
			sorts = append(sorts, sort)
		}

		fieldName := id.Identifier()
		left, right := utils.StrSplitLeft(fieldName, ".")
		if right == "" {
			// Not a nested field.
			attr := info.FindAttribute(fieldName)
			if attr == nil {
				return &apperror.Err{
					Public:  true,
					Code:    "unknown_field",
					Message: fmt.Sprintf("The collection %v does not have a field %v", info.Collection(), fieldName),
				}
			}

			// Field exists, so add it to the statement.
			sort.SetExpression(NewColFieldIdExpr(info.BackendName(), attr.BackendName()))
			sorts = append(sorts, sort)
			continue
		}

		// Nested field.
		relation := info.FindRelation(left)
		if relation != nil {
			// Check if parent join exists.
			join := q.GetJoin(relation.Name())
			if join == nil {
				// Parent not joined.
				// Auto-join it.
				q.Join(relation.Name())
				join = q.GetJoin(relation.Name())
			}

			// Add field to join.
			join.Sort(right, sort.Ascending())
			continue
		}

		// Check if an embedded attribute exists.
		attr := info.FindAttribute(left)
		if attr != nil && attr.BackendEmbed() {
			// Embedded attribute.
			// Maybe the backend supports selecting fields from it, so add
			// it to the statement.
			sort.SetExpression(NewColFieldIdExpr(info.BackendName(), attr.BackendName()))
			sorts = append(sorts, sort)
			continue
		}

		// Unknown field!
		return &apperror.Err{
			Public:  true,
			Code:    "unknown_field",
			Message: fmt.Sprintf("Collection %v does not have a field %v", info.Collection(), fieldName),
		}
	}

	return nil
}

func (q *Query) normalizeFilter(info *ModelInfo, filter Expression) apperror.Error {
	switch f := filter.(type) {
	case MultiExpression:
		for _, e := range f.Expressions() {
			if err := q.normalizeFilter(info, e); err != nil {
				return err
			}
		}

	case NestedExpression:
		if err := q.normalizeFilter(info, f.Expression()); err != nil {
			return err
		}

	case *ColFieldIdentifierExpr:
		if f.Collection() != "" {
			i := q.backend.ModelInfos().Find(f.Collection())
			if i != nil {
				return apperror.New("unknown_collection",
					fmt.Sprintf("The collection %v was not registered with the backend.", f.Collection()),
					true)
			}

			f.SetCollection(i.BackendName())

			info = i
		}

		field := f.Field()
		left, right := utils.StrSplitLeft(field, ".")
		attr := info.FindAttribute(left)
		if attr == nil || (right != "" && !attr.BackendEmbed()) {
			return &apperror.Err{
				Public:  true,
				Code:    "unknown_field",
				Message: fmt.Sprintf("The collection %v does not have a field %v", info.Collection(), field),
			}
		} else if right == "" {
			// Non-nested field that exists.
			f.SetField(attr.BackendName())
		} else {
			// Embedded field, so maybe the backend can handle the filter.
			f.SetField(attr.BackendName() + "." + right)
		}
	}

	return nil
}
