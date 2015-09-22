package dukedb

import (
	"fmt"
	"strings"
)

type Filter interface {
	Type() string
	SetField(string) Filter
}

type MultiFilter interface {
	Filter
	Add(...Filter) MultiFilter
}

/**
 * And.
 */

type multiFilter struct {
	Filters []Filter
}

func (m multiFilter) Type() string {
	panic("type_method_not_overwritten")
}

func (m *multiFilter) Add(filters ...Filter) MultiFilter {
	m.Filters = append(m.Filters, filters...)
	return m
}

func (m *multiFilter) SetField(field string) Filter {
	for _, filter := range m.Filters {
		filter.SetField(field)
	}
	return m
}

type AndCondition struct {
	multiFilter
}

// Ensure AndCondition implements MultiFilter.
var _ MultiFilter = (*AndCondition)(nil)

func (a *AndCondition) Type() string {
	return "and"
}

func And(f ...Filter) *AndCondition {
	a := AndCondition{}
	a.Filters = f
	return &a
}

/**
 * Or.
 */

type OrCondition struct {
	multiFilter
}

// Ensure OrCondition implements MultiFilter.
var _ MultiFilter = (*OrCondition)(nil)

func (o *OrCondition) Type() string {
	return "or"
}

func Or(f ...Filter) *OrCondition {
	or := OrCondition{}
	or.Filters = f
	return &or
}

/**
 * NOT.
 */

type NotCondition struct {
	multiFilter
}

// Ensure NotCondition implements MultiFilter.
var _ MultiFilter = (*NotCondition)(nil)

func (n NotCondition) Type() string {
	return "not"
}

func Not(f ...Filter) *NotCondition {
	not := &NotCondition{}
	not.Filters = f
	return not
}

/**
 * Generic field condition.
 */

type FieldCondition struct {
	Typ   string
	Field string
	Value interface{}
}

func (f *FieldCondition) Type() string {
	return f.Typ
}

func (f *FieldCondition) SetField(field string) Filter {
	f.Field = field
	return f
}

/**
 * Eq.
 */

func Eq(field string, val interface{}) *FieldCondition {
	eq := FieldCondition{
		Field: field,
		Value: val,
		Typ:   "eq",
	}
	return &eq
}

/**
 * Neq.
 */

func Neq(field string, val interface{}) *FieldCondition {
	neq := FieldCondition{
		Field: field,
		Value: val,
		Typ:   "neq",
	}
	return &neq
}

/**
 * Like.
 */

func Like(field string, val interface{}) *FieldCondition {
	like := FieldCondition{
		Field: field,
		Value: val,
		Typ:   "like",
	}
	return &like
}

/**
 * In.
 */

func In(field string, val interface{}) *FieldCondition {
	in := FieldCondition{
		Field: field,
		Value: val,
		Typ:   "in",
	}
	return &in
}

/**
 * Less than Lt.
 */

func Lt(field string, val interface{}) *FieldCondition {
	lt := FieldCondition{
		Field: field,
		Value: val,
		Typ:   "lt",
	}
	return &lt
}

/**
 * Less than eqal Lte.
 */

func Lte(field string, val interface{}) *FieldCondition {
	lte := FieldCondition{
		Field: field,
		Value: val,
		Typ:   "lte",
	}
	return &lte
}

/**
 * Greater than gt.
 */

func Gt(field string, val interface{}) *FieldCondition {
	gt := FieldCondition{
		Field: field,
		Value: val,
		Typ:   "gt",
	}
	return &gt
}

/**
 * Greater than equal gte.
 */

func Gte(field string, val interface{}) *FieldCondition {
	gte := FieldCondition{
		Field: field,
		Value: val,
		Typ:   "gte",
	}
	return &gte
}

func conditionToFilterType(cond string) string {
	typ := ""

	switch strings.ToLower(cond) {
	case "==":
		typ = "eq"
	case "=":
		typ = "eq"
	case "!=":
		typ = "neq"
	case "<":
		typ = "lt"
	case "<=":
		typ = "lte"
	case ">":
		typ = "gt"
	case ">=":
		typ = "gte"
	case "like":
		typ = "like"
	case "in":
		typ = "in"
	default:
		panic(fmt.Sprintf("Unknown field contidion: '%v'", cond))
	}

	return typ
}

/**
 * Query.
 */

type OrderSpec struct {
	Field     string
	Ascending bool
}

func Order(field string, asc bool) OrderSpec {
	return OrderSpec{Field: field, Ascending: asc}
}

func (o OrderSpec) String() string {
	s := o.Field + " "
	if o.Ascending {
		s += "asc"
	} else {
		s += "desc"
	}
	return s
}

type DbQuery struct {
	backend Backend

	collection string

	joins []RelationQuery

	limit   int
	offset  int
	orders  []OrderSpec
	fields  []string
	filters []Filter
}

// Ensure DbQuery implements Query.
var _ Query = (*DbQuery)(nil)

func Q(collection string) Query {
	return &DbQuery{
		collection: collection,
	}
}

func (q *DbQuery) GetCollection() string {
	return q.collection
}

/**
 * Limit methods.
 */

func (q *DbQuery) Limit(l int) Query {
	q.limit = l
	return q
}

func (q *DbQuery) GetLimit() int {
	return q.limit
}

/**
 * Offset methods.
 */

func (q *DbQuery) Offset(o int) Query {
	q.offset = o
	return q
}

func (q *DbQuery) GetOffset() int {
	return q.offset
}

/**
 * Fields methods.
 */

func (q *DbQuery) Fields(fields ...string) Query {
	q.fields = fields
	return q
}

func (q *DbQuery) AddFields(fields ...string) Query {
	q.fields = append(q.fields, fields...)
	return q
}

/**
 * Limit the query to specified fields.
 * If fields where already specified, they will be reduced.
 */
func (q *DbQuery) LimitFields(fields ...string) Query {
	if q.fields == nil {
		return q.Fields(fields...)
	}

	allowMap := make(map[string]bool)
	for _, field := range fields {
		allowMap[field] = true
	}

	finalFields := make([]string, 0)

	for _, field := range q.fields {
		if _, ok := allowMap[field]; ok {
			finalFields = append(finalFields, field)
		}
	}

	q.fields = finalFields

	return q
}

func (q *DbQuery) GetFields() []string {
	return q.fields
}

/**
 * Order methods.
 */

func (q *DbQuery) Order(name string, asc bool) Query {
	q.orders = append(q.orders, OrderSpec{Field: name, Ascending: asc})
	return q
}

func (q *DbQuery) SetOrders(orders ...OrderSpec) Query {
	q.orders = orders
	return q
}

func (q *DbQuery) GetOrders() []OrderSpec {
	return q.orders
}

/**
 * Filter methods.
 */

func (q *DbQuery) FilterQ(f ...Filter) Query {
	q.filters = append(q.filters, f...)
	return q
}

func (q *DbQuery) SetFilters(f ...Filter) Query {
	q.filters = f
	return q
}

func (q *DbQuery) Filter(field string, val interface{}) Query {
	return q.FilterQ(Eq(field, val))
}

func (q *DbQuery) FilterCond(field string, condition string, val interface{}) Query {
	typ := conditionToFilterType(condition)

	f := FieldCondition{
		Typ:   typ,
		Field: field,
		Value: val,
	}

	return q.FilterQ(&f)
}

func (q *DbQuery) AndQ(filters ...Filter) Query {
	return q.FilterQ(filters...)
}

func (q *DbQuery) And(field string, val interface{}) Query {
	return q.Filter(field, val)
}

func (q *DbQuery) AndCond(field, condition string, val interface{}) Query {
	return q.FilterCond(field, condition, val)
}

func (q *DbQuery) OrQ(filters ...Filter) Query {
	for _, filter := range filters {
		filterLen := 0
		if q.filters != nil {
			filterLen = len(q.filters)
		}

		if filterLen == 0 {
			// No filters set, so just filter regularily.
			return q.FilterQ(filter)
		} else if filterLen > 1 {
			// More than one filter.
			// Can not do OR with multiple clauses present.
			panic("invalid_or_with_multiple_clauses")
		}

		// One filter is already present.
		// If it is OR, append to the or.
		// Otherwise create a new top level Or.
		if q.filters[0].Type() == "or" {
			or := q.filters[0].(*OrCondition)
			or.Filters = append(or.Filters, filter)
		} else {
			// Other filter is not an OR, so just OR the two together.
			q.filters = []Filter{Or(q.filters[0], filter)}
		}
	}

	return q
}

func (q *DbQuery) Or(field string, val interface{}) Query {
	return q.OrQ(Eq(field, val))
}

func (q *DbQuery) OrCond(field string, condition string, val interface{}) Query {
	typ := conditionToFilterType(condition)

	f := FieldCondition{
		Typ:   typ,
		Field: field,
		Value: val,
	}

	return q.OrQ(&f)
}

func (q *DbQuery) NotQ(filters ...Filter) Query {
	q.filters = append(q.filters, Not(filters...))
	return q
}

func (q *DbQuery) Not(field string, val interface{}) Query {
	q.filters = append(q.filters, Neq(field, val))
	return q
}

func (q *DbQuery) NotCond(field string, condition string, val interface{}) Query {
	typ := conditionToFilterType(condition)

	f := FieldCondition{
		Typ:   typ,
		Field: field,
		Value: val,
	}

	return q.NotQ(&f)
}

func (q *DbQuery) GetFilters() []Filter {
	return q.filters
}

/**
 * Joins.
 */

func (q *DbQuery) JoinQ(jq RelationQuery) Query {
	jq.SetBaseQuery(q)
	q.joins = append(q.joins, jq)
	return q
}

func (q *DbQuery) Join(fieldName string) Query {
	q.joins = append(q.joins, RelQ(q, fieldName))
	return q
}

// Retrieve a join query for the specified field.
// Returns a *RelationQuery, or nil if not found.
// Supports nested Joins like Parent.Tags
func (q *DbQuery) GetJoin(field string) RelationQuery {
	if q.joins == nil {
		return nil
	}

	parts := strings.Split(field, ".")
	if len(parts) > 1 {
		field = parts[0]
	}

	for _, join := range q.joins {
		if join.GetRelationName() == field {
			if len(parts) > 1 {
				// Nested join, call GetJoin again on found join query.
				return join.GetJoin(strings.Join(parts[1:], "."))
			} else {
				// Not nested, just return the join.
				return join
			}
		}
	}

	// Join not found, return nil.
	return nil
}

func (q *DbQuery) GetJoins() []RelationQuery {
	return q.joins
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

func (q *DbQuery) Find(targetSlice ...interface{}) ([]interface{}, DbError) {
	if q.backend == nil {
		panic("Calling .Find() on query without backend")
	}

	return q.backend.Query(q, targetSlice...)
}

func (q *DbQuery) First(targetModel ...interface{}) (Model, DbError) {
	if q.backend == nil {
		panic("Calling .First() on query without backend")
	}

	return q.backend.QueryOne(q, targetModel...)
}

func (q *DbQuery) Last(targetModel ...interface{}) (Model, DbError) {
	if q.backend == nil {
		panic("Calling .Last() on query without backend")
	}
	return q.backend.Last(q, targetModel...)
}

func (q *DbQuery) Count() (int, DbError) {
	if q.backend == nil {
		panic("Calling .Count() on query without backend")
	}
	return q.backend.Count(q)
}

func (q *DbQuery) Delete() DbError {
	if q.backend == nil {
		panic("Calling .Delete() on query without backend")
	}
	return q.backend.DeleteMany(q)
}

/**
 * RelationQuery.
 */

const (
	InnerJoin = "inner"
	LeftJoin  = "left"
	RightJoin = "right"
	CrossJoin = "cross"
)

type DbRelationQuery struct {
	DbQuery

	baseQuery    Query
	relationName string

	joinType string

	joinFieldName    string
	foreignFieldName string
}

// Ensure DbRelationQuery implements RelationQuery.
var _ RelationQuery = (*DbRelationQuery)(nil)

func RelQ(q Query, name string) RelationQuery {
	relQ := DbRelationQuery{
		baseQuery:    q,
		relationName: name,
	}
	relQ.SetBackend(q.GetBackend())

	return &relQ
}

func RelQCustom(q Query, name, collection, joinKey, foreignKey, typ string) RelationQuery {
	relQ := &DbRelationQuery{
		baseQuery:        q,
		joinFieldName:    joinKey,
		foreignFieldName: foreignKey,
		joinType:         typ,
	}
	relQ.relationName = name
	relQ.collection = collection
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
	return q.relationName
}

func (q *DbRelationQuery) GetJoinType() string {
	return q.joinType
}

func (q *DbRelationQuery) GetJoinFieldName() string {
	return q.joinFieldName
}

func (q *DbRelationQuery) SetJoinFieldName(x string) {
	q.joinFieldName = x
}

func (q *DbRelationQuery) GetForeignFieldName() string {
	return q.foreignFieldName
}

func (q *DbRelationQuery) SetForeignFieldName(x string) {
	q.foreignFieldName = x
}

func (q *DbRelationQuery) Build() (Query, DbError) {
	if q.backend == nil {
		panic("Callind .Find() on a query without backend")
	}
	return q.backend.BuildRelationQuery(q)
}

// Backend methods.

func (q *DbRelationQuery) Find(targetSlice ...interface{}) ([]interface{}, DbError) {
	if q.backend == nil {
		panic("Callind .Find() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.Find(targetSlice...)
}

func (q *DbRelationQuery) First(targetModel ...interface{}) (Model, DbError) {
	if q.backend == nil {
		panic("Calling .First() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.First(targetModel...)
}

func (q *DbRelationQuery) Last(targetModel ...interface{}) (Model, DbError) {
	if q.backend == nil {
		panic("Calling .Last() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.Last(targetModel...)
}

func (q *DbRelationQuery) Count() (int, DbError) {
	if q.backend == nil {
		panic("Calling .Count() on a query without backend")
	}

	newQ, err := q.backend.BuildRelationQuery(q)
	if err != nil {
		return 0, err
	}
	return newQ.Count()
}

func (q *DbRelationQuery) Delete() DbError {
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

func (q *DbRelationQuery) JoinQ(jq RelationQuery) RelationQuery {
	q.DbQuery.JoinQ(jq)
	return q
}

func (q *DbRelationQuery) Join(fieldName string) RelationQuery {
	q.DbQuery.Join(fieldName)
	return q
}
