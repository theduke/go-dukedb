package dukedb

import (
	"fmt"
	"strings"
	"errors"
)

type Filter interface {
	Type() string
}

/**
 * And.
 */

type AndCondition struct {
	Filters []Filter
}

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
 	Filters []Filter
}

func (o *OrCondition) Type() string {
	return "or"
}

func Or(f ...Filter) *OrCondition {
	or := OrCondition{
		Filters: f,
	}
	return &or
}

/**
 * NOT.
 */

type NotCondition struct {
	Filter Filter
}

func (n NotCondition) Type() string {
	return "not"
}

func Not(f Filter) *NotCondition {
	return &NotCondition{
		Filter: f,
	}
}

/**
 * Generic field condition.
 */

type FieldCondition struct {
	Typ string
	Field string
	Value interface{}
}

func (f *FieldCondition) Type() string {
	return f.Typ
}


/**
 * Eq.
 */

func Eq(field string, val interface{}) *FieldCondition {
	eq := FieldCondition{
		Field: field,
		Value: val,
		Typ:"eq",
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
		Typ:"neq",
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
		Typ:"like",
	}
	return &like
}

/**
 * Less than Lt.
 */

func Lt(field string, val interface{}) *FieldCondition {
	lt := FieldCondition{
		Field: field,
		Value: val,
		Typ:"lt",
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
		Typ:"lte",
	}
	return &lte
}

/**
 * Greater than gt.
 */

func Gte(field string, val interface{}) *FieldCondition {
	gte := FieldCondition{
		Field: field,
		Value: val,
		Typ:"gte",
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

type Order struct {
	Field string
	Ascending bool
}

func (o Order) String() string {
	s := o.Field + " "
	if o.Ascending {
		s += "asc"
	} else {
		s += "desc"
	}
	return s
}

type Query struct {
	Model string

	Error error

	Backend Backend

	JoinTargetField string
	JoinField string // The DB name of the field that s joined on. (on the model this query is for)
	JoinedField string // The DB name of the field to join on. (in the parent)

	Joins []*RelationQuery

	LimitNum int	
	OffsetNum int
	Orders []Order

	FieldSpec []string
	Filters []Filter
}

func Q(model string) *Query {
	q := Query{}
	q.Model = model

	return &q
}

func (q *Query) Limit(l int) *Query {
	q.LimitNum = l
	return q
}

func (q *Query) Offset(o int) *Query {
	q.OffsetNum = o
	return q
}

func (q *Query) Fields(fields ...string) *Query {
	q.FieldSpec = fields
	return q
}

/**
 * Limit the query to specified fields.
 * If fields where already specified, they will be reduced.
 */
func (q *Query) LimitFields(fields ...string) *Query {
	if q.FieldSpec == nil {
		return q.Fields(fields...)
	}

	allowMap := make(map[string]bool)
	for _, field := range fields {
		allowMap[field] = true
	}

	finalFields := make([]string, 0)

	for _, field := range q.FieldSpec {
		if _, ok := allowMap[field]; ok {
			finalFields = append(finalFields, field)
		}
	}

	q.FieldSpec = finalFields

	return q
}

func (q *Query) Order(name string, asc bool) *Query {
	q.Orders = append(q.Orders, Order{Field: name, Ascending: asc})
	return q
}

func (q *Query) Query(f Filter) *Query {
	q.Filters = append(q.Filters, f)
	return q
}

func (q *Query) filterQ(filter Filter) *Query {
	q.Filters = append(q.Filters, filter)
	return q
}

func (q *Query) Filter(field string, val interface{}) *Query {
	return q.filterQ(Eq(field, val))
}

func (q *Query) FilterCond(field string, condition string, val interface{}) *Query {
	typ := conditionToFilterType(condition)

	f := FieldCondition{
		Typ:typ,
		Field: field,
		Value: val,
	}

	return q.filterQ(&f)
}

func (q *Query) AndQ(filter Filter) *Query {
	return q.filterQ(filter)
}

func (q *Query) And(field string, val interface{}) *Query {
	return q.Filter(field, val)
}


func (q *Query) AndCond(field, condition string, val interface{}) *Query {
	return q.FilterCond(field, condition, val)
}

func (q *Query) OrQ(filter Filter) *Query {
	filterLen := 0
	if q.Filters != nil {
		filterLen = len(q.Filters)
	}

	if filterLen == 0 {
		// No filters set, so just filter regularily.
		return q.filterQ(filter)
	} else if filterLen > 1 {
		// More than one filter.
		// Can not do OR with multiple clauses present.
		q.Error = errors.New("invalid_or_multiple_clauses")
		return q
	}

	// One filter is already present.
	// If it is OR, append to the or. 
	// Otherwise create a new top level Or.
	if q.Filters[0].Type() == "or" {
		or := q.Filters[0].(*OrCondition)
		or.Filters = append(or.Filters, filter)
	} else {
		// Other filter is not an OR, so just OR the two together.
		q.Filters = []Filter{Or(q.Filters[0], filter)}
	}

	return q
}

func (q *Query) Or(field string, val interface{}) *Query {
	return q.OrQ(Eq(field, val))
}

func (q *Query) OrCond(field string, condition string, val interface{}) *Query {
	typ := conditionToFilterType(condition)

	f := FieldCondition{
		Typ:typ,
		Field: field,
		Value: val,
	}

	return q.OrQ(&f)
}

/**
 * Joins.
 */

func (q *Query) JoinQ(jq *RelationQuery) *Query {
	jq.BaseQuery = q
	q.Joins = append(q.Joins, jq)
	return q
}

func (q *Query) Join(fieldName string) *Query {
	q.Joins = append(q.Joins, RelQ(q, fieldName))
	return q
}

/**
 * RelationQuery.
 */

func (q *Query) Related(name string) *RelationQuery {
	return RelQ(q, name)
}

func (q *Query) RelatedCustom(name, joinKey, foreignKey string) *RelationQuery {
	return RelQCustom(q, name, joinKey, foreignKey)
}

type RelationQuery struct {
	Query

	BaseQuery *Query
	RelationName string

	JoinFieldName string
	ForeignFieldName string
}

func RelQ(q *Query, name string) *RelationQuery {
	relQ := RelationQuery{
		BaseQuery: q,
		RelationName: name,
	}
	relQ.Backend = q.Backend

	return &relQ
}

func RelQCustom(q *Query, name, joinKey, foreignKey string) *RelationQuery {
	relQ := RelationQuery{
		BaseQuery: q,
		JoinFieldName: joinKey,
		ForeignFieldName: foreignKey,
	}
	relQ.Model = name
	relQ.Backend = q.Backend

	return &relQ
}

func (q *RelationQuery) Find() ([]Model, DbError) {
	if q.Backend == nil {
		panic("Callind .Find() on a query without backend")
	}

	newQ, err := q.Backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.Find()
}

func (q *RelationQuery) First() (Model, DbError) {
	if q.Backend == nil {
		panic("Callind .First() on a query without backend")
	}

	newQ, err := q.Backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.First()
}

func (q *RelationQuery) Last() (Model, DbError) {
	if q.Backend == nil {
		panic("Callind .Last() on a query without backend")
	}

	newQ, err := q.Backend.BuildRelationQuery(q)
	if err != nil {
		return nil, err
	}
	return newQ.Last()
}


func (q *RelationQuery) Count() (uint64, DbError) {
	if q.Backend == nil {
		panic("Callind .Count() on a query without backend")
	}

	newQ, err := q.Backend.BuildRelationQuery(q)
	if err != nil {
		return 0, err
	}
	return newQ.Count()
}

func (q *RelationQuery) Delete() DbError {
	if q.Backend == nil {
		panic("Callind .Delete() on a query without backend")
	}

	newQ, err := q.Backend.BuildRelationQuery(q)
	if err != nil {
		return err
	}
	return newQ.Delete()
}

/**
 * Backend functions.
 */

func (q *Query) Find() ([]Model, DbError) {
	if q.Backend == nil {
		panic("Calling .Find() on query without backend")
	}

	return q.Backend.Query(q)
}

func (q *Query) First() (Model, DbError) {
	if q.Backend == nil {
		panic("Calling .First() on query without backend")
	}

	return q.Backend.QueryOne(q)
}

func (q *Query) Last() (Model, DbError) {
	if q.Backend == nil {
		panic("Calling .Last() on query without backend")
	}
	return q.Backend.Last(q)
}

func (q *Query) Count() (uint64, DbError) {
	if q.Backend == nil {
		panic("Calling .Count() on query without backend")
	}
	return q.Backend.Count(q)
}


func (q *Query) Delete() DbError {
	if q.Backend == nil {
		panic("Calling .Delete() on query without backend")
	}
	return q.Backend.DeleteMany(q)
}
