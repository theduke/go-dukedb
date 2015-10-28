package dukedb

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/theduke/go-apperror"
)

const (
	CONSTRAINT_CASCADE     = "cascade"
	CONSTRAINT_RESTRICT    = "restrict"
	CONSTRAINT_SET_NULL    = "set_null"
	CONSTRAINT_SET_DEFAULT = "set_default"
)

var CONSTRAINTS map[string]bool = map[string]bool{
	"cascade":     true,
	"restrict":    true,
	"set_null":    true,
	"set_default": true,
}

/**
 * Expressions.
 *
 * NamedNestedExpr
 * TextExpression
 * FieldTypeExpression
 * ValueExpression
 * IdentifierExpression
 * CollectionFieldIdentifierExpression
 * NotNullConstraint
 * UniqueConstraint
 * UniqueFieldsConstraint
 * PrimaryKeyConstraint
 * AutoIncrementConstraint
 * DefaultValueConstraint
 * FieldUpdateConstraint
 * FieldDeleteConstraint
 * IndexConstraint
 * CheckConstraint
 * ReferenceConstraint
 * FieldExpression
 * FieldValueExpression
 * FunctionExpression
 * AndExpression
 * OrExpression
 * NotExpression
 * Filter
 * FieldFilter
 * FieldValueFilter
 * SortExpression
 */

// Expression represents an arbitrary database expression.
type Expression interface {
	// Type returns the type of the expression.
	Type() string
	// Validate validates the expression.
	Validate() apperror.Error
	// IsCacheable returns a flag indicating if this expression may be cached.
	IsCacheable() bool
	// GetIdentifiers returns all database identifiers contained within an expression.
	GetIdentifiers() []string
}

// NamedExpression is an expression that has a identifying name attached.
type NamedExpression interface {
	Expression
	GetName() string
}

type NamedExpr struct {
	Name string
}

func (e *NamedExpr) GetName() string {
	return e.Name
}

/**
 * TypedExpression.
 */

// TypedExpression represents a database expression that contains or results in a value.
// The ValueType is a a reflect.Type value
// Examples are field expresession that result in a field value.
//
// * SELECT fieldname FROM table: fieldname is an IdentifierExpression and a
// TypedExpression with the value of the field type.
type TypedExpression interface {
	Expression
	ValueType() reflect.Type
	SetValueType(typ reflect.Type)
}

type TypedExpr struct {
	Typ reflect.Type
}

func (e *TypedExpr) ValueType() reflect.Type {
	return e.Typ
}

func (e *TypedExpr) SetValueType(typ reflect.Type) {
	e.Typ = typ
}

/**
 * ArgumentExpression interface and embeddable.
 */

type ArgumentExpression interface {
	TypedExpression
	GetArgument() interface{}
	SetArgument(arg interface{})
}

type ArgumentExpr struct {
	TypedExpr
	Arg interface{}
}

func (e *ArgumentExpr) GetArgument() interface{} {
	return e.Arg
}

func (e *ArgumentExpr) SetArgument(arg interface{}) {
	e.Arg = e
}

/**
 * ArgumentsExpression interface and embeddable.
 */

// ArgumentsExpression represents an expression which requires arguments.
type ArgumentsExpression interface {
	GetArguments() []interface{}
	SetArguments(args []interface{})
	AddArguments(arg ...interface{})
}

type ArgumentsExpr struct {
	Args []interface{}
}

func (e *ArgumentsExpr) GetArguments() []interface{} {
	return e.Args
}

func (e *ArgumentsExpr) SetArguments(args []interface{}) {
	e.Args = args
}

func (e *ArgumentsExpr) AddArguments(args ...interface{}) {
	for _, arg := range args {
		e.Args = append(e.Args, arg)
	}
}

/**
 * NestedExpression interface and embeddable.
 */

// NestedExpression represents an expression which holds another expression.
// An example are SQL functions like MAX(...) or SUM(xxx).
type NestedExpression interface {
	Expression
	GetExpression() Expression
}

type NestedExpr struct {
	Nested Expression
}

func (e *NestedExpr) GetExpression() Expression {
	return e.Nested
}

func (e *NestedExpr) GetIdentifiers() []string {
	return e.Nested.GetIdentifiers()
}

/**
 * MultiExpression interface and embeddable.
 */

type MultiExpression interface {
	Expression
	GetExpressions() []Expression
	SetExpressions(expressions []Expression)
	Add(expression ...Expression)
}

type MultiExpr struct {
	Expressions []Expression
}

func (m *MultiExpr) GetExpressions() []Expression {
	return m.Expressions
}

func (m *MultiExpr) SetExpressions(e []Expression) {
	m.Expressions = e
}

func (m *MultiExpr) Add(expr ...Expression) {
	for _, e := range expr {
		m.Expressions = append(m.Expressions, e)
	}
}

func (m MultiExpr) GetIdentifiers() []string {
	ids := make([]string, 0)
	for _, expr := range m.Expressions {
		ids = append(ids, expr.GetIdentifiers()...)
	}
	return ids
}

/**
 * NamedNestedExpression.
 */

type NamedNestedExpression interface {
	Expression
	GetName() string
	GetExpression() Expression
}

type NamedNestedExpr struct {
	Name       string
	Expression Expression
}

// Ensure FieldTypeExpression implements Named and NestedExpression.
var _ Expression = (*NamedNestedExpr)(nil)

func (*NamedNestedExpr) Type() string {
	return "named_nested"
}

func (e *NamedNestedExpr) Validate() apperror.Error {
	if e.Name == "" {
		return apperror.New("empty_name")
	} else if e.Expression == nil {
		return apperror.New("empty_nested_expression")
	}
	return nil
}

func (e *NamedNestedExpr) IsCacheable() bool {
	return false
}

func (e *NamedNestedExpr) GetIdentifiers() []string {
	return e.Expression.GetIdentifiers()
}

func WrapNamedExpression(name string, expr Expression) *NamedNestedExpr {
	return &NamedNestedExpr{
		Name:       name,
		Expression: expr,
	}
}

/**
 * TextExpression.
 */

// TextExpression is plain text that will be used directly in the database.
type TextExpression struct {
	Text string
}

// Ensure FieldTypeExpression implements Expression.
var _ Expression = (*TextExpression)(nil)

func (*TextExpression) Type() string {
	return "text"
}

func (e *TextExpression) Validate() apperror.Error {
	return nil
}

func (e *TextExpression) IsCacheable() bool {
	return false
}

func (e *TextExpression) GetIdentifiers() []string {
	return nil
}

/**
 * FieldTypeExpression.
 */

type FieldTypeExpression struct {
	GoType reflect.Type
	Typ    string
}

// Ensure FieldTypeExpression implements Expression.
var _ Expression = (*FieldTypeExpression)(nil)

func (*FieldTypeExpression) Type() string {
	return "field_type"
}

func (e *FieldTypeExpression) Validate() apperror.Error {
	if e.GoType == nil && e.Typ == "" {
		return apperror.New("empty_type")
	}
	return nil
}

func (e *FieldTypeExpression) IsCacheable() bool {
	return false
}

func (e *FieldTypeExpression) GetIdentifiers() []string {
	return nil
}

/**
 * ValueExpression.
 */

// ValueExpression is an expression which just contains the corresponding value.
// For example, consider a sql statement SELECT * from table where field=X.
// Here, the X would be a ValueExpression.
type ValueExpression struct {
	TypedExpr
	Val interface{}
}

// Make sure ValueExpression implements TypedExpression.
var _ TypedExpression = (*ValueExpression)(nil)

func (*ValueExpression) Type() string {
	return "value"
}

func (e *ValueExpression) Validate() apperror.Error {
	if e.Val == nil {
		return apperror.New("empty_value")
	}
	return nil
}

func (e *ValueExpression) IsCacheable() bool {
	return false
}

func (ValueExpression) GetIdentifiers() []string {
	return []string{}
}

func Val(value interface{}, typ ...reflect.Type) *ValueExpression {
	e := &ValueExpression{Val: value}
	if len(typ) > 0 {
		if len(typ) > 1 {
			panic("Called dukedb.Val() with more than one type")
		}
		e.Typ = typ[0]
	}
	return e
}

/**
 * IdentifierExpression.
 */

// IdentifierExpression is a db expression for a database identifier.
// Identifiers could, for example, be column names or table names.
type IdentifierExpression struct {
	TypedExpr
	Identifier string
}

// Make sure IdentifierExpression implements TypedExpression.
var _ TypedExpression = (*IdentifierExpression)(nil)

func (*IdentifierExpression) Type() string {
	return "identifier"
}

func (e *IdentifierExpression) Validate() apperror.Error {
	if e.Identifier == "" {
		return apperror.New("empty_identifier")
	}
	return nil
}

func (e *IdentifierExpression) IsCacheable() bool {
	return false
}

func (e IdentifierExpression) GetIdentifiers() []string {
	return []string{e.Identifier}
}

// Identifier is a convenient way to create an *IdentifierExpression.
func Identifier(identifier string, typ ...reflect.Type) *IdentifierExpression {
	e := &IdentifierExpression{
		Identifier: identifier,
	}
	if len(typ) > 0 {
		if len(typ) > 1 {
			panic("Called dukedb.Identifier() with more than one type")
		}
		e.Typ = typ[0]
	}
	return e
}

/**
 * CollectionFieldIdentifierExpression.
 */

type CollectionFieldIdentifierExpression struct {
	TypedExpr
	Collection string
	Field      string
}

// Make sure CollectionFieldIdentifierExpression implements TypedExpression.
var _ TypedExpression = (*CollectionFieldIdentifierExpression)(nil)

func (*CollectionFieldIdentifierExpression) Type() string {
	return "collection_field_identifier"
}

func (e *CollectionFieldIdentifierExpression) Validate() apperror.Error {
	if e.Field == "" {
		return apperror.New("empty_field")
	}
	return nil
}

func (e *CollectionFieldIdentifierExpression) IsCacheable() bool {
	return false
}

func (e CollectionFieldIdentifierExpression) GetIdentifiers() []string {
	return []string{e.Collection, e.Field}
}

// ColFieldIdentifier is a convenient way to create an *CollectionFieldIdentifierExpression.
func ColFieldIdentifier(collection, field string, typ ...reflect.Type) *CollectionFieldIdentifierExpression {
	e := &CollectionFieldIdentifierExpression{
		Collection: collection,
		Field:      field,
	}
	if len(typ) > 0 {
		if len(typ) > 1 {
			panic("Called dukedb.Val() with more than one type")
		}
		e.Typ = typ[0]
	}
	return e
}

/**
 * ConstraintExpression.
 */

type ConstraintExpression interface {
	Expression
	GetName() string
}

type ConstraintExpr struct {
	Name string
}

func (e ConstraintExpr) GetName() string {
	return e.Name
}

func (*ConstraintExpr) GetIdentifiers() []string {
	return nil
}

/**
 * NotNullConstraint.
 */

type NotNullConstraint struct {
	ConstraintExpr
}

// Ensure NotNullConstraint implements ConstraintExpression.
var _ ConstraintExpression = (*NotNullConstraint)(nil)

func (*NotNullConstraint) Type() string {
	return "not_null"
}

func (e *NotNullConstraint) Validate() apperror.Error {
	return nil
}

func (e *NotNullConstraint) IsCacheable() bool {
	return false
}

/**
 * UniqueConstraint.
 */

type UniqueConstraint struct {
	ConstraintExpr
}

// Ensure UniqueConstraint implements ConstraintExpression.
var _ ConstraintExpression = (*UniqueConstraint)(nil)

func (e *UniqueConstraint) Validate() apperror.Error {
	return nil
}

func (e *UniqueConstraint) IsCacheable() bool {
	return false
}

func (*UniqueConstraint) Type() string {
	return "unique"
}

/**
 * UniqueFieldsConstraint.
 */

// UniqueFieldsConstraint is a collection constraint for multiple fields to be
// unique together.
// UniqueConstraint, in comparison, is only for a single field.
type UniqueFieldsConstraint struct {
	ConstraintExpr
	Fields []*IdentifierExpression
}

func (e *UniqueFieldsConstraint) Validate() apperror.Error {
	return nil
}

func (e *UniqueFieldsConstraint) IsCacheable() bool {
	return false
}

// Ensure UniqueFieldsConstraint implements ConstraintExpression.
var _ ConstraintExpression = (*UniqueFieldsConstraint)(nil)

func (*UniqueFieldsConstraint) Type() string {
	return "unique_fields"
}

/**
 * PrimaryKeyConstraint.
 */

type PrimaryKeyConstraint struct {
	ConstraintExpr
}

// Ensure NotNullConstraint implements ConstraintExpression.
var _ ConstraintExpression = (*PrimaryKeyConstraint)(nil)

func (*PrimaryKeyConstraint) Type() string {
	return "primary_key"
}

func (e *PrimaryKeyConstraint) Validate() apperror.Error {
	return nil
}

func (e *PrimaryKeyConstraint) IsCacheable() bool {
	return false
}

/**
 * AutoIncrementConstraint.
 */

type AutoIncrementConstraint struct {
	ConstraintExpr
}

// Ensure AutoIncrementConstraint implements ConstraintExpression.
var _ ConstraintExpression = (*AutoIncrementConstraint)(nil)

func (*AutoIncrementConstraint) Type() string {
	return "auto_increment"
}

func (e *AutoIncrementConstraint) Validate() apperror.Error {
	return nil
}

func (e *AutoIncrementConstraint) IsCacheable() bool {
	return false
}

/**
 * DefaultValueConstraint.
 */

type DefaultValueConstraint struct {
	ConstraintExpr
	Value Expression
}

// Ensure DefaultValueConstraint implements ConstraintExpression.
var _ ConstraintExpression = (*DefaultValueConstraint)(nil)

func (*DefaultValueConstraint) Type() string {
	return "default_value_constraint"
}

func (e *DefaultValueConstraint) Validate() apperror.Error {
	return nil
}

func (e *DefaultValueConstraint) IsCacheable() bool {
	return false
}

/**
 * FieldUpdateConstraint.
 */

type FieldUpdateConstraint struct {
	ConstraintExpr
	// Action is the action to be taken when a field is updated.
	// See CONSTRAINT_* constants.
	Action string
}

// Make sure FieldUpdateConstraint implements Expression.
var _ ConstraintExpression = (*FieldUpdateConstraint)(nil)

func (*FieldUpdateConstraint) Type() string {
	return "field_update_constraint"
}

func (e *FieldUpdateConstraint) Validate() apperror.Error {
	if e.Action == "" {
		return apperror.New("empty_action")
	} else if _, ok := CONSTRAINTS[e.Action]; !ok {
		return apperror.New("unknown_update_action", fmt.Sprintf("Unknown action for field update constraint: %v", e.Action))
	}
	return nil
}

func (e *FieldUpdateConstraint) IsCacheable() bool {
	return false
}

/**
 * FieldDeleteConstraint.
 */

type FieldDeleteConstraint struct {
	ConstraintExpr
	// Action is the action to be taken when a field is deleted.
	// See CONSTRAINT_* constants.
	Action string
}

// Make sure FieldUpdateConstraint implements Expression.
var _ ConstraintExpression = (*FieldDeleteConstraint)(nil)

func (*FieldDeleteConstraint) Type() string {
	return "field_delete_constraint"
}

func (e *FieldDeleteConstraint) Validate() apperror.Error {
	if e.Action == "" {
		return apperror.New("empty_action")
	} else if _, ok := CONSTRAINTS[e.Action]; !ok {
		return apperror.New("unknown_update_action", fmt.Sprintf("Unknown action for field update constraint: %v", e.Action))
	}
	return nil
}

func (e *FieldDeleteConstraint) IsCacheable() bool {
	return false
}

/**
 * IndexConstraint.
 *
 * Note: IndexConstraint 'misuses' the ConstraintExpr.Name field for the index
 * name.
 */

type IndexConstraint struct {
	ConstraintExpr
}

// Make sure IndexConstraint implements Constraint.
var _ ConstraintExpression = (*IndexConstraint)(nil)

func (*IndexConstraint) Type() string {
	return "index_constraint"
}

func (e *IndexConstraint) Validate() apperror.Error {
	return nil
}

func (e *IndexConstraint) IsCacheable() bool {
	return false
}

/**
 * CheckConstraint.
 */

type CheckConstraint struct {
	ConstraintExpr
	Checks []*FieldFilter
}

// Make sure CheckConstraint implements Constraint.
var _ ConstraintExpression = (*CheckConstraint)(nil)

func (*CheckConstraint) Type() string {
	return "check_constraint"
}

func (e *CheckConstraint) Validate() apperror.Error {
	if len(e.Checks) < 1 {
		return apperror.New("no_check_filters")
	}
	return nil
}

func (e *CheckConstraint) IsCacheable() bool {
	return false
}

/**
 * ReferenceConstraint.
 */

type ReferenceConstraint struct {
	ConstraintExpr
	ForeignKey *CollectionFieldIdentifierExpression
}

// Make sure ReferenceConstraint implements Constraint.
var _ ConstraintExpression = (*ReferenceConstraint)(nil)

func (*ReferenceConstraint) Type() string {
	return "reference_constraint"
}

func (e *ReferenceConstraint) Validate() apperror.Error {
	if e.ForeignKey == nil {
		return apperror.New("no_foreing_key")
	}
	return nil
}

func (e *ReferenceConstraint) IsCacheable() bool {
	return false
}

/**
 * FieldExpression.
 */

// FieldExpression represents the definition for a field.
type FieldExpression struct {
	// Name is the field name.
	Name string

	Typ *FieldTypeExpression

	Constraints []*ConstraintExpression
}

// Ensure FieldExpression implements Expression.
var _ Expression = (*FieldExpression)(nil)

func (*FieldExpression) Type() string {
	return "field"
}

func (e *FieldExpression) Validate() apperror.Error {
	if e.Name == "" {
		return apperror.New("empty_field")
	} else if e.Typ == nil {
		return apperror.New("empty_type")
	}

	return nil
}

func (e *FieldExpression) IsCacheable() bool {
	return false
}

func (FieldExpression) GetIdentifiers() []string {
	return nil
}

/**
 * FieldValueExpression.
 */

// FieldValueExpression represents a a value for a field.
// Used in create or update statements.
type FieldValueExpression struct {
	Field *IdentifierExpression
	Value *ValueExpression
}

// Ensure FieldValueExpression implements Expression.
var _ Expression = (*FieldValueExpression)(nil)

func (*FieldValueExpression) Type() string {
	return "field_value"
}

func (e *FieldValueExpression) Validate() apperror.Error {
	if e.Field == nil {
		return apperror.New("empty_field")
	} else if e.Value == nil {
		return apperror.New("empty_value")
	}

	return nil
}

func (e *FieldValueExpression) IsCacheable() bool {
	return false
}

func (e FieldValueExpression) GetIdentifiers() []string {
	return e.Field.GetIdentifiers()
}

func FieldVal(field string, val interface{}) *FieldValueExpression {
	return &FieldValueExpression{
		Field: Identifier(field),
		Value: Val(val),
	}
}

/**
 * FunctionExpression.
 */

// FunctionExpression represents a database function.
type FunctionExpression struct {
	NestedExpr
	Function string
}

// Ensure FunctionExpression implements NestedExpression.
var _ NestedExpression = (*FunctionExpression)(nil)

func (*FunctionExpression) Type() string {
	return "function"
}

func (e *FunctionExpression) Validate() apperror.Error {
	if e.Function == "" {
		return apperror.New("empty_function")
	} else if e.Nested == nil {
		return apperror.New("empty_function_expression")
	}

	return nil
}

func (e *FunctionExpression) IsCacheable() bool {
	return false
}

func (e FunctionExpression) GetIdentifiers() []string {
	return e.NestedExpr.GetIdentifiers()
}

func Func(function string, expr Expression) *FunctionExpression {
	e := &FunctionExpression{
		Function: function,
	}
	e.Nested = expr
	return e
}

/**
 * Logical AND, OR, NOT expressions.
 */

/**
 * AndExpression.
 */

type AndExpression struct {
	MultiExpr
}

// Ensure AndCondition implements MultiExpression.
var _ MultiExpression = (*AndExpression)(nil)

func (a *AndExpression) Type() string {
	return "and"
}

func (e *AndExpression) Validate() apperror.Error {
	if len(e.Expressions) < 1 {
		return apperror.New("no_and_expressions")
	}
	return nil
}

func (e *AndExpression) IsCacheable() bool {
	return false
}

func And(exprs ...Expression) *AndExpression {
	e := &AndExpression{}
	e.Expressions = exprs
	return e
}

/**
 * Or.
 */

type OrExpression struct {
	MultiExpr
}

// Ensure OrCondition implements MultiExpression.
var _ MultiExpression = (*OrExpression)(nil)

func (o *OrExpression) Type() string {
	return "or"
}

func (e *OrExpression) Validate() apperror.Error {
	if len(e.Expressions) < 1 {
		return apperror.New("no_or_expressions")
	}
	return nil
}

func (e *OrExpression) IsCacheable() bool {
	return false
}

func Or(exprs ...Expression) *OrExpression {
	or := &OrExpression{}
	or.Expressions = exprs
	return or
}

/**
 * NOT.
 */

type NotExpression struct {
	NestedExpr
}

// Ensure NotCondition implements NestedExpression.
var _ NestedExpression = (*NotExpression)(nil)

func (*NotExpression) Type() string {
	return "not"
}

func (e *NotExpression) Validate() apperror.Error {
	if e.Nested == nil {
		return apperror.New("no_not_expression")
	}
	return nil
}

func (e *NotExpression) IsCacheable() bool {
	return false
}

func Not(expr ...Expression) *NotExpression {
	not := &NotExpression{}
	if len(expr) == 1 {
		not.Nested = expr[0]
	} else if len(expr) > 1 {
		not.Nested = And(expr...)
	}
	return not
}

/**
 * Filters.
 */

const (
	OPERATOR_EQ   = "eq"
	OPERATOR_NEQ  = "neq"
	OPERATOR_LIKE = "like"
	OPERATOR_IN   = "in"
	OPERATOR_GT   = "gt"
	OPERATOR_GTE  = "gte"
	OPERATOR_LT   = "lt"
	OPERATOR_LTE  = "lte"
)

var OPERATOR_MAP map[string]bool = map[string]bool{
	OPERATOR_EQ:   true,
	OPERATOR_NEQ:  true,
	OPERATOR_LIKE: true,
	OPERATOR_IN:   true,
	OPERATOR_GT:   true,
	OPERATOR_GTE:  true,
	OPERATOR_LT:   true,
	OPERATOR_LTE:  true,
}

func MapOperator(op string) string {
	switch strings.ToLower(op) {
	case "==", "=":
		return OPERATOR_EQ
	case "!=":
		return OPERATOR_NEQ
	case "<":
		return OPERATOR_LT
	case "<=":
		return OPERATOR_LTE
	case ">":
		return OPERATOR_GT
	case ">=":
		return OPERATOR_GTE
	case "like":
		return OPERATOR_LIKE
	case "in":
		return OPERATOR_IN
	default:
		return ""
	}
}

/**
 * FilterExpression.
 */

type FilterExpression interface {
	Expression
	GetField() Expression
	GetOperator() string
	GetClause() Expression
}

/**
 * Filter.
 */

// Filter represents an expression that filters an arbitrary expression field by a clause with an operator.
type Filter struct {
	Field    Expression
	Operator string
	Clause   Expression
}

// Ensure Filter implements Expression.
var _ FilterExpression = (*Filter)(nil)

func (*Filter) Type() string {
	return "filter"
}

func (e *Filter) Validate() apperror.Error {
	if e.Field == nil {
		return apperror.New("empty_field")
	} else if e.Operator == "" {
		return apperror.New("empty_operator")
	} else if _, ok := OPERATOR_MAP[e.Operator]; !ok {
		return apperror.New("unknown_operator", fmt.Sprintf("Unknown operator %v", e.Operator))
	} else if e.Clause == nil {
		return apperror.New("empty_clause")
	}
	return nil
}

func (e *Filter) IsCacheable() bool {
	return false
}

func (f Filter) GetIdentifiers() []string {
	ids := f.Field.GetIdentifiers()
	ids = append(ids, f.Clause.GetIdentifiers()...)
	return ids
}

func (f Filter) GetField() Expression {
	return f.Field
}

func (f Filter) GetOperator() string {
	return f.Operator
}

func (f Filter) GetClause() Expression {
	return f.Clause
}

// NewFilter creates a new filter expression.
func NewFilter(field Expression, operator string, clause Expression) *Filter {
	return &Filter{
		Field:    field,
		Operator: operator,
		Clause:   clause,
	}
}

// F is a convenient alias for NewFilter().
func F(field Expression, operator string, clause Expression) *Filter {
	return NewFilter(field, operator, clause)
}

/**
 * FieldFilter.
 */

// FieldFilter is a filter that filters a database field by an expression.
type FieldFilter struct {
	Field    *CollectionFieldIdentifierExpression
	Operator string
	Clause   Expression
}

// Ensure FieldFilter implements Expression.
var _ FilterExpression = (*FieldFilter)(nil)

func (*FieldFilter) Type() string {
	return "field_filter"
}

func (e *FieldFilter) Validate() apperror.Error {
	if e.Field == nil {
		return apperror.New("empty_field")
	} else if e.Operator == "" {
		return apperror.New("empty_operator")
	} else if _, ok := OPERATOR_MAP[e.Operator]; !ok {
		return apperror.New("unknown_operator", fmt.Sprintf("Unknown operator %v", e.Operator))
	} else if e.Clause == nil {
		return apperror.New("empty_clause")
	}
	return nil
}

func (e *FieldFilter) IsCacheable() bool {
	return false
}

func (f FieldFilter) GetIdentifiers() []string {
	ids := f.Field.GetIdentifiers()
	ids = append(ids, f.Clause.GetIdentifiers()...)
	return ids
}

func (f FieldFilter) GetField() Expression {
	return f.Field
}

func (f FieldFilter) GetOperator() string {
	return f.Operator
}

func (f FieldFilter) GetClause() Expression {
	return f.Clause
}

// NewFieldFilter creates a new field filter expression.
func NewFieldFilter(field *CollectionFieldIdentifierExpression, operator string, clause Expression) *FieldFilter {
	return &FieldFilter{
		Field:    field,
		Operator: operator,
		Clause:   clause,
	}
}

// FF is a convenient alias for NewFieldFilter().
func FF(collection, field, operator string, clause Expression) *FieldFilter {
	return NewFieldFilter(ColFieldIdentifier(collection, field), operator, clause)
}

/**
 * FieldValueFilter.
 */

// FieldFilter is a filter that filters a database field by an expression.
type FieldValueFilter struct {
	Field    *CollectionFieldIdentifierExpression
	Operator string
	Value    *ValueExpression
}

// Ensure FieldValueFilter implements Expression.
var _ FilterExpression = (*FieldValueFilter)(nil)

func (*FieldValueFilter) Type() string {
	return "field_value_filter"
}

func (e *FieldValueFilter) Validate() apperror.Error {
	if e.Field == nil {
		return apperror.New("empty_field")
	} else if e.Operator == "" {
		return apperror.New("empty_operator")
	} else if _, ok := OPERATOR_MAP[e.Operator]; !ok {
		return apperror.New("unknown_operator", fmt.Sprintf("Unknown operator %v", e.Operator))
	} else if e.Value == nil {
		return apperror.New("empty_value")
	}
	return nil
}

func (e *FieldValueFilter) IsCacheable() bool {
	return false
}

func (f FieldValueFilter) GetIdentifiers() []string {
	return f.Field.GetIdentifiers()
}

func (f FieldValueFilter) GetField() Expression {
	return f.Field
}

func (f FieldValueFilter) GetOperator() string {
	return f.Operator
}

func (f FieldValueFilter) GetClause() Expression {
	return f.Value
}

// NewFieldFilter creates a new field filter expression.
func ValFilter(collection, field, operator string, val *ValueExpression) *FieldValueFilter {
	return &FieldValueFilter{
		Field:    ColFieldIdentifier(collection, field),
		Operator: operator,
		Value:    val,
	}
}

/**
 * Eq.
 */

func Eq(field string, val interface{}) *FieldValueFilter {
	return &FieldValueFilter{
		Field:    ColFieldIdentifier("", field),
		Operator: OPERATOR_EQ,
		Value:    Val(val),
	}
}

/**
 * Neq.
 */

func Neq(field string, val interface{}) *FieldValueFilter {
	return &FieldValueFilter{
		Field:    ColFieldIdentifier("", field),
		Operator: OPERATOR_NEQ,
		Value:    Val(val),
	}
}

/**
 * Like.
 */

func Like(field string, val interface{}) *FieldValueFilter {
	return &FieldValueFilter{
		Field:    ColFieldIdentifier("", field),
		Operator: OPERATOR_LIKE,
		Value:    Val(val),
	}
}

/**
 * In.
 */

func In(field string, val interface{}) *FieldValueFilter {
	return &FieldValueFilter{
		Field:    ColFieldIdentifier("", field),
		Operator: OPERATOR_IN,
		Value:    Val(val),
	}
}

/**
 * Less than Lt.
 */

func Lt(field string, val interface{}) *FieldValueFilter {
	return &FieldValueFilter{
		Field:    ColFieldIdentifier("", field),
		Operator: OPERATOR_LT,
		Value:    Val(val),
	}
}

/**
 * Less than eqal Lte.
 */

func Lte(field string, val interface{}) *FieldValueFilter {
	return &FieldValueFilter{
		Field:    ColFieldIdentifier("", field),
		Operator: OPERATOR_LTE,
		Value:    Val(val),
	}
}

/**
 * Greater than gt.
 */

func Gt(field string, val interface{}) *FieldValueFilter {
	return &FieldValueFilter{
		Field:    ColFieldIdentifier("", field),
		Operator: OPERATOR_GT,
		Value:    Val(val),
	}
}

/**
 * Greater than equal gte.
 */

func Gte(field string, val interface{}) *FieldValueFilter {
	return &FieldValueFilter{
		Field:    ColFieldIdentifier("", field),
		Operator: OPERATOR_GTE,
		Value:    Val(val),
	}
}

/**
 * SortExpression.
 */

type SortExpression struct {
	Field     Expression
	Ascending bool
}

// Ensure SortExpression implements Expression.
var _ NestedExpression = (*SortExpression)(nil)

func (*SortExpression) Type() string {
	return "sort"
}

func (e *SortExpression) Validate() apperror.Error {
	if e.Field == nil {
		return apperror.New("empty_field")
	}
	return nil
}

func (e *SortExpression) IsCacheable() bool {
	return false
}

func (e SortExpression) GetExpression() Expression {
	return e.Field
}

func (e *SortExpression) GetIdentifiers() []string {
	return e.Field.GetIdentifiers()
}

func SortExpr(field Expression, ascending bool) *SortExpression {
	return &SortExpression{
		Field:     field,
		Ascending: ascending,
	}
}

func Sort(field string, ascending bool) *SortExpression {
	return &SortExpression{
		Field:     Identifier(field),
		Ascending: ascending,
	}
}
