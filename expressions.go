package dukedb

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/theduke/go-apperror"
)

/**
 * Expressions.
 *
 * NamedNestedExpression
 * TextExpression
 * FieldTypeExpression
 * ValueExpression
 * IdentifierExpression
 * CollectionFieldIdentifierExpression
 * ConstraintExpression
 * ActionConstraint
 * DefaultValueConstraint
 * CheckConstraint
 * ReferenceConstraint
 * FieldExpression
 * FieldValueExpression
 * FunctionExpression
 * AndExpression
 * OrExpression
 * NotExpression
 * FilterExpression
 * SortExpression
 */

// Expression represents an arbitrary database expression.
type Expression interface {
	// Type returns the type of the expression.
	Type() string
	// Validate validates the expression.
	Validate() apperror.Error
	// GetIdentifiers returns all database identifiers contained within an expression.
	GetIdentifiers() []string
}

/**
 * noValidationExpr mixin.
 */

// noValidationMixin can be used as a mixin if an Expression does not have any
// validations.
type noValidatationMixin struct{}

func (*noValidatationMixin) Validate() apperror.Error {
	return nil
}

/**
 * noIdentifiersMixin.
 */

type noIdentifiersMixin struct{}

func (*noIdentifiersMixin) GetIdentifiers() []string {
	return nil
}

/**
 * NamedExpression.
 */

// NamedExpression is an expression that has a identifying name attached.
type NamedExpression interface {
	Expression
	Name() string
}

type namedExprMixin struct {
	name string
}

func (e *namedExprMixin) Name() string {
	return e.name
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

type typedExprMixin struct {
	valueType reflect.Type
}

func (e *typedExprMixin) ValueType() reflect.Type {
	return e.valueType
}

func (e *typedExprMixin) SetValueType(typ reflect.Type) {
	e.valueType = typ
}

/**
 * NestedExpression interface and embeddable.
 */

// NestedExpression represents an expression which holds another expression.
// An example are SQL functions like MAX(...) or SUM(xxx).
type NestedExpression interface {
	Expression
	Expression() Expression
}

type nestedExprMixin struct {
	expression Expression
}

func (e *nestedExprMixin) Expression() Expression {
	return e.expression
}

func (e *nestedExprMixin) GetIdentifiers() []string {
	return e.expression.GetIdentifiers()
}

/**
 * MultiExpression interface and embeddable.
 */

type MultiExpression interface {
	Expression
	Expressions() []Expression
	SetExpressions(expressions []Expression)
	Add(expression ...Expression)
}

type multiExprMixin struct {
	expressions []Expression
}

func (m *multiExprMixin) Expressions() []Expression {
	return m.expressions
}

func (m *multiExprMixin) SetExpressions(e []Expression) {
	m.expressions = e
}

func (m *multiExprMixin) Add(expr ...Expression) {
	m.expressions = append(m.expressions, expr...)
}

func (m multiExprMixin) GetIdentifiers() []string {
	ids := make([]string, 0)
	for _, expr := range m.expressions {
		ids = append(ids, expr.GetIdentifiers()...)
	}
	return ids
}

/**
 * NamedNestedExpression.
 */

type NamedNestedExpression interface {
	Expression
	Name() string
	Expression() Expression
}

type namedNestedExpr struct {
	namedExprMixin
	nestedExprMixin
}

// Ensure FieldTypeExpression implements NamedNestedExpr.
var _ NamedNestedExpression = (*namedNestedExpr)(nil)

func (*namedNestedExpr) Type() string {
	return "named_nested"
}

func (e *namedNestedExpr) Validate() apperror.Error {
	if e.name == "" {
		return apperror.New("empty_name")
	} else if e.expression == nil {
		return apperror.New("empty_nested_expression")
	}
	return nil
}

func (e *namedNestedExpr) GetIdentifiers() []string {
	return e.expression.GetIdentifiers()
}

// NameExpr attaches a name to another expression.
func NameExpr(name string, expr Expression) NamedNestedExpression {
	e := &namedNestedExpr{}
	e.name = name
	e.expression = expr
	return e
}

/**
 * TextExpression.
 */

type TextExpression interface {
	Expression
	Text() string
}

// TextExpression is plain text that will be used directly in the database.
type textExpr struct {
	noValidatationMixin

	noIdentifiersMixin

	text string
}

// Ensure textExpr implements TextExpression.
var _ TextExpression = (*textExpr)(nil)

func (*textExpr) Type() string {
	return "text"
}

func (e *textExpr) Text() string {
	return e.text
}

func TextExpr(text string) TextExpression {
	return &textExpr{text: text}
}

/**
 * FieldTypeExpression.
 */

type FieldTypeExpression interface {
	TypedExpression
	FieldType() string
}

type fieldTypeExpr struct {
	noIdentifiersMixin
	typedExprMixin

	fieldType string
}

// Ensure FieldTypeExpression implements FieldTypeExpression.
var _ FieldTypeExpression = (*fieldTypeExpr)(nil)

func (*fieldTypeExpr) Type() string {
	return "field_type"
}

func (e *fieldTypeExpr) FieldType() string {
	return e.fieldType
}

func (e *fieldTypeExpr) Validate() apperror.Error {
	if e.fieldType == "" {
		return apperror.New("empty_field_type")
	}
	return nil
}

func FieldTypeExpr(fieldType string, goType reflect.Type) FieldTypeExpression {
	e := &fieldTypeExpr{}
	e.fieldType = fieldType
	e.valueType = goType
	return e
}

/**
 * ValueExpression.
 */

// ValueExpression is an expression which just contains the corresponding value.
// For example, consider a sql statement SELECT * from table where field=X.
// Here, the X would be a ValueExpression.
type ValueExpression interface {
	TypedExpression
	Value() interface{}
}

type valueExpr struct {
	typedExprMixin

	noValidatationMixin
	noIdentifiersMixin

	value interface{}
}

// Make sure ValueExpression implements TypedExpression.
var _ ValueExpression = (*valueExpr)(nil)

func (*valueExpr) Type() string {
	return "value"
}

func (e *valueExpr) Value() interface{} {
	return e.value
}

func ValueExpr(value interface{}, typ ...reflect.Type) ValueExpression {
	e := &valueExpr{value: value}
	if len(typ) > 0 {
		if len(typ) > 1 {
			panic("Called dukedb.Val() with more than one type")
		}
		e.valueType = typ[0]
	}
	return e
}

/**
 * IdentifierExpression.
 */

// IdentifierExpression is a db expression for a database identifier.
// Identifiers could, for example, be column names or table names.

type IdentifierExpression interface {
	Expression

	Identifier() string
}

type identifierExpr struct {
	identifier string
}

// Make sure IdentifierExpression implements TypedExpression.
var _ IdentifierExpression = (*identifierExpr)(nil)

func (*identifierExpr) Type() string {
	return "identifier"
}

func (s *identifierExpr) Identifier() string {
	return s.identifier
}

func (e *identifierExpr) Validate() apperror.Error {
	if e.identifier == "" {
		return apperror.New("empty_identifier")
	}
	return nil
}

func (e identifierExpr) GetIdentifiers() []string {
	return []string{e.identifier}
}

// Identifier is a convenient way to create an IdentifierExpression.
func IdExpr(identifier string) IdentifierExpression {
	return &identifierExpr{
		identifier: identifier,
	}
}

/**
 * CollectionFieldIdentifierExpression.
 */

type CollectionFieldIdentifierExpression interface {
	Expression

	Collection() string
	Field() string
}

type colFieldIdentifierExpr struct {
	collection string
	field      string
}

// Make sure CollectionFieldIdentifierExpression implements TypedExpression.
var _ CollectionFieldIdentifierExpression = (*colFieldIdentifierExpr)(nil)

func (*colFieldIdentifierExpr) Type() string {
	return "collection_field_identifier"
}

func (s *colFieldIdentifierExpr) Collection() string {
	return s.collection
}

func (s *colFieldIdentifierExpr) Field() string {
	return s.field
}

func (e *colFieldIdentifierExpr) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	} else if e.field == "" {
		return apperror.New("empty_field")
	}
	return nil
}

func (e *colFieldIdentifierExpr) GetIdentifiers() []string {
	return []string{e.collection, e.field}
}

// ColFieldIdentifier is a convenient way to create an *CollectionFieldIdentifierExpression.
func ColFieldIdExpr(collection, field string) CollectionFieldIdentifierExpression {
	return &colFieldIdentifierExpr{
		collection: collection,
		field:      field,
	}
}

// BuildIdExpr is a convenience function for creating either an IdentiferExpression or a CollectionFieldIdentifierExpression.
// If collection is "", an IdentifierExpression is returned.
func BuildIdExpr(collection, id string) Expression {
	if collection == "" {
		return IdExpr(id)
	} else {
		return ColFieldIdExpr(collection, id)
	}
}

/**
 * Constraint.
 */

const (
	CONSTRAINT_NOT_NULL       = "not_null"
	CONSTRAINT_UNIQUE         = "unique"
	CONSTRAINT_PRIMARY_KEY    = "pk"
	CONSTRAINT_AUTO_INCREMENT = "auto_increment"
)

var CONSTRAINT_MAP map[string]string = map[string]string{
	"not_null":       "NOT NULL",
	"unique":         "UNIQUE",
	"pk":             "PRIMARY KEY",
	"auto_increment": "AUTO_INCREMENT",
}

type ConstraintExpression interface {
	Expression

	Constraint() string
}

type constraintExpr struct {
	noIdentifiersMixin

	constraint string
}

// Ensure NotNullConstraint implements ConstraintExpression.
var _ ConstraintExpression = (*constraintExpr)(nil)

func (*constraintExpr) Type() string {
	return "constraint"
}

func (e *constraintExpr) Constraint() string {
	return e.constraint
}

func (e *constraintExpr) Validate() apperror.Error {
	if e.constraint == "" {
		return apperror.New("empty_constraint")
	} else if _, ok := CONSTRAINT_MAP[e.constraint]; !ok {
		return apperror.New("unknown_constraint", fmt.Sprintf("Unknown constraint %v", e.constraint))
	}
	return nil
}

func Constr(constraint string) ConstraintExpression {
	if _, ok := CONSTRAINT_MAP[constraint]; !ok {
		panic(fmt.Sprintf("Unknown constraint: %v", constraint))
	}
	return &constraintExpr{
		constraint: constraint,
	}
}

/**
 * ActionConstraint.
 */

const (
	EVENT_UPDATE = "update"
	EVENT_DELETE = "delete"

	ACTION_CASCADE     = "cascade"
	ACTION_RESTRICT    = "restrict"
	ACTION_SET_NULL    = "set_null"
	ACTION_SET_DEFAULT = "set_default"
)

var ACTIONS_MAP map[string]string = map[string]string{
	ACTION_CASCADE:     "CASCADE",
	ACTION_RESTRICT:    "RESTRICT",
	ACTION_SET_NULL:    "SET NULL",
	ACTION_SET_DEFAULT: "SET DEFAULT",
}

type ActionConstraint interface {
	Expression

	Event() string
	Action() string
}

type actionConstraint struct {
	noIdentifiersMixin

	event  string
	action string
}

func (*actionConstraint) Type() string {
	return "action_constraint"
}

func (e *actionConstraint) Event() string {
	return e.event
}

func (e *actionConstraint) Action() string {
	return e.action
}

func (e *actionConstraint) Validate() apperror.Error {
	if e.event == "" {
		return apperror.New("empty_event")
	} else if !(e.event == EVENT_UPDATE || e.event == EVENT_DELETE) {
		return apperror.New("unknown_event", "Event must either be dukedb.EVENT_UPDATE or dukedb.EVENT_DELETE")
	} else if e.action == "" {
		return apperror.New("empty_action")
	} else if _, ok := ACTIONS_MAP[e.action]; !ok {
		return apperror.New("unknown_action", fmt.Sprintf("Unknown action %v", e.action))
	}
	return nil
}

func ActionConstr(event, action string) ActionConstraint {
	return &actionConstraint{
		event:  event,
		action: action,
	}
}

/**
 * DefaultValueConstraint.
 */

type DefaultValueConstraint interface {
	Expression
	DefaultValue() ValueExpression
}

type defaultValueConstraint struct {
	noIdentifiersMixin
	noValidatationMixin

	defaultValue ValueExpression
}

// Ensure DefaultValueConstraint implements ConstraintExpression.
var _ DefaultValueConstraint = (*defaultValueConstraint)(nil)

func (*defaultValueConstraint) Type() string {
	return "default_value_constraint"
}

func (c *defaultValueConstraint) DefaultValue() ValueExpression {
	return c.defaultValue
}

func DefaultValConstr(val interface{}) DefaultValueConstraint {
	return &defaultValueConstraint{
		defaultValue: ValueExpr(val),
	}
}

/**
 * CheckConstraint.
 */

type CheckConstraint interface {
	Expression
	Check() Expression
}

type checkConstraint struct {
	check Expression
}

// Make sure CheckConstraint implements Constraint.
var _ CheckConstraint = (*checkConstraint)(nil)

func (*checkConstraint) Type() string {
	return "check_constraint"
}

func (e *checkConstraint) Check() Expression {
	return e.check
}

func (e *checkConstraint) Validate() apperror.Error {
	if e.check == nil {
		return apperror.New("no_check_expression")
	}
	return nil
}

func (e *checkConstraint) GetIdentifiers() []string {
	return e.check.GetIdentifiers()
}

func CheckConstr(check Expression) CheckConstraint {
	return &checkConstraint{
		check: check,
	}
}

/**
 * ReferenceConstraint.
 */

type ReferenceConstraint interface {
	Expression

	ForeignKey() CollectionFieldIdentifierExpression
}

type referenceConstraint struct {
	noIdentifiersMixin

	foreignKey CollectionFieldIdentifierExpression
}

// Make sure ReferenceConstraint implements Constraint.
var _ ReferenceConstraint = (*referenceConstraint)(nil)

func (*referenceConstraint) Type() string {
	return "reference_constraint"
}

func (e *referenceConstraint) ForeignKey() CollectionFieldIdentifierExpression {
	return e.foreignKey
}

func (e *referenceConstraint) Validate() apperror.Error {
	if e.foreignKey == nil {
		return apperror.New("no_foreign_key")
	}
	return nil
}

func ReferenceConstr(foreignKey CollectionFieldIdentifierExpression) ReferenceConstraint {
	return &referenceConstraint{
		foreignKey: foreignKey,
	}
}

/**
 * FieldExpression.
 */

type FieldExpression interface {
	Expression

	Name() string
	FieldType() FieldTypeExpression
	Constraints() []Expression
}

// FieldExpression represents the definition for a field.
type fieldExpr struct {
	noIdentifiersMixin

	// Name is the field name.
	name        string
	fieldType   FieldTypeExpression
	constraints []Expression
}

// Ensure FieldExpression implements Expression.
var _ FieldExpression = (*fieldExpr)(nil)

func (*fieldExpr) Type() string {
	return "field"
}

func (e *fieldExpr) Name() string {
	return e.name
}

func (e *fieldExpr) FieldType() FieldTypeExpression {
	return e.fieldType
}

func (e *fieldExpr) Constraints() []Expression {
	return e.constraints
}

func (e *fieldExpr) Validate() apperror.Error {
	if e.name == "" {
		return apperror.New("empty_field")
	} else if e.fieldType == nil {
		return apperror.New("empty_field_type")
	}

	return nil
}

func FieldExpr(name string, fieldType FieldTypeExpression, constraints ...Expression) FieldExpression {
	return &fieldExpr{
		name:        name,
		fieldType:   fieldType,
		constraints: constraints,
	}
}

/**
 * FieldValueExpression.
 */

// FieldValueExpression represents a a value for a field.
// Used in create or update statements.
type FieldValueExpression interface {
	Expression

	// Must be either IdentifierExpression or CollectionFieldIdentifierExpression.
	Field() Expression
	Value() Expression
}

type fieldValueExpr struct {
	field Expression
	value Expression
}

// Ensure FieldValueExpression implements Expression.
var _ FieldValueExpression = (*fieldValueExpr)(nil)

func (*fieldValueExpr) Type() string {
	return "field_value"
}

func (e *fieldValueExpr) Field() Expression {
	return e.field
}

func (e *fieldValueExpr) Value() Expression {
	return e.value
}

func (e *fieldValueExpr) Validate() apperror.Error {
	if e.field == nil {
		return apperror.New("empty_field")
	} else if e.value == nil {
		return apperror.New("empty_value")
	}

	// Check that field is either IdentifierExpression of ColfieldIdentifer.
	if _, ok := e.field.(IdentifierExpression); ok {
	} else if _, ok := e.field.(CollectionFieldIdentifierExpression); ok {
	} else {
		return apperror.New("invalid_field_type", "Invalid field type: %v", reflect.TypeOf(e.field))
	}

	return nil
}

func (e fieldValueExpr) GetIdentifiers() []string {
	return e.field.GetIdentifiers()
}

func FieldValExpr(field, value Expression) FieldValueExpression {
	return &fieldValueExpr{
		field: field,
		value: value,
	}
}

/**
 * FunctionExpression.
 */

type FunctionExpression interface {
	NestedExpression
	Function() string
}

// FunctionExpression represents a database function.
type functionExpr struct {
	nestedExprMixin
	function string
}

// Ensure FunctionExpression implements NestedExpression.
var _ FunctionExpression = (*functionExpr)(nil)

func (*functionExpr) Type() string {
	return "function"
}

func (e *functionExpr) Function() string {
	return e.function
}

func (e *functionExpr) Validate() apperror.Error {
	if e.function == "" {
		return apperror.New("empty_function")
	} else if e.expression == nil {
		return apperror.New("empty_function_expression")
	}

	return nil
}

func (e functionExpr) GetIdentifiers() []string {
	return e.expression.GetIdentifiers()
}

func FuncExpr(function string, expr Expression) FunctionExpression {
	e := &functionExpr{
		function: function,
	}
	e.expression = expr
	return e
}

/**
 * Logical AND, OR, NOT expressions.
 */

/**
 * AndExpression.
 */

type AndExpression struct {
	multiExprMixin
}

// Ensure AndCondition implements MultiExpression.
var _ MultiExpression = (*AndExpression)(nil)

func (a *AndExpression) Type() string {
	return "and"
}

func (e *AndExpression) Validate() apperror.Error {
	if len(e.expressions) < 1 {
		return apperror.New("no_and_expressions")
	}
	return nil
}

func AndExpr(exprs ...Expression) *AndExpression {
	e := &AndExpression{}
	e.expressions = exprs
	return e
}

/**
 * Or.
 */

type OrExpression struct {
	multiExprMixin
}

// Ensure OrCondition implements MultiExpression.
var _ MultiExpression = (*OrExpression)(nil)

func (o *OrExpression) Type() string {
	return "or"
}

func (e *OrExpression) Validate() apperror.Error {
	if len(e.expressions) < 1 {
		return apperror.New("no_or_expressions")
	}
	return nil
}

func OrExpr(exprs ...Expression) *OrExpression {
	or := &OrExpression{}
	or.expressions = exprs
	return or
}

/**
 * NOT.
 */

type NotExpression interface {
	Not() Expression
}

type notExpr struct {
	not Expression
}

// Ensure NotCondition implements NestedExpression.
var _ NotExpression = (*notExpr)(nil)

func (*notExpr) Type() string {
	return "not"
}

func (e *notExpr) Not() Expression {
	return e.not
}

func (e *notExpr) Validate() apperror.Error {
	if e.not == nil {
		return apperror.New("no_not_expression")
	}
	return nil
}

func NotExpr(expr ...Expression) NotExpression {
	not := &notExpr{}
	if len(expr) == 1 {
		not.not = expr[0]
	} else if len(expr) > 1 {
		not.not = AndExpr(expr...)
	}
	return not
}

/**
 * Filters.
 */

const (
	OPERATOR_EQ   = "="
	OPERATOR_NEQ  = "!="
	OPERATOR_LIKE = "like"
	OPERATOR_IN   = "in"
	OPERATOR_GT   = ">"
	OPERATOR_GTE  = ">="
	OPERATOR_LT   = "<"
	OPERATOR_LTE  = "<="
)

var OPERATOR_MAP map[string]string = map[string]string{
	OPERATOR_EQ:   "eq",
	OPERATOR_NEQ:  "neq",
	OPERATOR_LIKE: "like",
	OPERATOR_IN:   "in",
	OPERATOR_GT:   "gt",
	OPERATOR_GTE:  "gte",
	OPERATOR_LT:   "lt",
	OPERATOR_LTE:  "lte",
}

func MapOperator(op string) string {
	switch strings.ToLower(op) {
	case "==":
		return "="
	case "=", "!=", "<", "<=", ">", ">=", "like", "in":
		return op
	default:
		return ""
	}
}

/**
 * FilterExpression.
 */

type FilterExpression interface {
	Expression
	Field() Expression
	Operator() string
	Clause() Expression
}

/**
 * Filter.
 */

// Filter represents an expression that filters an arbitrary expression field by a clause with an operator.
type filter struct {
	field    Expression
	operator string
	clause   Expression
}

// Ensure Filter implements Expression.
var _ FilterExpression = (*filter)(nil)

func (*filter) Type() string {
	return "filter"
}

func (f filter) Field() Expression {
	return f.field
}

func (f filter) Operator() string {
	return f.operator
}

func (f filter) Clause() Expression {
	return f.clause
}

func (e *filter) Validate() apperror.Error {
	if e.field == nil {
		return apperror.New("empty_field")
	} else if e.operator == "" {
		return apperror.New("empty_operator")
	} else if _, ok := OPERATOR_MAP[e.operator]; !ok {
		return apperror.New("unknown_operator", fmt.Sprintf("Unknown operator %v", e.operator))
	} else if e.clause == nil {
		return apperror.New("empty_clause")
	}
	return nil
}

func (f filter) GetIdentifiers() []string {
	ids := f.field.GetIdentifiers()
	ids = append(ids, f.clause.GetIdentifiers()...)
	return ids
}

// NewFilter creates a new filter expression.
func FilterExpr(field Expression, operator string, clause Expression) FilterExpression {
	return &filter{
		field:    field,
		operator: operator,
		clause:   clause,
	}
}

func FieldFilter(collection, field, operator string, clause Expression) FilterExpression {
	var fieldExpr Expression
	if collection != "" {
		fieldExpr = ColFieldIdExpr(collection, field)
	} else {
		fieldExpr = IdExpr(field)
	}
	return &filter{
		field:    fieldExpr,
		operator: operator,
		clause:   clause,
	}
}

func FieldValFilter(collection, field, operator string, value interface{}) FilterExpression {
	return FieldFilter(collection, field, operator, ValueExpr(value))
}

/**
 * Eq.
 */

func Eq(field string, val interface{}) FilterExpression {
	return FieldValFilter("", field, OPERATOR_EQ, val)
}

/**
 * Neq.
 */

func Neq(field string, val interface{}) FilterExpression {
	return FieldValFilter("", field, OPERATOR_NEQ, val)
}

/**
 * Like.
 */

func Like(field string, val interface{}) FilterExpression {
	return FieldValFilter("", field, OPERATOR_LIKE, val)
}

/**
 * In.
 */

func In(field string, val interface{}) FilterExpression {
	return FieldValFilter("", field, OPERATOR_IN, val)
}

/**
 * Less than Lt.
 */

func Lt(field string, val interface{}) FilterExpression {
	return FieldValFilter("", field, OPERATOR_LT, val)
}

/**
 * Less than eqal Lte.
 */

func Lte(field string, val interface{}) FilterExpression {
	return FieldValFilter("", field, OPERATOR_LTE, val)
}

/**
 * Greater than gt.
 */

func Gt(field string, val interface{}) FilterExpression {
	return FieldValFilter("", field, OPERATOR_GT, val)
}

/**
 * Greater than equal gte.
 */

func Gte(field string, val interface{}) FilterExpression {
	return FieldValFilter("", field, OPERATOR_GTE, val)
}

/**
 * SortExpression.
 */

type SortExpression interface {
	NestedExpression
	Ascending() bool
}

type sortExpr struct {
	nestedExprMixin
	ascending bool
}

// Ensure SortExpression implements Expression.
var _ SortExpression = (*sortExpr)(nil)

func (*sortExpr) Type() string {
	return "sort"
}

func (s *sortExpr) Ascending() bool {
	return s.ascending
}

func (e *sortExpr) Validate() apperror.Error {
	if e.expression == nil {
		return apperror.New("empty_field_expression")
	}
	return nil
}

func SortExpr(expr Expression, ascending bool) SortExpression {
	e := &sortExpr{
		ascending: ascending,
	}
	e.expression = expr
	return e
}

func Sort(collection, field string, ascending bool) SortExpression {
	return SortExpr(BuildIdExpr(collection, field), ascending)
}
