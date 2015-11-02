package expressions

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/theduke/go-apperror"
)

type Expression interface {
}

// ValidatableExpression must be implemented by expressions that can be
// validated.
type ValidatableExpression interface {
	Validate() apperror.Error
}

// IdentifierExpression must be implemented by expressions containing
// identifiers.
type IdentifierExpression interface {
	GetIdentifiers() []Expression
}

func getIdentifiers(expr Expression) []Expression {
	if idents, ok := expr.(IdentifierExpr); ok {
		return idents.GetIdentifiers()
	}
	return nil
}

/**
 * FieldedExpression.
 */

type FieldedExpression interface {
	Fields() []Expression
	AddField(field Expression)
	SetFields(fields []Expression)
}

type fieldedExprMixin struct {
	fields []Expression
}

func (e *fieldedExprMixin) Fields() []Expression {
	return e.fields
}

func (e *fieldedExprMixin) AddField(field Expression) {
	e.fields = append(e.fields, field)
}

func (e *fieldedExprMixin) SetFields(fields []Expression) {
	e.fields = fields
}

/**
 * NamedExpression.
 */

// NamedExpression should be implemented by expressions that are/can be named.
type NamedExpression interface {
	Name() string
	SetName(name string)
}

type namedExprMixin struct {
	name string
}

func (e *namedExprMixin) Name() string {
	return e.name
}

func (e *namedExprMixin) SetName(name string) {
	e.name = name
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
	Type() reflect.Type
	SetType(typ reflect.Type)
}

type typedExprMixin struct {
	typ reflect.Type
}

func (e *typedExprMixin) Type() reflect.Type {
	return e.typ
}

func (e *typedExprMixin) SetType(typ reflect.Type) {
	e.typ = typ
}

/**
 * NamedTypedExpression.
 */

type NamedTypedExpression interface {
	NamedExpression
	TypedExpression
}

/**
 * NestedExpression interface and embeddable.
 */

// NestedExpression represents an expression which holds another expression.
// An example are SQL functions like MAX(...) or SUM(xxx).
type NestedExpression interface {
	Expression() Expression
}

type nestedExprMixin struct {
	expression Expression
}

func (e *nestedExprMixin) Expression() Expression {
	return e.expression
}

func (e *nestedExprMixin) GetIdentifiers() []Expression {
	return getIdentifiers(e.expression)
}

/**
 * MultiExpression interface and embeddable.
 */

type MultiExpression interface {
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

func (m multiExprMixin) GetIdentifiers() []Expression {
	ids := make([]Expression, 0)
	for _, expr := range m.expressions {
		if idents, ok := expr.(IdentifierExpression); ok {
			ids = append(ids, idents.GetIdentifiers()...)
		}
	}
	return ids
}

/**
 * NamedNestedExpression.
 */

type NamedNestedExpr struct {
	namedExprMixin
	typedExprMixin
	nestedExprMixin
}

func (e *NamedNestedExpr) Validate() apperror.Error {
	if e.name == "" {
		return apperror.New("empty_name")
	} else if e.expression == nil {
		return apperror.New("empty_nested_expression")
	}
	return nil
}

// NameExpr attaches a name to another expression.
func NameExpr(name string, expr Expression, typ ...reflect.Type) *NamedNestedExpr {
	e := &NamedNestedExpr{}
	e.name = name
	e.expression = expr
	if len(typ) > 0 {
		e.typ = typ[0]
	}
	return e
}

/**
 * TextExpression.
 */

// TextExpression is plain text that will be used directly in the database.
type TextExpr struct {
	text string
}

func (e *TextExpr) Text() string {
	return e.text
}

func NewTextExpr(text string) *TextExpr {
	return &TextExpr{text: text}
}

/**
 * FieldTypeExpression.
 */

type FieldTypeExpr struct {
	typedExprMixin
	fieldType string
}

func (e *FieldTypeExpr) FieldType() string {
	return e.fieldType
}

func (e *FieldTypeExpr) Validate() apperror.Error {
	if e.fieldType == "" {
		return apperror.New("empty_field_type")
	}
	return nil
}

func NewFieldTypeExpr(fieldType string, goType reflect.Type) *FieldTypeExpr {
	e := &FieldTypeExpr{}
	e.fieldType = fieldType
	e.typ = goType
	return e
}

/**
 * ValueExpression.
 */

// ValueExpression is an expression which just contains the corresponding value.
// For example, consider a sql statement SELECT * from table where field=X.
// Here, the X would be a ValueExpression.
type ValueExpr struct {
	typedExprMixin
	value interface{}
}

func (e *ValueExpr) Value() interface{} {
	return e.value
}

func NewValueExpr(value interface{}, typ ...reflect.Type) *ValueExpr {
	e := &ValueExpr{value: value}
	if len(typ) > 0 {
		if len(typ) > 1 {
			panic("Called dukedb.Val() with more than one type")
		}
		e.typ = typ[0]
	}
	return e
}

/**
 * IdentifierExpression.
 */

// IdentifierExpression is a db expression for a database identifier.
// Identifiers could, for example, be column names or table names.

type IdentifierExpr struct {
	identifier string
}

func (s *IdentifierExpr) Identifier() string {
	return s.identifier
}

func (s *IdentifierExpr) SetIdentifier(id string) {
	s.identifier = id
}

func (e *IdentifierExpr) Validate() apperror.Error {
	if e.identifier == "" {
		return apperror.New("empty_identifier")
	}
	return nil
}

func (e *IdentifierExpr) GetIdentifiers() []Expression {
	return []Expression{e.identifier}
}

// Identifier is a convenient way to create an IdentifierExpression.
func NewIdExpr(identifier string) *IdentifierExpr {
	return &IdentifierExpr{
		identifier: identifier,
	}
}

/**
 * CollectionFieldIdentifierExpression.
 */

type ColFieldIdentifierExpr struct {
	collection string
	field      string
}

func (s *ColFieldIdentifierExpr) Collection() string {
	return s.collection
}

func (s *ColFieldIdentifierExpr) SetCollection(collection string) {
	s.collection = collection
}

func (s *ColFieldIdentifierExpr) Field() string {
	return s.field
}

func (s *ColFieldIdentifierExpr) SetField(field string) {
	s.field = field
}

func (e *ColFieldIdentifierExpr) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	} else if e.field == "" {
		return apperror.New("empty_field")
	}
	return nil
}

func (e *ColFieldIdentifierExpr) GetIdentifiers() []Expression {
	return []Expression{e.collection, e.field}
}

// ColFieldIdentifier is a convenient way to create an *CollectionFieldIdentifierExpression.
func NewColFieldIdExpr(collection, field string) *ColFieldIdentifierExpr {
	return &ColFieldIdentifierExpr{
		collection: collection,
		field:      field,
	}
}

/**
 * FieldSelectorExpr
 */

type FieldSelectorExpr struct {
	NamedNestedExpr
}

func NewFieldSelectorExpr(name string, expr Expression, typ reflect.Type) *FieldSelectorExpr {
	e := &FieldSelectorExpr{}
	e.name = name
	e.expression = expr
	e.typ = typ
	return e
}

func NewFieldSelector(name, collection, field string, typ reflect.Type) *FieldSelectorExpr {
	id := BuildIdExpr(collection, field)
	e := &FieldSelectorExpr{}
	e.name = name
	e.expression = id
	e.typ = typ
	return e
}

// BuildIdExpr is a convenience function for creating either an IdentiferExpression or a CollectionFieldIdentifierExpression.
// If collection is "", an IdentifierExpression is returned.
func BuildIdExpr(collection, id string) Expression {
	if collection == "" {
		return NewIdExpr(id)
	} else {
		return NewColFieldIdExpr(collection, id)
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
	CONSTRAINT_INDEX          = "index"
)

var CONSTRAINT_MAP map[string]string = map[string]string{
	CONSTRAINT_NOT_NULL:       "NOT NULL",
	CONSTRAINT_UNIQUE:         "UNIQUE",
	CONSTRAINT_PRIMARY_KEY:    "PRIMARY KEY",
	CONSTRAINT_AUTO_INCREMENT: "AUTO_INCREMENT",
	CONSTRAINT_INDEX:          "",
}

type ConstraintExpr struct {
	constraint string
}

func (e *ConstraintExpr) Constraint() string {
	return e.constraint
}

func (e *ConstraintExpr) Validate() apperror.Error {
	if e.constraint == "" {
		return apperror.New("empty_constraint")
	} else if _, ok := CONSTRAINT_MAP[e.constraint]; !ok {
		return apperror.New("unknown_constraint", fmt.Sprintf("Unknown constraint %v", e.constraint))
	}
	return nil
}

func NewConstraintExpr(constraint string) *ConstraintExpr {
	if _, ok := CONSTRAINT_MAP[constraint]; !ok {
		panic(fmt.Sprintf("Unknown constraint: %v", constraint))
	}
	return &ConstraintExpr{
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

type ActionConstraint struct {
	event  string
	action string
}

func (e *ActionConstraint) Event() string {
	return e.event
}

func (e *ActionConstraint) Action() string {
	return e.action
}

func (e *ActionConstraint) Validate() apperror.Error {
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

func NewActionConstraint(event, action string) *ActionConstraint {
	return &ActionConstraint{
		event:  event,
		action: action,
	}
}

/**
 * DefaultValueConstraint.
 */

type DefaultValueConstraint struct {
	defaultValue Expression
}

func (c *DefaultValueConstraint) DefaultValue() Expression {
	return c.defaultValue
}

func NewDefaultValConstraint(expr Expression) *DefaultValueConstraint {
	e := &DefaultValueConstraint{
		defaultValue: expr,
	}
	return e
}

/**
 * CheckConstraint.
 */

type CheckConstraint struct {
	check Expression
}

func (e *CheckConstraint) Check() Expression {
	return e.check
}

func (e *CheckConstraint) Validate() apperror.Error {
	if e.check == nil {
		return apperror.New("no_check_expression")
	}
	return nil
}

func (e *CheckConstraint) GetIdentifiers() []Expression {
	if idents, ok := e.check.(IdentifierExpression); ok {
		return idents.GetIdentifiers()
	}
	return nil
}

func NewCheckConstraint(check Expression) *CheckConstraint {
	return &CheckConstraint{
		check: check,
	}
}

/**
 * ReferenceConstraint.
 */

type ReferenceConstraint struct {
	foreignKey *ColFieldIdentifierExpr
}

func (e *ReferenceConstraint) ForeignKey() *ColFieldIdentifierExpr {
	return e.foreignKey
}

func (e *ReferenceConstraint) Validate() apperror.Error {
	if e.foreignKey == nil {
		return apperror.New("no_foreign_key")
	}
	return nil
}

func NewReferenceConstraint(collection, field string) *ReferenceConstraint {
	return &ReferenceConstraint{
		foreignKey: NewColFieldIdExpr(collection, field),
	}
}

/**
 * UniqueFieldsConstraint.
 */

type UniqueFieldsConstraint struct {
	fields []Expression
}

func (c *UniqueFieldsConstraint) UniqueFields() []Expression {
	return c.fields
}

func (c *UniqueFieldsConstraint) Validate() apperror.Error {
	if len(c.fields) < 1 {
		return apperror.New("no_unique_fields")
	}
	return nil
}

func NewUniqueFieldsConstraint(fields ...Expression) *UniqueFieldsConstraint {
	return &UniqueFieldsConstraint{
		fields: fields,
	}
}

/**
 * FieldExpression.
 */

// FieldExpression represents the definition for a field.
type FieldExpr struct {
	// Name is the field name.
	name        string
	fieldType   *FieldTypeExpr
	constraints []Expression
}

func (e *FieldExpr) Name() string {
	return e.name
}

func (e *FieldExpr) FieldType() *FieldTypeExpr {
	return e.fieldType
}

func (e *FieldExpr) Constraints() []Expression {
	return e.constraints
}

func (e *FieldExpr) AddConstraint(constraint Expression) {
	e.constraints = append(e.constraints, constraint)
}

func (e *FieldExpr) Validate() apperror.Error {
	if e.name == "" {
		return apperror.New("empty_field")
	} else if e.fieldType == nil {
		return apperror.New("empty_field_type")
	}

	return nil
}

func NewFieldExpr(name string, fieldType *FieldTypeExpr, constraints ...Expression) *FieldExpr {
	return &FieldExpr{
		name:        name,
		fieldType:   fieldType,
		constraints: constraints,
	}
}

/**
 * FieldValueExpression.
 */

type FieldValueExpr struct {
	field Expression
	value Expression
}

func (e *FieldValueExpr) Field() Expression {
	return e.field
}

func (e *FieldValueExpr) Value() Expression {
	return e.value
}

func (e *FieldValueExpr) Validate() apperror.Error {
	if e.field == nil {
		return apperror.New("empty_field")
	} else if e.value == nil {
		return apperror.New("empty_value")
	}

	// Check that field is either IdentifierExpression of ColfieldIdentifer.
	if _, ok := e.field.(*IdentifierExpr); ok {
	} else if _, ok := e.field.(*ColFieldIdentifierExpr); ok {
	} else {
		return apperror.New("invalid_field_type", "Invalid field type: %v", reflect.TypeOf(e.field))
	}

	return nil
}

func (e *FieldValueExpr) GetIdentifiers() []Expression {
	if idents, ok := e.field.(IdentifierExpression); ok {
		return idents.GetIdentifiers()
	}
	return nil
}

func NewFieldValExpr(field, value Expression) *FieldValueExpr {
	return &FieldValueExpr{
		field: field,
		value: value,
	}
}

func NewFieldVal(field string, value interface{}, typ ...reflect.Type) *FieldValueExpr {
	return NewFieldValExpr(NewIdExpr(field), NewValueExpr(value, typ...))
}

/**
 * FunctionExpression.
 */

// FunctionExpression represents a database function.
type FunctionExpr struct {
	nestedExprMixin
	function string
}

func (e *FunctionExpr) Function() string {
	return e.function
}

func (e *FunctionExpr) Validate() apperror.Error {
	if e.function == "" {
		return apperror.New("empty_function")
	} else if e.expression == nil {
		return apperror.New("empty_function_expression")
	}

	return nil
}

func (e *FunctionExpr) GetIdentifiers() []Expression {
	return getIdentifiers(e.expression)
}

func NewFuncExpr(function string, expr Expression) *FunctionExpr {
	e := &FunctionExpr{
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

type AndExpr struct {
	multiExprMixin
}

func (e *AndExpr) Validate() apperror.Error {
	if len(e.expressions) < 1 {
		return apperror.New("no_and_expressions")
	}
	return nil
}

func NewAndExpr(exprs ...Expression) *AndExpr {
	e := &AndExpr{}
	e.expressions = exprs
	return e
}

/**
 * Or.
 */

type OrExpr struct {
	multiExprMixin
}

func (e *OrExpr) Validate() apperror.Error {
	if len(e.expressions) < 1 {
		return apperror.New("no_or_expressions")
	}
	return nil
}

func NewOrExpr(exprs ...Expression) *OrExpr {
	or := &OrExpr{}
	or.expressions = exprs
	return or
}

/**
 * NOT.
 */

type NotExpr struct {
	not Expression
}

func (e *NotExpr) Not() Expression {
	return e.not
}

func (e *NotExpr) Validate() apperror.Error {
	if e.not == nil {
		return apperror.New("no_not_expression")
	}
	return nil
}

func (e *NotExpr) GetIdentifiers() []Expression {
	return getIdentifiers(e.not)
}

func NewNotExpr(expr ...Expression) *NotExpr {
	not := &NotExpr{}
	if len(expr) == 1 {
		not.not = expr[0]
	} else if len(expr) > 1 {
		not.not = NewAndExpr(expr...)
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
	Field() Expression
	Operator() string
	Clause() Expression
}

/**
 * Filter.
 */

// Filter represents an expression that filters an arbitrary expression field by a clause with an operator.
type Filter struct {
	field    Expression
	operator string
	clause   Expression
}

func (f *Filter) Field() Expression {
	return f.field
}

func (f *Filter) Operator() string {
	return f.operator
}

func (f *Filter) Clause() Expression {
	return f.clause
}

func (e *Filter) Validate() apperror.Error {
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

func (f *Filter) GetIdentifiers() []Expression {
	ids := getIdentifiers(f.field)
	ids = append(ids, getIdentifiers(f.clause)...)
	return ids
}

// NewFilter creates a new filter expression.
func NewFilter(field Expression, operator string, clause Expression) *Filter {
	return &Filter{
		field:    field,
		operator: operator,
		clause:   clause,
	}
}

func NewFieldFilter(collection, field, operator string, clause Expression) *Filter {
	var fieldExpr Expression
	if collection != "" {
		fieldExpr = NewColFieldIdExpr(collection, field)
	} else {
		fieldExpr = NewIdExpr(field)
	}
	return &Filter{
		field:    fieldExpr,
		operator: operator,
		clause:   clause,
	}
}

func NewFieldValFilter(collection, field, operator string, value interface{}) *Filter {
	return NewFieldFilter(collection, field, operator, NewValueExpr(value))
}

/**
 * Eq.
 */

func Eq(collection, field string, val interface{}) *Filter {
	return NewFieldValFilter(collection, field, OPERATOR_EQ, val)
}

/**
 * Neq.
 */

func Neq(collection, field string, val interface{}) *Filter {
	return NewFieldValFilter(collection, field, OPERATOR_NEQ, val)
}

/**
 * Like.
 */

func Like(collection, field string, val interface{}) *Filter {
	return NewFieldValFilter(collection, field, OPERATOR_LIKE, val)
}

/**
 * In.
 */

func In(collection, field string, val interface{}) *Filter {
	return NewFieldValFilter(collection, field, OPERATOR_IN, val)
}

/**
 * Less than Lt.
 */

func Lt(collection, field string, val interface{}) *Filter {
	return NewFieldValFilter(collection, field, OPERATOR_LT, val)
}

/**
 * Less than eqal Lte.
 */

func Lte(collection, field string, val interface{}) *Filter {
	return NewFieldValFilter(collection, field, OPERATOR_LTE, val)
}

/**
 * Greater than gt.
 */

func Gt(collection, field string, val interface{}) *Filter {
	return NewFieldValFilter(collection, field, OPERATOR_GT, val)
}

/**
 * Greater than equal gte.
 */

func Gte(collection, field string, val interface{}) *Filter {
	return NewFieldValFilter(collection, field, OPERATOR_GTE, val)
}

/**
 * SortExpression.
 */

type SortExpr struct {
	nestedExprMixin
	ascending bool
}

func (s *SortExpr) Ascending() bool {
	return s.ascending
}

func (s *SortExpr) SetAscending(asc bool) {
	s.ascending = asc
}

func (e *SortExpr) Validate() apperror.Error {
	if e.expression == nil {
		return apperror.New("empty_field_expression")
	}
	return nil
}

func NewSortExpr(expr Expression, ascending bool) *SortExpr {
	e := &SortExpr{
		ascending: ascending,
	}
	e.expression = expr
	return e
}

func NewSort(collection, field string, ascending bool) *SortExpr {
	return NewSortExpr(BuildIdExpr(collection, field), ascending)
}
