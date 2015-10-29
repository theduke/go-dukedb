package dukedb

import (
	"fmt"
	"strings"

	"github.com/theduke/go-apperror"
)

/**
 * List of statements:
 *
 * CreateCollectionStatement
 * RenameCollectionStatement
 * DropCollectionStatement
 * AddCollectionFieldStatement
 * RenameCollectionFieldStatement
 * DropCollectionFieldStatement
 * CreateIndexStatement
 * DropIndexStatement
 * SelectStatement
 * JoinStatement
 * MutationStatement
 * CreateStatement
 * UpdateStatement
* /

/**
 * Statements.
*/

/**
 * CreateCollectionStatement.
 */

type CreateCollectionStatement interface {
	Expression

	Collection() string
	IfNotExists() bool
	Fields() []FieldExpression
	Constraints() []Expression
}

// CollectionStatement represents the definition for a collection.
type createCollectionStmt struct {
	noIdentifiersMixin

	// Name is the collection name.
	collection string

	ifNotExists bool

	// Fields are the collection fields.
	fields []FieldExpression

	// Constraints are the constraints applied to the table, like
	// UniqueFieldsConstraint, CheckConstraint, ...
	constraints []Expression
}

// Ensure CreateCollectionStatement implements Expression.
var _ CreateCollectionStatement = (*createCollectionStmt)(nil)

func (*createCollectionStmt) Type() string {
	return "create_collection"
}

func (s *createCollectionStmt) Collection() string {
	return s.collection
}

func (s *createCollectionStmt) IfNotExists() bool {
	return s.ifNotExists
}

func (s *createCollectionStmt) Fields() []FieldExpression {
	return s.fields
}

func (s *createCollectionStmt) Constraints() []Expression {
	return s.constraints
}

func (e *createCollectionStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	}
	return nil
}

func CreateColStmt(collection string, ifNotExists bool, fields []FieldExpression, constraints []Expression) CreateCollectionStatement {
	return &createCollectionStmt{
		collection:  collection,
		ifNotExists: ifNotExists,
		fields:      fields,
		constraints: constraints,
	}
}

/**
 * RenameCollectionStatement.
 */

type RenameCollectionStatement interface {
	Expression

	Collection() string
	NewName() string
}

type renameCollectionStmt struct {
	noIdentifiersMixin

	// Collection is the current name of the collection.
	collection string
	newName    string
}

// Ensure RenameCollectionStatement implements Expression.
var _ RenameCollectionStatement = (*renameCollectionStmt)(nil)

func (*renameCollectionStmt) Type() string {
	return "rename_collection"
}

func (s *renameCollectionStmt) Collection() string {
	return s.collection
}

func (s *renameCollectionStmt) NewName() string {
	return s.newName
}

func (e *renameCollectionStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	} else if e.newName == "" {
		return apperror.New("empty_new_collection_name")
	}
	return nil
}

func RenameColStmt(collection, newName string) RenameCollectionStatement {
	return &renameCollectionStmt{
		collection: collection,
		newName:    newName,
	}
}

/**
 * DropCollectionStatement.
 */

type DropCollectionStatement interface {
	Expression

	Collection() string
	IfExists() bool
	Cascade() bool
}

// DropCollectionStatement is an expression for dropping a collection.
type dropCollectionStmt struct {
	noIdentifiersMixin

	// Name is the collection name.
	collection string

	ifExists bool
	cascade  bool
}

// Ensure DropCollectionStatement implements Expression.
var _ DropCollectionStatement = (*dropCollectionStmt)(nil)

func (*dropCollectionStmt) Type() string {
	return "drop_collection"
}

func (s *dropCollectionStmt) Collection() string {
	return s.collection
}

func (s *dropCollectionStmt) IfExists() bool {
	return s.ifExists
}

func (s *dropCollectionStmt) Cascade() bool {
	return s.cascade
}

func (e *dropCollectionStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	}
	return nil
}

func DropColStmt(collection string, ifExists, cascade bool) DropCollectionStatement {
	return &dropCollectionStmt{
		collection: collection,
		ifExists:   ifExists,
		cascade:    cascade,
	}
}

/**
 * CreateFieldStatement.
 */

type CreateFieldStatement interface {
	Expression

	Collection() string
	Field() FieldExpression
}

// AddCollectionFieldStatement is an expression to add a field to a collection.
type createFieldStmt struct {
	noIdentifiersMixin

	collection string
	field      FieldExpression
}

// Ensure AddCollectionFieldStatement implements Expression.
var _ CreateFieldStatement = (*createFieldStmt)(nil)

func (*createFieldStmt) Type() string {
	return "create_collection_field"
}

func (s *createFieldStmt) Collection() string {
	return s.collection
}

func (s *createFieldStmt) Field() FieldExpression {
	return s.field
}

func (e *createFieldStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	} else if e.Field == nil {
		return apperror.New("empty_field")
	}
	return nil
}

func CreateFieldStmt(collection string, field FieldExpression) CreateFieldStatement {
	return &createFieldStmt{
		collection: collection,
		field:      field,
	}
}

/**
 * RenameFieldStatement.
 */

type RenameFieldStatement interface {
	Expression

	Collection() string
	Field() string
	NewName() string
}

type renameFieldStmt struct {
	noIdentifiersMixin

	collection string
	field      string
	newName    string
}

// Ensure RenameCollectionFieldStatement implements Expression.
var _ RenameFieldStatement = (*renameFieldStmt)(nil)

func (*renameFieldStmt) Type() string {
	return "rename_collection_field"
}

func (s *renameFieldStmt) Collection() string {
	return s.collection
}

func (s *renameFieldStmt) Field() string {
	return s.field
}

func (s *renameFieldStmt) NewName() string {
	return s.newName
}

func (e *renameFieldStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	} else if e.field == "" {
		return apperror.New("empty_field")
	} else if e.newName == "" {
		return apperror.New("empty_new_field_name")
	}
	return nil
}

func RenameFieldStmt(collection, field, newName string) RenameFieldStatement {
	return &renameFieldStmt{
		collection: collection,
		field:      field,
		newName:    newName,
	}
}

/**
 * DropFieldStatement.
 */

type DropFieldStatement interface {
	Expression

	Collection() string
	Field() string
	IfExists() bool
	Cascade() bool
}

type dropFieldStmt struct {
	noIdentifiersMixin

	collection string
	field      string
	ifExists   bool
	cascade    bool
}

// Ensure DropCollectionFieldStatement implements Expression.
var _ DropFieldStatement = (*dropFieldStmt)(nil)

func (*dropFieldStmt) Type() string {
	return "drop_collection_field"
}

func (s *dropFieldStmt) Collection() string {
	return s.collection
}

func (s *dropFieldStmt) Field() string {
	return s.field
}

func (s *dropFieldStmt) IfExists() bool {
	return s.ifExists
}

func (s *dropFieldStmt) Cascade() bool {
	return s.cascade
}

func (e *dropFieldStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	} else if e.field == "" {
		return apperror.New("empty_field")
	}
	return nil
}

func DropFieldStmt(collection, field string, ifExists, cascade bool) DropFieldStatement {
	return &dropFieldStmt{
		collection: collection,
		field:      field,
		ifExists:   ifExists,
		cascade:    cascade,
	}
}

/**
 * CreateIndexStatement.
 */

type CreateIndexStatement interface {
	Expression

	IndexName() string
	IndexExpression() Expression

	Expressions() []Expression
	Unique() bool
	Method() string
}

type createIndexStmt struct {
	noIdentifiersMixin

	name            string
	indexExpression Expression
	expressions     []Expression
	unique          bool
	// Indexing method.
	method string
}

// Ensure AddIndexStatement implements Expression.
var _ CreateIndexStatement = (*createIndexStmt)(nil)

func (*createIndexStmt) Type() string {
	return "create_index"
}

func (s *createIndexStmt) IndexName() string {
	return s.name
}

func (s *createIndexStmt) IndexExpression() Expression {
	return s.indexExpression
}

func (s *createIndexStmt) Expressions() []Expression {
	return s.expressions
}

func (s *createIndexStmt) Unique() bool {
	return s.unique
}

func (s *createIndexStmt) Method() string {
	return s.method
}

func (e *createIndexStmt) Validate() apperror.Error {
	if len(e.expressions) < 1 {
		return apperror.New("no_index_expressions")
	} else if e.name == "" {
		return apperror.New("empty_index_name")
	}
	return nil
}

func CreateIndexStmt(name string, indexExpr Expression, expressions []Expression, unique bool, method string) CreateIndexStatement {
	return &createIndexStmt{
		name:            name,
		indexExpression: indexExpr,
		expressions:     expressions,
		unique:          unique,
		method:          method,
	}
}

/**
 * DropIndexStatement.
 */

type DropIndexStatement interface {
	Expression

	IndexName() string
	IfExists() bool
	Cascade() bool
}

type dropIndexStmt struct {
	noIdentifiersMixin

	name     string
	ifExists bool
	cascade  bool
}

// Ensure DropIndexStatement implements Expression.
var _ DropIndexStatement = (*dropIndexStmt)(nil)

func (*dropIndexStmt) Type() string {
	return "drop_index"
}

func (s *dropIndexStmt) IndexName() string {
	return s.name
}

func (s *dropIndexStmt) IfExists() bool {
	return s.ifExists
}

func (s *dropIndexStmt) Cascade() bool {
	return s.cascade
}

func (e *dropIndexStmt) Validate() apperror.Error {
	if e.name == "" {
		return apperror.New("empty_index_name")
	}
	return nil
}

func DropIndexStmt(name string, ifExists, cascade bool) DropIndexStatement {
	return &dropIndexStmt{
		name:     name,
		ifExists: ifExists,
		cascade:  cascade,
	}
}

/**
 * SelectStatement.
 */

type SelectStatement interface {
	NamedExpression

	Collection() string
	SetCollection(collection string)

	Fields() []Expression
	SetFields(fields []Expression)
	AddField(fields ...Expression)

	Filter() Expression
	SetFilter(f Expression)
	FilterAnd(filter Expression)
	FilterOr(filter Expression)

	Sorts() []SortExpression
	SetSorts(sorts []SortExpression)
	AddSort(sort SortExpression)

	Limit() int
	SetLimit(limit int)

	Offset() int
	SetOffset(offset int)

	Joins() []JoinStatement
	SetJoins(joins []JoinStatement)
	GetJoin(name string) JoinStatement
	AddJoin(join JoinStatement)
}

// SelectStatement represents a database select.
type selectStmt struct {
	namedExprMixin
	noIdentifiersMixin

	collection string
	// Fields are arbitrary field expressions.
	fields []Expression
	filter Expression
	sorts  []SortExpression

	limit  int
	offset int

	joins []JoinStatement
}

// Ensure SelectStatement implements NamedExpression.
var _ SelectStatement = (*selectStmt)(nil)

func SelectStmt(collection string) SelectStatement {
	return &selectStmt{
		collection: collection,
	}
}

func (*selectStmt) Type() string {
	return "select"
}

func (s *selectStmt) Collection() string {
	return s.collection
}

func (s *selectStmt) SetCollection(col string) {
	s.collection = col
}

/**
 * Fields.
 */

func (s *selectStmt) Fields() []Expression {
	return s.fields
}

func (s *selectStmt) SetFields(fields []Expression) {
	s.fields = fields
}

func (s *selectStmt) AddField(fields ...Expression) {
	s.fields = append(s.fields, fields...)
}

/**
 * Filters.
 */

func (s *selectStmt) Filter() Expression {
	return s.filter
}

func (s *selectStmt) SetFilter(filter Expression) {
	s.filter = filter
}

func (s *selectStmt) FilterAnd(filter Expression) {
	if s.filter == nil {
		s.filter = filter
	} else if andExpr, ok := s.filter.(*AndExpression); ok {
		andExpr.Add(filter)
	} else {
		s.filter = AndExpr(s.filter, filter)
	}
}

func (s *selectStmt) FilterOr(filter Expression) {
	if s.filter == nil {
		s.filter = filter
	} else if orExpr, ok := s.filter.(*OrExpression); ok {
		orExpr.Add(filter)
	} else {
		s.filter = OrExpr(s.filter, filter)
	}
}

/**
 * Sorts.
 */

func (s *selectStmt) Sorts() []SortExpression {
	return s.sorts
}

func (s *selectStmt) SetSorts(sorts []SortExpression) {
	s.sorts = sorts
}

func (s *selectStmt) AddSort(sort SortExpression) {
	s.sorts = append(s.sorts, sort)
}

/**
 * Limit.
 */

func (s *selectStmt) Limit() int {
	return s.limit
}

func (s *selectStmt) SetLimit(limit int) {
	s.limit = limit
}

/**
 * Offset.
 */

func (s *selectStmt) Offset() int {
	return s.offset
}

func (s *selectStmt) SetOffset(offset int) {
	s.offset = offset
}

/**
 * Joins.
 */

func (s *selectStmt) Joins() []JoinStatement {
	return s.joins
}

func (s *selectStmt) SetJoins(joins []JoinStatement) {
	for _, join := range joins {
		join.SetParentSelect(s)
	}
	s.joins = joins
}

func (s *selectStmt) AddJoin(join JoinStatement) {
	join.SetParentSelect(s)
	s.joins = append(s.joins, join)
}

// Retrieve a join query for the specified field.
// Supports nested Joins like 'Parent.Tags'.
func (s *selectStmt) GetJoin(field string) JoinStatement {
	// Avoid extra work if no joins are set.
	if s.joins == nil || len(s.joins) == 0 {
		return nil
	}

	parts := strings.Split(field, ".")
	if len(parts) > 1 {
		field = parts[0]
	}

	for _, join := range s.joins {
		if join.RelationName() == field {
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

func (e *selectStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	}
	return nil
}

func (s selectStmt) GetIdentifiers() []string {
	ids := make([]string, 0)
	// Fields.
	for _, f := range s.fields {
		ids = append(ids, f.GetIdentifiers()...)
	}
	// Filter.
	ids = append(ids, s.filter.GetIdentifiers()...)
	// Sorts.
	for _, sort := range s.sorts {
		ids = append(ids, sort.GetIdentifiers()...)
	}
	// Joins.
	for _, join := range s.joins {
		ids = append(ids, join.GetIdentifiers()...)
	}
	return ids
}

/**
 * JoinStatement.
 */

const (
	JOIN_INNER       = "inner"
	JOIN_LEFT        = "left"
	JOIN_OUTER_LEFT  = "outer_left"
	JOIN_RIGHT       = "right"
	JOIN_OUTER_RIGHT = "outer_right"
	JOIN_FULL        = "full"
	JOIN_OUTER_FULL  = "outer_full"
	JOIN_CROSS       = "cross"
)

var JOIN_MAP map[string]string = map[string]string{
	"inner":       "INNER JOIN",
	"left":        "LEFT JOIN",
	"outer_left":  "LEFT OUTER JOIN",
	"right":       "RIGHT JOIN",
	"outer_right": "RIGHT OUTER JOIN",
	"full":        "FULL JOIN",
	"outer_full":  "FULL OUTER JOIN",
	"cross":       "CROSS JOIN",
}

const (
	RELATION_TYPE_HAS_ONE    = "has_one"
	RELATION_TYPE_HAS_MANY   = "has_many"
	RELATION_TYPE_BELONGS_TO = "belongs_to"
	RELATION_TYPE_M2M        = "m2m"
)

var RELATION_TYPE_MAP map[string]bool = map[string]bool{
	"has_one":    true,
	"has_many":   true,
	"belongs_to": true,
	"m2m":        true,
}

type JoinStatement interface {
	SelectStatement

	// The parent select statement.
	ParentSelect() SelectStatement
	SetParentSelect(stmt SelectStatement)

	RelationName() string
	SetRelationName(name string)

	// One of the RELATION_TPYE_* constants.
	RelationType() string
	SetRelationType(typ string)

	// One of the JOIN_* constants.
	JoinType() string
	SetJoinType(typ string)

	JoinCondition() Expression
	SetJoinCondition(expr Expression)

	// Returns the select for the join.
	// Do not confuse it with ParentSelect(), which returns the select this join belongs to.
	SelectStatement() SelectStatement
}

// JoinStatement represents a database join.
type joinStmt struct {
	selectStmt

	parent SelectStatement

	// One of the RELATION_TYPE_* constants.
	relationType string

	relationName string

	// One of the JOIN_* constants.
	joinType string

	joinCondition Expression
}

// Ensure JoinStatement implements Expression.
var _ JoinStatement = (*joinStmt)(nil)

func (*joinStmt) Type() string {
	return "join"
}

func (s *joinStmt) ParentSelect() SelectStatement {
	return s.parent
}

func (s *joinStmt) SetParentSelect(stmt SelectStatement) {
	s.parent = stmt
}

func (s *joinStmt) RelationName() string {
	return s.relationName
}

func (s *joinStmt) SetRelationName(n string) {
	s.relationName = n
}

func (s *joinStmt) RelationType() string {
	return s.relationType
}

func (s *joinStmt) SetRelationType(typ string) {
	s.relationType = typ
}

func (s *joinStmt) JoinType() string {
	return s.joinType
}

func (s *joinStmt) SetJoinType(typ string) {
	s.joinType = typ
}

func (s *joinStmt) JoinCondition() Expression {
	return s.joinCondition
}

func (s *joinStmt) SetJoinCondition(e Expression) {
	s.joinCondition = e
}

func (s *joinStmt) SelectStatement() SelectStatement {
	return &s.selectStmt
}

func (e *joinStmt) Validate() apperror.Error {
	if err := e.selectStmt.Validate(); err != nil {
		return err
	} else if e.joinType == "" {
		return apperror.New("empty_join_type")
	} else if _, ok := JOIN_MAP[e.joinType]; !ok {
		return apperror.New("unknown_join_type", fmt.Sprintf("Unknown join type %v", e.joinType))
	} else if e.joinCondition == nil {
		return apperror.New("no_join_condition_expression")
	} else if e.relationType == "" {
		return apperror.New("no_relation_type")
	} else if _, ok := RELATION_TYPE_MAP[e.relationType]; !ok {
		return apperror.New("unknown_relation_type", fmt.Sprintf("Unknown relation type %v", e.relationType))
	}
	return nil
}

func (s *joinStmt) GetIdentifiers() []string {
	ids := s.selectStmt.GetIdentifiers()
	ids = append(ids, s.joinCondition.GetIdentifiers()...)
	return ids
}

func JoinStmt(relationName, joinType string, joinCondition Expression) JoinStatement {
	return &joinStmt{
		relationName:  relationName,
		joinType:      joinType,
		joinCondition: joinCondition,
	}
}

/**
 * MutationExpression.
 */

type MutationStatement interface {
	NamedExpression
	Collection() string
	SetCollection(col string)
	Values() []FieldValueExpression
	SetValues([]FieldValueExpression)
}

type mutationStmt struct {
	namedExprMixin
	collection string
	values     []FieldValueExpression
}

func (e *mutationStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	} else if len(e.values) < 1 {
		return apperror.New("no_values")
	}
	return nil
}

func (e mutationStmt) Collection() string {
	return e.collection
}

func (e *mutationStmt) SetCollection(col string) {
	e.collection = col
}

func (e mutationStmt) Values() []FieldValueExpression {
	return e.values
}

func (e *mutationStmt) SetValues(vals []FieldValueExpression) {
	e.values = vals
}

func (s mutationStmt) GetIdentifiers() []string {
	ids := make([]string, 0)
	for _, val := range s.values {
		ids = append(ids, val.GetIdentifiers()...)
	}
	return ids
}

/**
 * CreateStatement.
 */

type CreateStatement interface {
	MutationStatement
}

type createStmt struct {
	mutationStmt
}

// Ensure CreateStatement implements Expression.
var _ CreateStatement = (*createStmt)(nil)

func (*createStmt) Type() string {
	return "create"
}

func CreateStmt(collection string, values []FieldValueExpression) CreateStatement {
	stmt := &createStmt{}
	stmt.collection = collection
	stmt.values = values
	return stmt
}

/**
 * UpdateStatement.
 */

type UpdateStatement interface {
	MutationStatement
	// Select is the select statement to specify which models to update.
	Select() SelectStatement
	SetSelect(stmt SelectStatement)
}

type updateStmt struct {
	mutationStmt
	// Select is the select statement to specify which models to update.
	selectStmt SelectStatement
}

// Ensure UpdateStatement implements Expression.
var _ UpdateStatement = (*updateStmt)(nil)

func (*updateStmt) Type() string {
	return "update"
}

func (s *updateStmt) Select() SelectStatement {
	return s.selectStmt
}

func (s *updateStmt) SetSelect(x SelectStatement) {
	s.selectStmt = x
}

func (e *updateStmt) Validate() apperror.Error {
	if err := e.mutationStmt.Validate(); err != nil {
		return err
	} else if e.selectStmt == nil {
		return apperror.New("empty_select")
	}
	return nil
}

func UpdateStmt(collection string, values []FieldValueExpression, selectStmt SelectStatement) UpdateStatement {
	stmt := &updateStmt{}
	stmt.collection = collection
	stmt.values = values
	stmt.selectStmt = selectStmt
	return stmt
}
