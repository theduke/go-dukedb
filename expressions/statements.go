package expressions

import (
	"fmt"

	"github.com/theduke/go-apperror"
)

/**
 * Statements.
 */

/**
 * CreateCollectionStatement.
 */

// CollectionStatement represents the definition for a collection.
type CreateCollectionStmt struct {
	// Name is the collection name.
	collection string

	ifNotExists bool

	// Fields are the collection fields.
	fields []*FieldExpr

	// Constraints are the constraints applied to the table, like
	// UniqueFieldsConstraint, CheckConstraint, ...
	constraints []Expression
}

func (s *CreateCollectionStmt) Collection() string {
	return s.collection
}

func (s *CreateCollectionStmt) IfNotExists() bool {
	return s.ifNotExists
}

func (s *CreateCollectionStmt) Fields() []*FieldExpr {
	return s.fields
}

func (s *CreateCollectionStmt) Constraints() []Expression {
	return s.constraints
}

func (e *CreateCollectionStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	}
	return nil
}

func NewCreateColStmt(collection string, ifNotExists bool, fields []*FieldExpr, constraints []Expression) *CreateCollectionStmt {
	return &CreateCollectionStmt{
		collection:  collection,
		ifNotExists: ifNotExists,
		fields:      fields,
		constraints: constraints,
	}
}

/**
 * RenameCollectionStatement.
 */

type RenameCollectionStmt struct {
	// Collection is the current name of the collection.
	collection string
	newName    string
}

func (s *RenameCollectionStmt) Collection() string {
	return s.collection
}

func (s *RenameCollectionStmt) NewName() string {
	return s.newName
}

func (e *RenameCollectionStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	} else if e.newName == "" {
		return apperror.New("empty_new_collection_name")
	}
	return nil
}

func NewRenameColStmt(collection, newName string) *RenameCollectionStmt {
	return &RenameCollectionStmt{
		collection: collection,
		newName:    newName,
	}
}

/**
 * DropCollectionStatement.
 */

// DropCollectionStatement is an expression for dropping a collection.
type DropCollectionStmt struct {
	// Name is the collection name.
	collection string

	ifExists bool
	cascade  bool
}

func (s *DropCollectionStmt) Collection() string {
	return s.collection
}

func (s *DropCollectionStmt) IfExists() bool {
	return s.ifExists
}

func (s *DropCollectionStmt) Cascade() bool {
	return s.cascade
}

func (e *DropCollectionStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	}
	return nil
}

func NewDropColStmt(collection string, ifExists, cascade bool) *DropCollectionStmt {
	return &DropCollectionStmt{
		collection: collection,
		ifExists:   ifExists,
		cascade:    cascade,
	}
}

/**
 * CreateFieldStatement.
 */

// AddCollectionFieldStatement is an expression to add a field to a collection.
type CreateFieldStmt struct {
	collection string
	field      *FieldExpr
}

func (s *CreateFieldStmt) Collection() string {
	return s.collection
}

func (s *CreateFieldStmt) Field() *FieldExpr {
	return s.field
}

func (e *CreateFieldStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	} else if e.Field == nil {
		return apperror.New("empty_field")
	}
	return nil
}

func NewCreateFieldStmt(collection string, field *FieldExpr) *CreateFieldStmt {
	return &CreateFieldStmt{
		collection: collection,
		field:      field,
	}
}

/**
 * RenameFieldStatement.
 */

type RenameFieldStmt struct {
	collection string
	field      string
	newName    string
}

func (s *RenameFieldStmt) Collection() string {
	return s.collection
}

func (s *RenameFieldStmt) Field() string {
	return s.field
}

func (s *RenameFieldStmt) NewName() string {
	return s.newName
}

func (e *RenameFieldStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	} else if e.field == "" {
		return apperror.New("empty_field")
	} else if e.newName == "" {
		return apperror.New("empty_new_field_name")
	}
	return nil
}

func NewRenameFieldStmt(collection, field, newName string) *RenameFieldStmt {
	return &RenameFieldStmt{
		collection: collection,
		field:      field,
		newName:    newName,
	}
}

/**
 * DropFieldStatement.
 */

type DropFieldStmt struct {
	collection string
	field      string
	ifExists   bool
	cascade    bool
}

func (s *DropFieldStmt) Collection() string {
	return s.collection
}

func (s *DropFieldStmt) Field() string {
	return s.field
}

func (s *DropFieldStmt) IfExists() bool {
	return s.ifExists
}

func (s *DropFieldStmt) Cascade() bool {
	return s.cascade
}

func (e *DropFieldStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	} else if e.field == "" {
		return apperror.New("empty_field")
	}
	return nil
}

func NewDropFieldStmt(collection, field string, ifExists, cascade bool) *DropFieldStmt {
	return &DropFieldStmt{
		collection: collection,
		field:      field,
		ifExists:   ifExists,
		cascade:    cascade,
	}
}

/**
 * CreateIndexStatement.
 */

type CreateIndexStmt struct {
	name            string
	indexExpression Expression
	expressions     []Expression
	unique          bool
	// Indexing method.
	method string
}

func (s *CreateIndexStmt) IndexName() string {
	return s.name
}

func (s *CreateIndexStmt) IndexExpression() Expression {
	return s.indexExpression
}

func (s *CreateIndexStmt) Expressions() []Expression {
	return s.expressions
}

func (s *CreateIndexStmt) Unique() bool {
	return s.unique
}

func (s *CreateIndexStmt) Method() string {
	return s.method
}

func (e *CreateIndexStmt) Validate() apperror.Error {
	if len(e.expressions) < 1 {
		return apperror.New("no_index_expressions")
	} else if e.name == "" {
		return apperror.New("empty_index_name")
	}
	return nil
}

func NewCreateIndexStmt(name string, indexExpr Expression, expressions []Expression, unique bool, method string) *CreateIndexStmt {
	return &CreateIndexStmt{
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

type DropIndexStmt struct {
	name     string
	ifExists bool
	cascade  bool
}

func (s *DropIndexStmt) IndexName() string {
	return s.name
}

func (s *DropIndexStmt) IfExists() bool {
	return s.ifExists
}

func (s *DropIndexStmt) Cascade() bool {
	return s.cascade
}

func (e *DropIndexStmt) Validate() apperror.Error {
	if e.name == "" {
		return apperror.New("empty_index_name")
	}
	return nil
}

func NewDropIndexStmt(name string, ifExists, cascade bool) *DropIndexStmt {
	return &DropIndexStmt{
		name:     name,
		ifExists: ifExists,
		cascade:  cascade,
	}
}

/**
 * SelectStatement.
 */

// SelectStatement represents a database select.
type SelectStmt struct {
	namedExprMixin

	collection string
	// Fields are arbitrary field expressions.
	fields []Expression
	filter Expression
	sorts  []*SortExpr

	limit  int
	offset int

	joins []*JoinStmt
}

func NewSelectStmt(collection string) *SelectStmt {
	return &SelectStmt{
		collection: collection,
	}
}

func (s *SelectStmt) Collection() string {
	return s.collection
}

func (s *SelectStmt) SetCollection(col string) {
	s.collection = col
}

/**
 * Fields.
 */

func (s *SelectStmt) Fields() []Expression {
	return s.fields
}

func (s *SelectStmt) SetFields(fields []Expression) {
	s.fields = fields
}

func (s *SelectStmt) AddField(fields ...Expression) {
	s.fields = append(s.fields, fields...)
}

/**
 * Filters.
 */

func (s *SelectStmt) Filter() Expression {
	return s.filter
}

func (s *SelectStmt) SetFilter(filter Expression) {
	s.filter = filter
}

func (s *SelectStmt) FilterAnd(filter Expression) {
	if s.filter == nil {
		s.filter = filter
	} else if andExpr, ok := s.filter.(*AndExpr); ok {
		andExpr.Add(filter)
	} else {
		s.filter = NewAndExpr(s.filter, filter)
	}
}

func (s *SelectStmt) FilterOr(filter Expression) {
	if s.filter == nil {
		s.filter = filter
	} else if orExpr, ok := s.filter.(*OrExpr); ok {
		orExpr.Add(filter)
	} else {
		s.filter = NewOrExpr(s.filter, filter)
	}
}

/**
 * Sorts.
 */

func (s *SelectStmt) Sorts() []*SortExpr {
	return s.sorts
}

func (s *SelectStmt) SetSorts(sorts []*SortExpr) {
	s.sorts = sorts
}

func (s *SelectStmt) AddSort(sort *SortExpr) {
	s.sorts = append(s.sorts, sort)
}

/**
 * Limit.
 */

func (s *SelectStmt) Limit() int {
	return s.limit
}

func (s *SelectStmt) SetLimit(limit int) {
	s.limit = limit
}

/**
 * Offset.
 */

func (s *SelectStmt) Offset() int {
	return s.offset
}

func (s *SelectStmt) SetOffset(offset int) {
	s.offset = offset
}

/**
 * Joins.
 */

func (s *SelectStmt) Joins() []*JoinStmt {
	return s.joins
}

func (s *SelectStmt) SetJoins(joins []*JoinStmt) {
	s.joins = joins
}

func (s *SelectStmt) AddJoin(join *JoinStmt) {
	s.joins = append(s.joins, join)
}

func (e *SelectStmt) Validate() apperror.Error {
	if e.collection == "" {
		return apperror.New("empty_collection")
	}
	return nil
}

func (s *SelectStmt) GetIdentifiers() []Expression {
	ids := make([]Expression, 0)
	// Fields.
	for _, f := range s.fields {
		ids = append(ids, getIdentifiers(f)...)
	}
	// Filter.
	ids = append(ids, getIdentifiers(s.filter)...)
	// Sorts.
	for _, sort := range s.sorts {
		ids = append(ids, getIdentifiers(sort)...)
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

// JoinStatement represents a database join.
type JoinStmt struct {
	SelectStmt

	// One of the JOIN_* constants.
	joinType string

	joinCondition Expression
}

func (s *JoinStmt) JoinType() string {
	return s.joinType
}

func (s *JoinStmt) SetJoinType(typ string) {
	s.joinType = typ
}

func (s *JoinStmt) JoinCondition() Expression {
	return s.joinCondition
}

func (s *JoinStmt) SetJoinCondition(e Expression) {
	s.joinCondition = e
}

func (s *JoinStmt) SelectStatement() *SelectStmt {
	return &s.SelectStmt
}

func (e *JoinStmt) Validate() apperror.Error {
	if err := e.SelectStmt.Validate(); err != nil {
		return err
	} else if e.joinType == "" {
		return apperror.New("empty_join_type")
	} else if _, ok := JOIN_MAP[e.joinType]; !ok {
		return apperror.New("unknown_join_type", fmt.Sprintf("Unknown join type %v", e.joinType))
	} else if e.joinCondition == nil {
		return apperror.New("no_join_condition_expression")
	}
	return nil
}

func (s *JoinStmt) GetIdentifiers() []Expression {
	ids := s.SelectStmt.GetIdentifiers()
	ids = append(ids, getIdentifiers(s.joinCondition)...)
	return ids
}

func NewJoinStmt(collection, joinType string, joinCondition Expression) *JoinStmt {
	s := &JoinStmt{
		joinType:      joinType,
		joinCondition: joinCondition,
	}
	s.collection = collection
	return s
}

/**
 * MutationExpression.
 */

type mutationStmt struct {
	namedExprMixin
	fieldedExprMixin
	collection string
	values     []*FieldValueExpr
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

func (e mutationStmt) Values() []*FieldValueExpr {
	return e.values
}

func (e *mutationStmt) SetValues(vals []*FieldValueExpr) {
	e.values = vals
}

func (s mutationStmt) GetIdentifiers() []Expression {
	ids := make([]Expression, 0)
	for _, val := range s.values {
		ids = append(ids, getIdentifiers(val)...)
	}
	return ids
}

/**
 * CreateStatement.
 */

type CreateStmt struct {
	mutationStmt
}

// Ensure CreateStatement implements FieldedExpression.
var _ FieldedExpression = (*CreateStmt)(nil)

func NewCreateStmt(collection string, values []*FieldValueExpr) *CreateStmt {
	stmt := &CreateStmt{}
	stmt.collection = collection
	stmt.values = values
	return stmt
}

/**
 * UpdateStatement.
 */

type UpdateStmt struct {
	mutationStmt
	// Select is the select statement to specify which models to update.
	selectStmt *SelectStmt
}

func (s *UpdateStmt) Select() *SelectStmt {
	return s.selectStmt
}

func (s *UpdateStmt) SetSelect(x *SelectStmt) {
	s.selectStmt = x
}

func (e *UpdateStmt) Validate() apperror.Error {
	if err := e.mutationStmt.Validate(); err != nil {
		return err
	} else if e.selectStmt == nil {
		return apperror.New("empty_select")
	}
	return nil
}

func NewUpdateStmt(collection string, values []*FieldValueExpr, selectStmt *SelectStmt) *UpdateStmt {
	stmt := &UpdateStmt{}
	stmt.collection = collection
	stmt.values = values
	stmt.selectStmt = selectStmt
	return stmt
}

/**
 * DeleteStmtm.
 */

type DeleteStmt struct {
	collection string
	selectStmt *SelectStmt
}

func NewDeleteStmt(collection string, selectStmt *SelectStmt) *DeleteStmt {
	return &DeleteStmt{
		collection: collection,
		selectStmt: selectStmt,
	}
}

func (d *DeleteStmt) Validate() apperror.Error {
	if d.collection == "" {
		return apperror.New("empty_collection")
	} else if d.selectStmt == nil {
		return apperror.New("empty_select_stmt")
	}
	return nil
}

func (d *DeleteStmt) GetIdentifiers() []Expression {
	return nil
}

func (d *DeleteStmt) Collection() string {
	return d.collection
}

func (d *DeleteStmt) SetCollection(collection string) {
	d.collection = collection
}

func (d *DeleteStmt) SelectStmt() *SelectStmt {
	return d.selectStmt
}

func (d *DeleteStmt) SetSelectStmt(x *SelectStmt) {
	d.selectStmt = x
}
