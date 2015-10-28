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
 * AddIndexStatement
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

// CollectionStatement represents the definition for a collection.
type CreateCollectionStatement struct {
	// Name is the collection name.
	Collection string

	IfNotExists bool

	// Fields are the collection fields.
	Fields []*FieldExpression

	// Constraints are the constraints applied to the table, like
	// UniqueFieldsConstraint, CheckConstraint, ...
	Constraints []*ConstraintExpression
}

// Ensure CreateCollectionStatement implements Expression.
var _ Expression = (*CreateCollectionStatement)(nil)

func (*CreateCollectionStatement) Type() string {
	return "create_collection"
}

func (e *CreateCollectionStatement) Validate() apperror.Error {
	if e.Collection == "" {
		return apperror.New("empty_collection")
	}
	return nil
}

func (e *CreateCollectionStatement) IsCacheable() bool {
	return false
}

func (CreateCollectionStatement) GetIdentifiers() []string {
	return nil
}

/**
 * RenameCollectionStatement.
 */

type RenameCollectionStatement struct {
	// Collection is the current name of the collection.
	Collection    string
	NewCollection string
}

// Ensure RenameCollectionStatement implements Expression.
var _ Expression = (*RenameCollectionStatement)(nil)

func (*RenameCollectionStatement) Type() string {
	return "rename_collection"
}

func (e *RenameCollectionStatement) Validate() apperror.Error {
	if e.Collection == "" {
		return apperror.New("empty_collection")
	} else if e.NewCollection == "" {
		return apperror.New("empty_new_collection_name")
	}
	return nil
}

func (e *RenameCollectionStatement) IsCacheable() bool {
	return false
}

func (RenameCollectionStatement) GetIdentifiers() []string {
	return nil
}

/**
 * DropCollectionStatement.
 */

// DropCollectionStatement is an expression for dropping a collection.
type DropCollectionStatement struct {
	// Name is the collection name.
	Collection string

	IfExists bool
	Cascade  bool
}

// Ensure DropCollectionStatement implements Expression.
var _ Expression = (*DropCollectionStatement)(nil)

func (*DropCollectionStatement) Type() string {
	return "drop_collection"
}

func (e *DropCollectionStatement) Validate() apperror.Error {
	if e.Collection == "" {
		return apperror.New("empty_collection")
	}
	return nil
}

func (e *DropCollectionStatement) IsCacheable() bool {
	return false
}

func (DropCollectionStatement) GetIdentifiers() []string {
	return nil
}

/**
 * AddCollectionFieldStatement.
 */

// AddCollectionFieldStatement is an expression to add a field to a collection.
type AddCollectionFieldStatement struct {
	Collection string
	Field      *FieldExpression
}

// Ensure AddCollectionFieldStatement implements Expression.
var _ Expression = (*AddCollectionFieldStatement)(nil)

func (*AddCollectionFieldStatement) Type() string {
	return "add_collection_field"
}

func (e *AddCollectionFieldStatement) Validate() apperror.Error {
	if e.Collection == "" {
		return apperror.New("empty_collection")
	} else if e.Field == nil {
		return apperror.New("empty_field")
	}
	return nil
}

func (e *AddCollectionFieldStatement) IsCacheable() bool {
	return false
}

func (AddCollectionFieldStatement) GetIdentifiers() []string {
	return nil
}

/**
 * RenameCollectionFieldStatement.
 */

type RenameCollectionFieldStatement struct {
	Collection string
	Field      string
	NewName    string
}

// Ensure RenameCollectionFieldStatement implements Expression.
var _ Expression = (*RenameCollectionFieldStatement)(nil)

func (*RenameCollectionFieldStatement) Type() string {
	return "add_collection_field"
}

func (e *RenameCollectionFieldStatement) Validate() apperror.Error {
	if e.Collection == "" {
		return apperror.New("empty_collection")
	} else if e.Field == "" {
		return apperror.New("empty_field")
	} else if e.NewName == "" {
		return apperror.New("empty_new_field_name")
	}
	return nil
}

func (e *RenameCollectionFieldStatement) IsCacheable() bool {
	return false
}

func (RenameCollectionFieldStatement) GetIdentifiers() []string {
	return nil
}

/**
 * DropCollectionFieldStatement.
 */

type DropCollectionFieldStatement struct {
	Collection string
	Field      string
	IfExists   bool
	Cascasde   bool
}

// Ensure DropCollectionFieldStatement implements Expression.
var _ Expression = (*DropCollectionFieldStatement)(nil)

func (*DropCollectionFieldStatement) Type() string {
	return "drop_collection_field"
}

func (e *DropCollectionFieldStatement) Validate() apperror.Error {
	if e.Collection == "" {
		return apperror.New("empty_collection")
	} else if e.Field == "" {
		return apperror.New("empty_field")
	}
	return nil
}

func (e *DropCollectionFieldStatement) IsCacheable() bool {
	return false
}

func (DropCollectionFieldStatement) GetIdentifiers() []string {
	return nil
}

/**
 * AddIndexStatement.
 */

type AddIndexStatement struct {
	Collection string
	Field      string
	IndexName  string
}

// Ensure AddIndexStatement implements Expression.
var _ Expression = (*AddIndexStatement)(nil)

func (*AddIndexStatement) Type() string {
	return "add_index"
}

func (e *AddIndexStatement) Validate() apperror.Error {
	if e.Collection == "" {
		return apperror.New("empty_collection")
	} else if e.Field == "" {
		return apperror.New("empty_field")
	} else if e.IndexName == "" {
		return apperror.New("empty_index_name")
	}
	return nil
}

func (e *AddIndexStatement) IsCacheable() bool {
	return false
}

func (AddIndexStatement) GetIdentifiers() []string {
	return nil
}

/**
 * DropIndexStatement.
 */

type DropIndexStatement struct {
	Collection string
	IndexName  string
}

// Ensure DropIndexStatement implements Expression.
var _ Expression = (*DropIndexStatement)(nil)

func (*DropIndexStatement) Type() string {
	return "drop_index"
}

func (e *DropIndexStatement) Validate() apperror.Error {
	if e.Collection == "" {
		return apperror.New("empty_collection")
	} else if e.IndexName == "" {
		return apperror.New("empty_index_name")
	}
	return nil
}

func (e *DropIndexStatement) IsCacheable() bool {
	return false
}

func (DropIndexStatement) GetIdentifiers() []string {
	return nil
}

/**
 * SelectStatement.
 */

// SelectStatement represents a database select.
type SelectStatement struct {
	NamedExpr
	Collection string
	Fields     []Expression
	Filter     Expression
	Sorts      []*SortExpression

	Limit  int
	Offset int

	Joins []*JoinStatement
}

// Ensure SelectStatement implements NamedExpression.
var _ NamedExpression = (*SelectStatement)(nil)

func (*SelectStatement) Type() string {
	return "select"
}

func (e *SelectStatement) Validate() apperror.Error {
	if e.Collection == "" {
		return apperror.New("empty_collection")
	}
	return nil
}

func (e *SelectStatement) IsCacheable() bool {
	return false
}

func (s SelectStatement) GetIdentifiers() []string {
	ids := make([]string, 0)
	// Fields.
	for _, f := range s.Fields {
		ids = append(ids, s.GetIdentifiers()...)
	}
	// Filter.
	ids = append(ids, s.Filter.GetIdentifiers()...)
	// Sorts.
	for _, sort := range s.Sorts {
		ids = append(ids, sort.GetIdentifiers()...)
	}
	// Joins.
	for _, join := range s.Joins {
		ids = append(ids, join.GetIdentifiers()...)
	}
	return ids
}

func (s *SelectStatement) AddField(fields ...Expression) {
	s.Fields = append(s.Fields, fields...)
}

func (s *SelectStatement) FilterAnd(filter Expression) {
	if s.Filter == nil {
		s.Filter = filter
	} else if andExpr, ok := s.Filter.(*AndExpression); ok {
		andExpr.Add(filter)
	} else {
		s.Filter = And(s.Filter, filter)
	}
}

func (s *SelectStatement) FilterOr(filter Expression) {
	if s.Filter == nil {
		s.Filter = filter
	} else if orExpr, ok := s.Filter.(*OrExpression); ok {
		orExpr.Add(filter)
	} else {
		s.Filter = Or(s.Filter, filter)
	}
}

func (s *SelectStatement) AddSort(sort *SortExpression) {
	s.Sorts = append(s.Sorts, sort)
}

func (s *SelectStatement) AddJoin(join *JoinStatement) {
	join.Base = s
	s.Joins = append(s.Joins, join)
}

// Retrieve a join query for the specified field.
// Returns a *RelationQuery, or nil if not found.
// Supports nested Joins like 'Parent.Tags'.
func (s *SelectStatement) GetJoin(field string) *JoinStatement {
	// Avoid extra work if no joins are set.
	if s.Joins == nil || len(s.Joins) == 0 {
		return nil
	}

	parts := strings.Split(field, ".")
	if len(parts) > 1 {
		field = parts[0]
	}

	for _, join := range s.Joins {
		if join.RelationName == field {
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

func (s *SelectStatement) FixNesting() apperror.Error {
	if err := s.FixNestedJoins(); err != nil {
		return err
	}
	s.FixNestedFields()
	return nil
}

func (s *SelectStatement) FixNestedJoins() apperror.Error {
	if len(s.Joins) < 1 {
		return nil
	}
	return s.fixNestedJoinsRecursive(2, 1)
}

func (s *SelectStatement) fixNestedJoinsRecursive(lvl, maxLvl int) apperror.Error {
	remainingJoins := make([]*JoinStatement, 0)

	for index, join := range s.Joins {
		if join.RelationName == "" {
			// No RelationName set, so ignore this custom join.
			remainingJoins = append(remainingJoins, join)
			continue
		}

		parts := strings.Split(join.RelationName, ".")
		joinLvl := len(parts)
		if joinLvl > maxLvl {
			maxLvl = joinLvl
		}

		if joinLvl != lvl {
			// Join is not on the level currently processed, so skip.
			remainingJoins = append(remainingJoins, join)
			continue
		}

		parentJoin := s.GetJoin(strings.Join(parts[0:joinLvl-2], "."))
		if parentJoin == nil {
			return &apperror.Err{
				Public:  true,
				Code:    "invalid_join",
				Message: fmt.Sprintf("Invalid nested join '%v': parent join %v not found", join.RelationName, parts[0]),
			}
		}
		parentJoin.AddJoin(join)
	}

	s.Joins = remainingJoins

	if lvl < maxLvl {
		return s.fixNestedJoinsRecursive(lvl+1, maxLvl)
	}
	return nil
}

func (s *SelectStatement) FixNestedFields() {
	if len(s.Fields) < 1 {
		return
	}

	remainingFields := make([]Expression, 0)

	for _, fieldExpr := range s.Fields {
		field, ok := fieldExpr.(*IdentifierExpression)
		if !ok {
			remainingFields = append(remainingFields, field)
			continue
		}

		parts := strings.Split(field.Identifier, ".")
		if len(parts) < 2 {
			remainingFields = append(remainingFields, field)
			continue
		}

		// Nested field, so try to find parent join.
		joinName := strings.Join(parts[0:len(parts)-2], ".")
		join := s.GetJoin(joinName)
		if join == nil {
			/*
				return &apperror.Error{
					Public: true,
					Code: "invalid_nested_field",
					Message: fmt.Printf("Invalid nested field '%v': the parent join %v does not exist", field.Identifier, joinName),
				}
			*/

			// Maybe the backend supports nested fields, so leave the field untouched.
			remainingFields = append(remainingFields, field)
		} else {
			// Found parent join, so add the field to it.
			join.AddField(field)
		}
	}

	s.Fields = remainingFields
}

const (
	JOIN_INNER = "inner"
	JOIN_LEFT  = "left"
	JOIN_RIGHT = "right"
	JOIN_CROSS = "cross"
)

/**
 * JoinStatement.
 */

// JoinStatement represents a database join.
type JoinStatement struct {
	SelectStatement

	Base *SelectStatement

	RelationName string

	// One of the JOIN_* constants.
	JoinType string

	JoinCondition Expression
}

// Ensure JoinStatement implements Expression.
var _ Expression = (*JoinStatement)(nil)

func (*JoinStatement) Type() string {
	return "join"
}

func (e *JoinStatement) Validate() apperror.Error {
	if err := e.SelectStatement.Validate(); err != nil {
		return err
	} else if e.JoinType == "" {
		return apperror.New("empty_join_type")
	} else if e.JoinCondition == nil {
		return apperror.New("no_join_condition_expression")
	}
	return nil
}

func (e *JoinStatement) IsCacheable() bool {
	return false
}

func (s JoinStatement) GetIdentifiers() []string {
	ids := s.SelectStatement.GetIdentifiers()
	ids = append(ids, s.JoinCondition.GetIdentifiers()...)
	return ids
}

func Join(relationName, joinType string, joinCondition Expression) *JoinStatement {
	return &JoinStatement{
		RelationName:  relationName,
		JoinType:      joinType,
		JoinCondition: joinCondition,
	}
}

/**
 * MutationExpression.
 */

type MutationStatement interface {
	NamedExpression
	GetCollection() string
	SetCollection(col string)
	GetValues() []*FieldValueExpression
	SetValues([]*FieldValueExpression)
}

type MutationStmt struct {
	NamedExpr
	Collection string
	Values     []*FieldValueExpression
}

func (MutationStmt) Type() string {
	return "mutation"
}

func (e *MutationStmt) Validate() apperror.Error {
	if e.Collection == "" {
		return apperror.New("empty_collection")
	} else if len(e.Values) < 1 {
		return apperror.New("no_values")
	}
	return nil
}

func (e *MutationStmt) IsCacheable() bool {
	return false
}

func (e MutationStmt) GetCollection() string {
	return e.Collection
}

func (e *MutationStmt) SetCollection(col string) {
	e.Collection = col
}

func (e MutationStmt) GetValues() []*FieldValueExpression {
	return e.Values
}

func (e *MutationStmt) SetValues(vals []*FieldValueExpression) {
	e.Values = vals
}

func (s MutationStmt) GetIdentifiers() []string {
	ids := make([]string, 0)
	for _, val := range s.Values {
		ids = append(ids, val.GetIdentifiers()...)
	}
	return ids
}

/**
 * CreateStatement.
 */

type CreateStatement struct {
	MutationStmt
}

// Ensure CreateStatement implements Expression.
var _ MutationStatement = (*CreateStatement)(nil)

func (*CreateStatement) Type() string {
	return "create"
}

/**
 * UpdateStatement.
 */

type UpdateStatement struct {
	MutationStmt
	// Select is the select statement to specify which models to update.
	Select *SelectStatement
}

// Ensure UpdateStatement implements Expression.
var _ MutationStatement = (*UpdateStatement)(nil)

func (*UpdateStatement) Type() string {
	return "update"
}

func (e *UpdateStatement) Validate() apperror.Error {
	if err := e.MutationStmt.Validate(); err != nil {
		return err
	} else if e.Select == nil {
		return apperror.New("empty_select")
	}
	return nil
}

func (e *UpdateStatement) IsCacheable() bool {
	return false
}
