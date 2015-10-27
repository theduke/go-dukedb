package dukedb

import ()

/**
 * Statements.
 */

/**
 * CreateCollectionStatement.
 */

// CollectionExpression represents the definition for a collection.
type CreateCollectionStatement struct {
	// Name is the collection name.
	Collection string

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

/**
 * DropCollectionStatement.
 */

// DropCollectionStatement is an expression for dropping a collection.
type DropCollectionStatement struct {
	// Name is the collection name.
	Collection string
}

// Ensure DropCollectionStatement implements Expression.
var _ Expression = (*DropCollectionStatement)(nil)

func (*DropCollectionStatement) Type() string {
	return "drop_collection"
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

/**
 * DropCollectionFieldStatement.
 */

type DropCollectionFieldStatement struct {
	Collection string
	Field      string
}

// Ensure DropCollectionFieldStatement implements Expression.
var _ Expression = (*DropCollectionFieldStatement)(nil)

func (*DropCollectionFieldStatement) Type() string {
	return "drop_collection_field"
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

/**
 * SelectStatement.
 */

// SelectStatement represents a database select.
type SelectStatement struct {
	Collection string
	Fields     []Expression
	Filter     Expression
	Sorts      []*SortExpression

	Limit  int
	Offset int

	Joins []*JoinStatement
}

// Ensure SelectStatement implements Expression.
var _ Expression = (*SelectStatement)(nil)

func (*SelectStatement) Type() string {
	return "select"
}

func (s SelectStatement) GetIdentifiers() []string {
	ids := make([]string, 0)
	// Fields.
	for _, f := range s.Fields {
		ids = append(ids, s.GetIdentifiers()...)
	}
	// Filter.
	ids = append(ids, s.Filter.GetIdentifiers())
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
	} else if orExpr, ok := s.Filter(*OrExpression); ok {
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

func (s JoinStatement) GetIdentifiers() []string {
	ids := s.SelectStatement.GetIdentifiers()
	ids = append(ids, s.JoinCondition.GetIdentifiers()...)
	return ids
}

func Join(relationName, joinType string, joinCondition Expression) {
	return &JoinStatement{
		RelationName:  relationName,
		JoinType:      joinType,
		JoinCondition: joinCondition,
	}
}

/**
 * CreateStatement.
 */

type CreateStatement struct {
	Collection string
	Values     []*FieldValueExpression
}

// Ensure CreateStatement implements Expression.
var _ Expression = (*CreateStatement)(nil)

func (*CreateStatement) Type() string {
	return "create"
}

func (s CreateStatement) GetIdentifiers() []string {
	ids := make([]string, 0)
	for _, val := range s.Values {
		ids = append(ids, val.GetIdentifiers()...)
	}
	return ids
}

/**
 * UpdateStatement.
 */

type UpdateStatement struct {
	// Select is the select statement to specify which models to update.
	Select SelectStatement
	// Values holds the field values to update.
	Values []*FieldValueExpression
}

// Ensure UpdateStatement implements Expression.
var _ Expression = (*UpdateStatement)(nil)

func (*UpdateStatement) Type() string {
	return "update"
}

func (s UpdateStatement) GetIdentifiers() []string {
	ids := s.Select.GetIdentifiers()
	for _, val := range s.Values {
		ids = append(ids, val.GetIdentifiers()...)
	}
	return ids
}
