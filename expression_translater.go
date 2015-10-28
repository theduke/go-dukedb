package dukedb

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/theduke/go-apperror"
)

// StatementTranslator takes a Statement or Expression and converts them to a string.
type ExpressionTranslator struct {
	buffer    bytes.Buffer
	Arguments []interface{}
}

func (ExpressionTranslator) QuoteIdentifier(id string) string {
	return "\"" + strings.Replace(id, "\"", "\\\"", -1) + "\""
}

func (ExpressionTranslator) QuoteValue(val interface{}) string {
	return "'" + fmt.Sprintf("%v", val) + "'"
}

func (ExpressionTranslator) PlaceHolder() string {
	return "?"
}

// Arg adds an argument to the arguments list.
func (t *ExpressionTranslator) Arg(val interface{}) {
	t.Arguments = append(t.Arguments, val)
}

// W writes a string to the buffer.
func (t *ExpressionTranslator) W(strs ...string) {
	for _, str := range strs {
		if _, err := t.buffer.WriteString(str); err != nil {
			panic(fmt.Sprintf("Could not write to buffer: %v", err))
		}
	}
}

// WQ quotes an identifier and writes it to the buffer.
func (t *ExpressionTranslator) WQ(str string) {
	t.W(t.QuoteIdentifier(str))
}

// Reset the buffer.
func (t *ExpressionTranslator) Reset() {
	t.buffer.Reset()
}

func (t *ExpressionTranslator) Translate(expression Expression) apperror.Error {
	panic("translator.Panic() not overwritten.")
}

func (t *ExpressionTranslator) String() string {
	return t.buffer.String()
}

/**
 * Basic SQL translater.
 */

type SqlTranslater struct {
	ExpressionTranslator
}

func (t *SqlTranslater) Translate(expression Expression) apperror.Error {
	if err := expression.Validate(); err != nil {
		return apperror.Wrap(err, "invalid_"+expression.Type()+"_expression")
	}

	switch e := expression.(type) {
	case *CreateCollectionStatement:
		t.Reset()

		t.W("CREATE TABLE ")
		if e.IfNotExists {
			t.W("IF NOT EXISTS ")
		}
		t.WQ(e.Collection)
		t.WQ("(")

		lastIndex := len(e.Fields) - 1
		for i, field := range e.Fields {
			if err := t.Translate(field); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}

		t.W(")")

	case *RenameCollectionStatement:
		t.Reset()

		t.W("ALTER TABLE ")
		t.WQ(e.Collection)
		t.W(" RENAME TO ")
		t.WQ(e.NewCollection)

	case *DropCollectionStatement:
		t.Reset()

		t.W("DROP TABLE ")
		if e.IfExists {
			t.W("IF EXISTS ")
		}
		t.WQ(e.Collection)
		if e.Cascade {
			t.W(" CASCADE")
		}

	case *AddCollectionFieldStatement:
		t.Reset()

		t.W("ALTER TABLE ")
		t.WQ(e.Collection)
		t.WQ(" ADD COLUMN ")
		if err := t.Translate(e.Field); err != nil {
			return err
		}

	case *RenameCollectionFieldStatement:
		t.Reset()

		t.W("ALTER TABLE ")
		t.WQ(e.Collection)
		t.W(" RENAME COLUMN ")
		t.WQ(e.Field)
		t.W(" TO ")
		t.WQ(e.NewName)

	case *DropCollectionFieldStatement:
		t.Reset()

		t.W("ALTER TABLE ")
		t.WQ(e.Collection)
		t.W("DROP COLUMN ")
		if e.IfExists {
			t.W("IF EXISTS ")
		}
		t.WQ(e.Field)
		if e.Cascasde {
			t.W(" CASCADE")
		}

	case *CreateIndexStatement:
		t.Reset()

		t.W("CREATE ")
		if e.Unique {
			t.W("UNIQUE ")
		}
		t.W("INDEX ")
		t.WQ(e.IndexName)
		t.W(" ON ")
		t.WQ(e.Collection)
		if e.Method != "" {
			t.W("USING ", e.Method, " ")
		}

		t.W("(")
		lastIndex := len(e.Expressions) - 1
		for index, expr := range e.Expressions {
			if err := t.Translate(expr); err != nil {
				return err
			}
			if index < lastIndex {
				t.W(", ")
			}
		}
		t.W(")")

	case *DropIndexStatement:
		t.Reset()

		t.W("DROP INDEX ")
		if e.IfExists {
			t.W(" IF EXISTS ")
		}
		t.WQ(e.IndexName)
		if e.Cascade {
			t.W(" CASCADE")
		}

	case *SelectStatement:
		t.Reset()

		t.W("SELECT ")

		// Field expressions.
		lastIndex := len(e.Fields) - 1
		for i, expr := range e.Fields {
			if err := t.Translate(expr.GetExpression()); err != nil {
				return err
			}
			t.W(" AS ")
			t.WQ(e.Collection + "." + expr.GetName())
			if i < lastIndex {
				t.W(", ")
			}
		}

		// Join fields.
		for _, join := range e.Joins {
			if join.RelationType != RELATION_TYPE_HAS_ONE {
				continue
			}
			t.W(", ")
			lastIndex := len(join.Fields) - 1
			for i, field := range join.Fields {
				if err := t.Translate(field.GetExpression()); err != nil {
					return err
				}
				t.W(" AS ")
				name := join.RelationName
				if name == "" {
					name = join.Collection
				}
				t.WQ(name + "." + field.GetName())
				if i < lastIndex {
					t.W(", ")
				}
			}
		}

		t.W(" FROM ")
		t.WQ(e.Collection)
		t.W(" ")

		// Join clauses.
		for i, join := range e.Joins {
			if err := t.Translate(join); err != nil {
				return err
			}
			t.W(" ")
		}

		if e.Filter != nil {
			t.W("WHERE ")
			if err := t.Translate(e.Filter); err != nil {
				return err
			}
			t.W(" ")
		}

		if len(e.Sorts) > 0 {
			t.W("ORDER BY ")
			lastIndex := len(e.Sorts) - 1
			for i, sort := range e.Sorts {
				if err := t.Translate(sort); err != nil {
					return err
				}
				if i < lastIndex {
					t.W(", ")
				}
			}
		}

		if e.Limit > 0 {
			t.W(" LIMIT ", strconv.Itoa(e.Limit))
		}
		if e.Offset > 0 {
			t.W(" OFFSET ", strconv.Itoa(e.Offset))
		}

	case *JoinStatement:
		name := e.RelationName
		if name == "" {
			name = e.Collection
		}
		t.W(JOIN_SQL_MAP[e.JoinType])
		t.WQ(name)
		t.W(" ON ")
		if err := t.Translate(e.JoinCondition); err != nil {
			return err
		}

	case *CreateStatement:
		t.Reset()

		t.W("INSERT INTO ")
		t.WQ(e.Collection)
		t.W("(")
		lastIndex := len(e.Values) - 1
		for i, field := range e.Values {
			if err := t.Translate(field); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}
		t.W(") VALUES(")
		for i, field := range e.Values {
			if err := t.Translate(field); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(",")
			}
		}
		t.W(")")

	case *UpdateStatement:
		t.Reset()

		t.W("UPDATE ")
		t.WQ(e.Collection)
		t.W(" SET ")

		lastIndex := len(e.Values) - 1
		for i, field := range e.Values {
			if err := t.Translate(field); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}

		if e.Select != nil {
			if e.Select.Filter != nil {
				t.W("WHERE ")
				if err := t.Translate(e.Select.Filter); err != nil {
					return err
				}
				t.W(" ")
			}

			if len(e.Select.Sorts) > 0 {
				t.W("ORDER BY ")
				lastIndex := len(e.Select.Sorts) - 1
				for i, sort := range e.Select.Sorts {
					if err := t.Translate(sort); err != nil {
						return nil
					}
					if i < lastIndex {
						t.W(", ")
					}
				}
			}

			if e.Select.Limit > 0 {
				t.W(" LIMIT ", strconv.Itoa(e.Select.Limit))
			}
			if e.Select.Offset > 0 {
				t.W(" OFFSET ", strconv.Itoa(e.Select.Offset))
			}
		}

	case *NamedNestedExpr:
		return t.Translate(e.Expression)

	case *TextExpression:
		t.W(e.Text)

	case *FieldTypeExpression:
		t.W(e.Typ)

	case *ValueExpression:
		t.W(t.PlaceHolder())
		t.Arg(e.Val)

	case *IdentifierExpression:
		t.WQ(e.Identifier)

	case *CollectionFieldIdentifierExpression:
		if e.Collection != "" {
			t.WQ(e.Collection)
			t.W(".")
		}
		t.WQ(e.Field)

	case *NotNullConstraint:
		t.W("NOT NULL")

	case *UniqueConstraint:
		t.W("UNIQUE")

	case *UniqueFieldsConstraint:
		t.W("UNIQUE (")
		lastIndex := len(e.Fields) - 1
		for i, field := range e.Fields {
			if err := t.Translate(field); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}
		t.W(")")

	case *PrimaryKeyConstraint:
		t.W("PRIMARY KEY")

	case *AutoIncrementConstraint:
		t.W("AUTO INCREMENT")

	case *DefaultValueConstraint:
		t.W("DEFAULT ")
		if err := t.Translate(e.Value); err != nil {
			return err
		}

	case *FieldUpdateConstraint:
		t.W("ON UPDATE ", e.Action)

	case *FieldDeleteConstraint:
		t.W("ON DELETE ", e.Action)

	case *IndexConstraint:

	case *CheckConstraint:
		t.W("CHECK (")
		if err := t.Translate(e.Check); err != nil {
			return err
		}
		t.W(")")

	case *ReferenceConstraint:
		t.W("REFERENCES ")
		t.WQ(e.ForeignKey.Collection)
		t.W(" (")
		t.WQ(e.ForeignKey.Field)
		t.W(")")

	case *FieldExpression:
		t.WQ(e.Name)
		t.W(" ")
		if err := t.Translate(e.Typ); err != nil {
			return err
		}
		if len(e.Constraints) > 0 {
			t.W(" ")
			for _, constraint := range e.Constraints {
				if err := t.Translate(constraint); err != nil {
					return err
				}
			}
		}

	case *FieldValueExpression:
		if err := t.Translate(e.Field); err != nil {
			return err
		}
		t.W("=")
		if err := t.Translate(e.Value); err != nil {
			return err
		}

	case *FunctionExpression:
		t.W(e.Function, "(")
		if err := t.Translate(e.Nested); err != nil {
			return err
		}
		t.W(")")

	case *AndExpression:
		t.W("(")
		lastIndex := len(e.Expressions) - 1
		for i, expr := range e.Expressions {
			if err := t.Translate(expr); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(" AND ")
			}
		}
		t.W(")")

	case *OrExpression:
		t.W("(")
		lastIndex := len(e.Expressions) - 1
		for i, expr := range e.Expressions {
			if err := t.Translate(expr); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(" OR ")
			}
		}
		t.W(")")

	case *NotExpression:
		t.W("NOT ")
		if err := t.Translate(e.Nested); err != nil {
			return err
		}

	case FilterExpression:
		if err := t.Translate(e.GetField()); err != nil {
			return err
		}
		t.W(" ", e.GetOperator(), " ")
		if err := t.Translate(e.GetClause()); err != nil {
			return err
		}

	case *SortExpression:
		if err := t.Translate(e.Field); err != nil {
			return err
		}
		if e.Ascending {
			t.W(" ASC")
		} else {
			t.W(" DESC")
		}

	default:
		panic(fmt.Sprintf("Unhandled statement type: %v", reflect.TypeOf(expression)))
	}

	return nil
}
