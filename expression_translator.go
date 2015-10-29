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
	arguments []interface{}

	// Counter for the translations performed since last .Reset().
	translationCounter int
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
	t.arguments = append(t.arguments, val)
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
	t.translationCounter = 0
}

func (t *ExpressionTranslator) Translate(expression Expression) apperror.Error {
	panic("translator.Panic() not overwritten.")
}

func (t *ExpressionTranslator) String() string {
	return t.buffer.String()
}

func (t ExpressionTranslator) Arguments() []interface{} {
	return t.arguments
}

/**
 * Basic SQL translater.
 */

type SqlTranslator struct {
	ExpressionTranslator
}

func (t *SqlTranslator) Translate(expression Expression) apperror.Error {
	if err := expression.Validate(); err != nil {
		return apperror.Wrap(err, "invalid_"+expression.Type()+"_expression")
	}

	switch e := expression.(type) {
	case NamedNestedExpression:
		return t.Translate(e.Expression())

	case TextExpression:
		t.W(e.Text())

	case FieldTypeExpression:
		t.W(e.FieldType())

	case ValueExpression:
		t.W(t.PlaceHolder())
		t.Arg(e.Value())

	case IdentifierExpression:
		t.WQ(e.Identifier())

	case CollectionFieldIdentifierExpression:
		if e.Collection() != "" {
			t.WQ(e.Collection())
			t.W(".")
		}
		t.WQ(e.Field())

	case ConstraintExpression:
		switch e.Constraint() {
		case CONSTRAINT_NOT_NULL:
			t.W("NOT NULL")
		case CONSTRAINT_UNIQUE:
			t.W("UNIQUE")
		case CONSTRAINT_PRIMARY_KEY:
			t.W("PRIMARY KEY")
		case CONSTRAINT_AUTO_INCREMENT:
			t.W("AUTO_INCREMENT")
		}

	case ActionConstraint:
		t.W("ON ", strings.ToUpper(e.Event()), " ", ACTIONS_MAP[e.Action()])

	case UniqueFieldsConstraint:
		t.W("UNIQUE(")
		lastIndex := len(e.UniqueFields()) - 1
		for i, field := range e.UniqueFields() {
			if err := t.Translate(field); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}
		t.W(")")

	case DefaultValueConstraint:
		t.W("DEFAULT ")
		if err := t.Translate(e.DefaultValue()); err != nil {
			return err
		}

	case CheckConstraint:
		t.W("CHECK (")
		if err := t.Translate(e.Check()); err != nil {
			return err
		}
		t.W(")")

	case ReferenceConstraint:
		t.W("REFERENCES ")
		fk := e.ForeignKey()
		t.WQ(fk.Collection())
		t.W(" (")
		t.WQ(fk.Field())
		t.W(")")

	case FieldExpression:
		t.WQ(e.Name())
		t.W(" ")
		if err := t.Translate(e.FieldType()); err != nil {
			return err
		}

		constraints := e.Constraints()
		if len(constraints) > 0 {
			t.W(" ")
			lastIndex := len(constraints) - 1
			for index, constraint := range constraints {
				if err := t.Translate(constraint); err != nil {
					return err
				}
				if index < lastIndex {
					t.W(" ")
				}
			}
		}

	case FieldValueExpression:
		if err := t.Translate(e.Field()); err != nil {
			return err
		}
		t.W(" = ")
		if err := t.Translate(e.Value()); err != nil {
			return err
		}

	case FunctionExpression:
		t.W(e.Function(), "(")
		if err := t.Translate(e.Expression()); err != nil {
			return err
		}
		t.W(")")

	case *AndExpression:
		lastIndex := len(e.Expressions()) - 1
		if lastIndex > 0 {
			t.W("(")
		}
		for i, expr := range e.Expressions() {
			if err := t.Translate(expr); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(" AND ")
			}
		}
		if lastIndex > 0 {
			t.W(")")
		}

	case *OrExpression:
		lastIndex := len(e.Expressions()) - 1

		// Wrap in parantheses if more than one filter.
		if lastIndex > 0 {
			t.W("(")
		}
		for i, expr := range e.Expressions() {
			if err := t.Translate(expr); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(" OR ")
			}
		}
		if lastIndex > 0 {
			t.W(")")
		}

	case NotExpression:
		t.W("NOT ")
		if err := t.Translate(e.Not()); err != nil {
			return err
		}

	case FilterExpression:
		if err := t.Translate(e.Field()); err != nil {
			return err
		}
		t.W(" ", e.Operator(), " ")
		if err := t.Translate(e.Clause()); err != nil {
			return err
		}

	case SortExpression:
		if err := t.Translate(e.Expression()); err != nil {
			return err
		}
		if e.Ascending() {
			t.W(" ASC")
		} else {
			t.W(" DESC")
		}

	case CreateCollectionStatement:
		t.W("CREATE TABLE ")
		if e.IfNotExists() {
			t.W("IF NOT EXISTS ")
		}
		t.WQ(e.Collection())
		t.W(" (")

		// Fields.
		lastIndex := len(e.Fields()) - 1
		for i, field := range e.Fields() {
			if err := t.Translate(field); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}

		// Constraints.
		lastIndex = len(e.Constraints()) - 1
		for i, constraint := range e.Constraints() {
			t.W(", ")
			if err := t.Translate(constraint); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}

		t.W(")")

	case RenameCollectionStatement:
		t.W("ALTER TABLE ")
		t.WQ(e.Collection())
		t.W(" RENAME TO ")
		t.WQ(e.NewName())

	// Warning: this NEEDS to appear before DropCollectionStatement,
	// since otherwise the interfaces get mixed up.
	case DropFieldStatement:
		t.W("ALTER TABLE ")
		t.WQ(e.Collection())
		t.W(" DROP COLUMN ")
		if e.IfExists() {
			t.W("IF EXISTS ")
		}
		t.WQ(e.Field())
		if e.Cascade() {
			t.W(" CASCADE")
		}

	case DropCollectionStatement:
		t.W("DROP TABLE ")
		if e.IfExists() {
			t.W("IF EXISTS ")
		}
		t.WQ(e.Collection())
		if e.Cascade() {
			t.W(" CASCADE")
		}

	case CreateFieldStatement:
		t.W("ALTER TABLE ")
		t.WQ(e.Collection())
		t.W(" ADD COLUMN ")
		if err := t.Translate(e.Field()); err != nil {
			return err
		}

	case RenameFieldStatement:
		t.W("ALTER TABLE ")
		t.WQ(e.Collection())
		t.W(" RENAME COLUMN ")
		t.WQ(e.Field())
		t.W(" TO ")
		t.WQ(e.NewName())

	case CreateIndexStatement:
		t.W("CREATE ")
		if e.Unique() {
			t.W("UNIQUE ")
		}
		t.W("INDEX ")
		t.WQ(e.IndexName())
		t.W(" ON ")
		if err := t.Translate(e.IndexExpression()); err != nil {
			return err
		}
		if e.Method() != "" {
			t.W(" USING ", e.Method(), " ")
		}

		t.W("(")
		lastIndex := len(e.Expressions()) - 1
		for index, expr := range e.Expressions() {
			if err := t.Translate(expr); err != nil {
				return err
			}
			if index < lastIndex {
				t.W(", ")
			}
		}
		t.W(")")

	case DropIndexStatement:
		t.W("DROP INDEX ")
		if e.IfExists() {
			t.W("IF EXISTS ")
		}
		t.WQ(e.IndexName())
		if e.Cascade() {
			t.W(" CASCADE")
		}

	case SelectStatement:
		// If counter is bigger than 0, this is a subquery and needs to be
		// wrapped in parantheses.
		isSubQuery := t.translationCounter > 0
		if isSubQuery {
			t.W("(")
		}

		t.W("SELECT ")

		// Field expressions.
		lastIndex := len(e.Fields()) - 1
		for i, expr := range e.Fields() {
			if err := t.Translate(expr); err != nil {
				return err
			}
			if named, ok := expr.(NamedExpression); ok {
				t.W(" AS ")
				t.WQ(named.Name())
			}
			if i < lastIndex {
				t.W(", ")
			}
		}

		// Join fields.
		for _, join := range e.Joins() {
			if join.RelationType() != RELATION_TYPE_HAS_ONE {
				continue
			}
			t.W(", ")
			lastIndex := len(join.Fields()) - 1
			for i, field := range join.Fields() {
				if err := t.Translate(field); err != nil {
					return err
				}
				if named, ok := field.(NamedExpression); ok {
					t.W(" AS ")
					t.WQ(named.Name())
				}
				if i < lastIndex {
					t.W(", ")
				}
			}
		}

		t.W(" FROM ")
		t.WQ(e.Collection())

		// Join clauses.
		for _, join := range e.Joins() {
			t.W(" ")
			if err := t.Translate(join); err != nil {
				return err
			}
		}

		if e.Filter() != nil {
			t.W(" WHERE ")
			if err := t.Translate(e.Filter()); err != nil {
				return err
			}
		}

		if len(e.Sorts()) > 0 {
			t.W(" ORDER BY ")
			lastIndex := len(e.Sorts()) - 1
			for i, sort := range e.Sorts() {
				if err := t.Translate(sort); err != nil {
					return err
				}
				if i < lastIndex {
					t.W(", ")
				}
			}
		}

		if e.Limit() > 0 {
			t.W(" LIMIT ", strconv.Itoa(e.Limit()))
		}
		if e.Offset() > 0 {
			t.W(" OFFSET ", strconv.Itoa(e.Offset()))
		}

		// If counter is bigger than 0, this is a subquery and needs to be
		// wrapped in parantheses.
		if isSubQuery {
			t.W(")")
		}

	case JoinStatement:
		name := e.RelationName()
		if name == "" {
			name = e.Collection()
		}
		t.W(JOIN_MAP[e.JoinType()])
		t.WQ(name)
		t.W(" ON ")
		if err := t.Translate(e.JoinCondition()); err != nil {
			return err
		}

	case UpdateStatement:
		t.W("UPDATE ")
		t.WQ(e.Collection())
		t.W(" SET ")

		lastIndex := len(e.Values()) - 1
		for i, field := range e.Values() {
			if err := t.Translate(field); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}

		sel := e.Select()
		if sel != nil {
			if sel.Filter() != nil {
				t.W("WHERE ")
				if err := t.Translate(sel.Filter()); err != nil {
					return err
				}
				t.W(" ")
			}

			if len(sel.Sorts()) > 0 {
				t.W("ORDER BY ")
				lastIndex := len(sel.Sorts()) - 1
				for i, sort := range sel.Sorts() {
					if err := t.Translate(sort); err != nil {
						return nil
					}
					if i < lastIndex {
						t.W(", ")
					}
				}
			}

			if sel.Limit() > 0 {
				t.W(" LIMIT ", strconv.Itoa(sel.Limit()))
			}
			if sel.Offset() > 0 {
				t.W(" OFFSET ", strconv.Itoa(sel.Offset()))
			}
		}

	case CreateStatement:
		t.W("INSERT INTO ")
		t.WQ(e.Collection())
		t.W("(")
		lastIndex := len(e.Values()) - 1
		for i, field := range e.Values() {
			if err := t.Translate(field); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}
		t.W(") VALUES(")
		for i, field := range e.Values() {
			if err := t.Translate(field); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(",")
			}
		}
		t.W(")")

	default:
		panic(fmt.Sprintf("Unhandled statement type: %v", reflect.TypeOf(expression)))
	}

	t.translationCounter += 1
	return nil
}
