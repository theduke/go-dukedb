package expressions

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/theduke/go-apperror"
)

type ExpressionTranslator interface {
	QuoteIdentifier(id string) string
	QuoteValue(val interface{}) string
	Placeholder() string
	Arg(val *ValueExpr)
	W(strs ...string)
	WQ(str string)
	Reset()

	PrepareExpression(expression Expression) apperror.Error
	Translate(expression Expression) apperror.Error

	String() string
	Arguments() []*ValueExpr
	RawArguments() []interface{}
}

// StatementTranslator takes a Statement or Expression and converts them to a string.
type BaseTranslator struct {
	buffer    bytes.Buffer
	arguments []*ValueExpr

	// Counter for the translations performed since last .Reset().
	translationCounter int

	translator ExpressionTranslator
}

func NewBaseTranslator(translator ExpressionTranslator) BaseTranslator {
	return BaseTranslator{
		translator: translator,
	}
}

func (BaseTranslator) PrepareExpression(expression Expression) apperror.Error {
	return nil
}

func (BaseTranslator) New() ExpressionTranslator {
	return &BaseTranslator{}
}

func (BaseTranslator) QuoteIdentifier(id string) string {
	return "\"" + strings.Replace(id, "\"", "\\\"", -1) + "\""
}

func (BaseTranslator) QuoteValue(val interface{}) string {
	return "'" + fmt.Sprintf("%v", val) + "'"
}

func (BaseTranslator) Placeholder() string {
	return "?"
}

// Arg adds an argument to the arguments list.
func (t *BaseTranslator) Arg(val *ValueExpr) {
	t.arguments = append(t.arguments, val)
}

// W writes a string to the buffer.
func (t *BaseTranslator) W(strs ...string) {
	for _, str := range strs {
		if _, err := t.buffer.WriteString(str); err != nil {
			panic(fmt.Sprintf("Could not write to buffer: %v", err))
		}
	}
}

// WQ quotes an identifier and writes it to the buffer.
func (t *BaseTranslator) WQ(str string) {
	t.W(t.QuoteIdentifier(str))
}

// Reset the buffer.
func (t *BaseTranslator) Reset() {
	t.buffer.Reset()
	t.translationCounter = 0
}

func (t *BaseTranslator) Translate(expression Expression) apperror.Error {
	panic("translator.Panic() not overwritten.")
}

func (t *BaseTranslator) String() string {
	return t.buffer.String()
}

func (t BaseTranslator) Arguments() []*ValueExpr {
	return t.arguments
}

func (t BaseTranslator) RawArguments() []interface{} {
	raw := make([]interface{}, 0)
	for _, val := range t.arguments {
		raw = append(raw, val.Value())
	}
	return raw
}

/**
 * Basic SQL translater.
 */

type SqlTranslator struct {
	BaseTranslator
}

// Ensure SqlTranslator implements ExpressionTranslater.
var _ ExpressionTranslator = (*SqlTranslator)(nil)

func (SqlTranslator) New() ExpressionTranslator {
	return &SqlTranslator{}
}

func (t *SqlTranslator) PrepareExpression(expression Expression) apperror.Error {
	switch e := expression.(type) {
	case *SelectStmt:
		if len(e.Fields()) < 1 {
			e.AddField(NewTextExpr("*"))
		}
	}

	return nil
}

func (t *SqlTranslator) Translate(expression Expression) apperror.Error {
	if validator, ok := expression.(ValidatableExpression); ok {
		if err := validator.Validate(); err != nil {
			return apperror.Wrap(err, "invalid_expression_"+reflect.TypeOf(expression).String())
		}
	}

	switch e := expression.(type) {
	case *NamedNestedExpr:
		return t.translator.Translate(e.Expression())

	case *TextExpr:
		t.W(e.Text())

	case *FieldTypeExpr:
		t.W(strings.TrimSpace(e.FieldType()))

	case *ValueExpr:
		t.W(t.translator.Placeholder())
		t.Arg(e)

	case *IdentifierExpr:
		t.WQ(e.Identifier())

	case *ColFieldIdentifierExpr:
		if e.Collection() != "" {
			t.WQ(e.Collection())
			t.W(".")
		}
		t.WQ(e.Field())

	case *FieldSelectorExpr:
		if err := t.translator.Translate(e.expression); err != nil {
			return err
		}
		t.W(" AS ")
		t.WQ(e.name)

	case *ConstraintExpr:
		switch e.Constraint() {
		case CONSTRAINT_NOT_NULL:
			t.W("NOT NULL")
		case CONSTRAINT_UNIQUE:
			t.W("UNIQUE")
		case CONSTRAINT_PRIMARY_KEY:
			t.W("PRIMARY KEY")
		case CONSTRAINT_AUTO_INCREMENT:
			t.W("AUTO_INCREMENT")
		default:
			return apperror.New("unknown_constraint", "Unknown constraint: "+e.Constraint())
		}

	case *ActionConstraint:
		t.W("ON ", strings.ToUpper(e.Event()), " ", ACTIONS_MAP[e.Action()])

	case *UniqueFieldsConstraint:
		t.W("UNIQUE(")
		lastIndex := len(e.UniqueFields()) - 1
		for i, field := range e.UniqueFields() {
			if err := t.translator.Translate(field); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}
		t.W(")")

	case *DefaultValueConstraint:
		t.W("DEFAULT ")
		if err := t.translator.Translate(e.DefaultValue()); err != nil {
			return err
		}

	case *CheckConstraint:
		t.W("CHECK (")
		if err := t.translator.Translate(e.Check()); err != nil {
			return err
		}
		t.W(")")

	case *ReferenceConstraint:
		t.W("REFERENCES ")
		fk := e.ForeignKey()
		t.WQ(fk.Collection())
		t.W(" (")
		t.WQ(fk.Field())
		t.W(")")

	case *FieldExpr:
		t.WQ(e.Name())
		t.W(" ")
		if err := t.translator.Translate(e.FieldType()); err != nil {
			return err
		}

		constraints := e.Constraints()
		if len(constraints) > 0 {
			for _, constraint := range constraints {
				t.W(" ")
				if err := t.translator.Translate(constraint); err != nil {
					return err
				}
			}
		}

	case *FieldValueExpr:
		if err := t.translator.Translate(e.Field()); err != nil {
			return err
		}
		t.W(" = ")
		if err := t.translator.Translate(e.Value()); err != nil {
			return err
		}

	case *FunctionExpr:
		t.W(e.Function(), "(")
		if err := t.translator.Translate(e.Expression()); err != nil {
			return err
		}
		t.W(")")

	case *AndExpr:
		lastIndex := len(e.Expressions()) - 1
		if lastIndex > 0 {
			t.W("(")
		}
		for i, expr := range e.Expressions() {
			if err := t.translator.Translate(expr); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(" AND ")
			}
		}
		if lastIndex > 0 {
			t.W(")")
		}

	case *OrExpr:
		lastIndex := len(e.Expressions()) - 1

		// Wrap in parantheses if more than one filter.
		if lastIndex > 0 {
			t.W("(")
		}
		for i, expr := range e.Expressions() {
			if err := t.translator.Translate(expr); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(" OR ")
			}
		}
		if lastIndex > 0 {
			t.W(")")
		}

	case *NotExpr:
		t.W("NOT ")
		if err := t.translator.Translate(e.Not()); err != nil {
			return err
		}

	case FilterExpression:
		if err := t.translator.Translate(e.Field()); err != nil {
			return err
		}
		t.W(" ", e.Operator(), " ")
		if err := t.translator.Translate(e.Clause()); err != nil {
			return err
		}

	case *SortExpr:
		if err := t.translator.Translate(e.Expression()); err != nil {
			return err
		}
		if e.Ascending() {
			t.W(" ASC")
		} else {
			t.W(" DESC")
		}

	case *CreateCollectionStmt:
		t.W("CREATE TABLE ")
		if e.IfNotExists() {
			t.W("IF NOT EXISTS ")
		}
		t.WQ(e.Collection())
		t.W(" (")

		// Fields.
		lastIndex := len(e.Fields()) - 1
		for i, field := range e.Fields() {
			if err := t.translator.Translate(field); err != nil {
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
			if err := t.translator.Translate(constraint); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}

		t.W(")")

	case *RenameCollectionStmt:
		t.W("ALTER TABLE ")
		t.WQ(e.Collection())
		t.W(" RENAME TO ")
		t.WQ(e.NewName())

	case *DropFieldStmt:
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

	case *DropCollectionStmt:
		t.W("DROP TABLE ")
		if e.IfExists() {
			t.W("IF EXISTS ")
		}
		t.WQ(e.Collection())
		if e.Cascade() {
			t.W(" CASCADE")
		}

	case *CreateFieldStmt:
		t.W("ALTER TABLE ")
		t.WQ(e.Collection())
		t.W(" ADD COLUMN ")
		if err := t.translator.Translate(e.Field()); err != nil {
			return err
		}

	case *RenameFieldStmt:
		t.W("ALTER TABLE ")
		t.WQ(e.Collection())
		t.W(" RENAME COLUMN ")
		t.WQ(e.Field())
		t.W(" TO ")
		t.WQ(e.NewName())

	case *CreateIndexStmt:
		t.W("CREATE ")
		if e.Unique() {
			t.W("UNIQUE ")
		}
		t.W("INDEX ")
		t.WQ(e.IndexName())
		t.W(" ON ")
		if err := t.translator.Translate(e.IndexExpression()); err != nil {
			return err
		}
		if e.Method() != "" {
			t.W(" USING ", e.Method(), " ")
		}

		t.W("(")
		lastIndex := len(e.Expressions()) - 1
		for index, expr := range e.Expressions() {
			if err := t.translator.Translate(expr); err != nil {
				return err
			}
			if index < lastIndex {
				t.W(", ")
			}
		}
		t.W(")")

	case *DropIndexStmt:
		t.W("DROP INDEX ")
		if e.IfExists() {
			t.W("IF EXISTS ")
		}
		t.WQ(e.IndexName())
		if e.Cascade() {
			t.W(" CASCADE")
		}

	case *SelectStmt:
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
			if err := t.translator.Translate(expr); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}

		// Join fields.
		for _, join := range e.Joins() {
			t.W(", ")
			lastIndex := len(join.Fields()) - 1
			for i, field := range join.Fields() {
				if err := t.translator.Translate(field); err != nil {
					return err
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
			if err := t.translator.Translate(join); err != nil {
				return err
			}
		}

		if e.Filter() != nil {
			t.W(" WHERE ")
			if err := t.translator.Translate(e.Filter()); err != nil {
				return err
			}
		}

		if len(e.Sorts()) > 0 {
			t.W(" ORDER BY ")
			lastIndex := len(e.Sorts()) - 1
			for i, sort := range e.Sorts() {
				if err := t.translator.Translate(sort); err != nil {
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

	case *JoinStmt:
		name := e.Name()
		if name == "" {
			name = e.Collection()
		}
		t.W(JOIN_MAP[e.JoinType()])
		t.WQ(name)
		t.W(" ON ")
		if err := t.translator.Translate(e.JoinCondition()); err != nil {
			return err
		}

	case *CreateStmt:
		t.W("INSERT INTO ")
		t.WQ(e.Collection())
		t.W("(")
		lastIndex := len(e.Values()) - 1
		for i, field := range e.Values() {
			if err := t.translator.Translate(field.Field()); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(", ")
			}
		}
		t.W(") VALUES(")
		for i, field := range e.Values() {
			if err := t.translator.Translate(field.Value()); err != nil {
				return err
			}
			if i < lastIndex {
				t.W(",")
			}
		}
		t.W(")")

	case *UpdateStmt:
		t.W("UPDATE ")
		t.WQ(e.Collection())
		t.W(" SET ")

		lastIndex := len(e.Values()) - 1
		for i, field := range e.Values() {
			if err := t.translator.Translate(field); err != nil {
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
				if err := t.translator.Translate(sel.Filter()); err != nil {
					return err
				}
				t.W(" ")
			}

			if len(sel.Sorts()) > 0 {
				t.W("ORDER BY ")
				lastIndex := len(sel.Sorts()) - 1
				for i, sort := range sel.Sorts() {
					if err := t.translator.Translate(sort); err != nil {
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

	default:
		panic(fmt.Sprintf("Unhandled statement type: %v", reflect.TypeOf(expression)))
	}

	t.translationCounter += 1
	return nil
}

func NewSqlTranslator(translator ExpressionTranslator) SqlTranslator {
	t := SqlTranslator{
		BaseTranslator: NewBaseTranslator(translator),
	}
	return t
}
