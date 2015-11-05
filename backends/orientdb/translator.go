package orientdb

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/theduke/go-apperror"
	"github.com/theduke/go-reflector"

	db "github.com/theduke/go-dukedb"
	. "github.com/theduke/go-dukedb/expressions"
)

const (
	PROPERTY_BOOLEAN      = "boolean"
	PROPERTY_INTEGER      = "integer"
	PROPERTY_SHORT        = "short"
	PROPERTY_LONG         = "long"
	PROPERTY_FLOAT        = "float"
	PROPERTY_DOUBLE       = "double"
	PROPERTY_DATETIME     = "datetime"
	PROPERTY_STRING       = "string"
	PROPERTY_BINARY       = "binary"
	PROPERTY_EMBEDDED     = "embedded"
	PROPERTY_EMBEDDEDLIST = "embeddedlist"
	PROPERTY_EMBEDDEDSET  = "embeddedset"
	PROPERTY_EMBEDDEDMAP  = "embeddedmap"
	PROPERTY_LINK         = "link"
	PROPERTY_LINKLIST     = "linklist"
	PROPERTY_LINKSET      = "linkset"
	PROPERTY_LINKMAP      = "linkmap"
	PROPERTY_BYTE         = "byte"
	PROPERTY_TRANSIENT    = "transient"
	PROPERTY_DATE         = "date"
	PROPERTY_CUSTOM       = "custom"
	PROPERTY_DECIMAL      = "decimal"
	PROPERTY_LINKBAG      = "linkbag"
	PROPERTY_ANY          = "any"
	PROPERTY_UNKNOWN      = "unknown"
)

type OrientTranslator struct {
	SqlTranslator

	backend   *Backend
	modelInfo db.ModelInfos
}

func NewTranslator(b *Backend) *OrientTranslator {
	t := &OrientTranslator{}
	t.SqlTranslator = NewSqlTranslator(t)
	t.backend = b
	t.modelInfo = b.ModelInfos()
	return t
}

func (t *OrientTranslator) New() ExpressionTranslator {
	return NewTranslator(t.backend)
}

func (t *OrientTranslator) PrepareExpression(expression Expression) apperror.Error {
	switch e := expression.(type) {
	}

	return nil
}

func (OrientTranslator) QuoteIdentifier(id string) string {
	return id
}

func (OrientTranslator) DetermineColumnType(attr *db.Attribute) (string, apperror.Error) {
	if attr.BackendType() != "" {
		return attr.BackendType(), nil
	}
	if attr.BackendMarshal() {
		return "string", nil
	}
	if attr.BackendEmbed() {
		return "embedded", nil
	}

	switch attr.Type().Kind() {
	case reflect.Bool:
		return "boolean", nil

	case reflect.Int8, reflect.Int16, reflect.Uint8:
		return "short", nil

	case reflect.Int32, reflect.Uint16:
		return "integer", nil

	case reflect.Int64, reflect.Int, reflect.Uint32, reflect.Uint64, reflect.Uint:
		return "long", nil

	case reflect.Float32:
		return "float", nil

	case reflect.Float64:
		return "double", nil

	case reflect.String:
		return "string", nil

	case reflect.Struct, reflect.Ptr:
		if attr.StructName() == "time.Time" {
			return "datetime", nil
		}

	case reflect.Slice:
		if attr.StructName() == "byte" {
			return "byte", nil
		}
	}
	return "", apperror.New("unsupported_column_type",
		fmt.Sprintf("Field %v has unsupported type %v (orientdb)", attr.Name(), attr.Type()))
}

func (t *OrientTranslator) Translate(expression Expression) apperror.Error {
	if validator, ok := expression.(ValidatableExpression); ok {
		if err := validator.Validate(); err != nil {
			return apperror.Wrap(err, "invalid_expression_"+reflect.TypeOf(expression).String())
		}
	}

	switch e := expression.(type) {

	case *FieldExpr:
		t.WQ(e.Name())
		t.W(" ")
		if err := t.Translate(e.FieldType()); err != nil {
			return err
		}
		return nil

	case FilterExpression:
		if err := t.Translate(e.Field()); err != nil {
			return err
		}
		t.W(" ", e.Operator(), " ")

		if e.Operator() != OPERATOR_IN {
			if err := t.Translate(e.Clause()); err != nil {
				return err
			}
		} else {
			val, ok := e.Clause().(*ValueExpr)
			if !ok {
				return apperror.New("invalid_in_filter")
			}

			r, err := reflector.Reflect(val.Value()).Slice()
			if err != nil {
				return apperror.New("invalid_in_filter_value")
			}

			if r.Len() < 1 {
				return apperror.New("invalid_in_filter_no_values")
			}

			t.W("[")

			lastIndex := r.Len() - 1
			for i, item := range r.Items() {
				t.W(t.Placeholder())
				if i < lastIndex {
					t.W(",")
				}

				t.Arg(NewValueExpr(item.Interface(), item.Type()))
			}
			t.W("]")
		}
		return nil

	case *CreateCollectionStmt:
		t.W("CREATE CLASS ")
		t.WQ(e.Collection())
		return nil

	case *RenameCollectionStmt:
		t.W("ALTER CLASS ")
		t.WQ(e.Collection())
		t.W(" NAME ")
		t.WQ(e.NewName())
		return nil

	case *DropFieldStmt:
		t.W("DROP PROPERTY ")
		t.WQ(e.Collection() + "." + e.Field())
		return nil

	case *DropCollectionStmt:
		t.W("DROP CLASS ")
		t.WQ(e.Collection())
		return nil

	case *CreateFieldStmt:
		t.W("CREATE PROPERTY ", e.Collection())
		if err := t.Translate(e.Field()); err != nil {
			return err
		}
		return nil

	case *RenameFieldStmt:
		t.W("ALTER PROPERTY ")
		t.WQ(e.Collection())
		t.W(".")
		t.WQ(e.Field())
		t.W(" NAME ")
		t.WQ(e.NewName())
		return nil

	case *AlterPropertyStmt:
		t.W("ALTER PROPERTY ")
		t.WQ(e.Collection())
		t.W(".")
		t.WQ(e.Field())
		t.W(" ")
		t.WQ(e.Attribute())
		t.W(" ")
		t.W(t.Placeholder())
		t.Arg(e.Value())

	case *CreateIndexStmt:
		t.W("CREATE INDEX ")
		t.WQ(e.IndexName())
		t.W(" ON ")
		if err := t.Translate(e.IndexExpression()); err != nil {
			return err
		}

		t.W(" (")
		lastIndex := len(e.Expressions()) - 1
		for index, expr := range e.Expressions() {
			if err := t.Translate(expr); err != nil {
				return err
			}
			if index < lastIndex {
				t.W(", ")
			}
		}
		t.W(") ")

		if e.Unique() {
			t.W("UNIQUE")
		} else {
			t.W("NOTUNIQUE")
		}
		return nil

	case *DropIndexStmt:
		t.W("DROP INDEX ")
		t.WQ(e.IndexName())
		return nil

	case *SelectStmt:
		// If counter is bigger than 0, this is a subquery and needs to be
		// wrapped in parantheses.
		isSubQuery := t.TranslationCounter > 0
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
			if i < lastIndex {
				t.W(", ")
			}
		}

		// Join fields.
		for _, join := range e.Joins() {
			if len(join.Fields()) < 1 {
				continue
			}

			t.W(", ")
			lastIndex := len(join.Fields()) - 1
			for i, field := range join.Fields() {
				if err := t.Translate(field); err != nil {
					return err
				}
				if i < lastIndex {
					t.W(", ")
				}
			}
		}

		t.W(" FROM ")
		t.WQ(e.Collection())

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

		if e.Offset() > 0 {
			t.W(" SKIP ", strconv.Itoa(e.Offset()))
		}
		if e.Limit() > 0 {
			t.W(" LIMIT ", strconv.Itoa(e.Limit()))
		}

		// Build up a fetch plan.
		fetchPlan := ""
		// Join clauses.
		lastIndex = len(e.Joins()) - 1
		for index, join := range e.Joins() {
			fetchPlan += join.Name() + ":1"
			if index < lastIndex {
				fetchPlan += " "
			}
		}
		if fetchPlan != "" {
			t.W(" FETCHPLAN ", fetchPlan)
		}

		// If counter is bigger than 0, this is a subquery and needs to be
		// wrapped in parantheses.
		if isSubQuery {
			t.W(")")
		}
		return nil

	case *UpdateStmt:
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
				t.W(" WHERE ")
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

			if sel.Offset() > 0 {
				t.W(" SKIP ", strconv.Itoa(sel.Offset()))
			}
			if sel.Limit() > 0 {
				t.W(" LIMIT ", strconv.Itoa(sel.Limit()))
			}
		}
		return nil

	case *DeleteStmt:
		t.W("DELETE FROM ")
		t.WQ(e.Collection())

		sel := e.SelectStmt()
		if sel != nil {
			if sel.Filter() != nil {
				t.W(" WHERE ")
				if err := t.Translate(sel.Filter()); err != nil {
					return err
				}
			}

			if len(sel.Sorts()) > 0 {
				t.W(" ORDER BY ")
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

			if sel.Offset() > 0 {
				t.W(" SKIP ", strconv.Itoa(sel.Offset()))
			}
			if sel.Limit() > 0 {
				t.W(" LIMIT ", strconv.Itoa(sel.Limit()))
			}
		}
		return nil
	}

	return t.BaseTranslator.Translate(expression)
}
