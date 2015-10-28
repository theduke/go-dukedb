package dukedb

import (
	"bytes"
	"fmt"
	"reflect"
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

// W writes a string to the buffer.
func (t *ExpressionTranslator) W(str string) {
	if _, err := t.buffer.WriteString(str); err != nil {
		panic(fmt.Sprintf("Could not write to buffer: %v", err))
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
	switch e := expression.(type) {
	case *CreateCollectionStatement:
		if e.Collection == "" {
			return apperror.New("invalid_create_collection_statement", "Create collection statement with empty collection name.")
		}
		t.Reset()

		t.W("CREATE TABLE ")
		if e.IfNotExists {
			t.W("IF NOT EXISTS ")
		}
		t.WQ(e.Collection)
		t.WQ("(")

		lastIndex := len(e.Fields) - 1
		for i, field := range e.Fields {
			t.Translate(field)
			if i < lastIndex {
				t.W(", ")
			}
		}

		t.W(")")

	case *RenameCollectionStatement:
		if e.Collection == "" || e.NewCollection == "" {
			return apperror.New("invalid_rename_collection_statement", "Rename collection statement with empty Collection or NewCollection.")
		}
		t.Reset()

		t.W("ALTER TABLE ")
		t.WQ(e.Collection)
		t.W(" RENAME TO ")
		t.WQ(e.NewCollection)

	case *DropCollectionStatement:
		if e.Collection == "" {
			return apperror.New("invalid_drop_collection_statement", "Drop collection statement with empty collection name.")
		}
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
		if e.Collection == "" || e.Field == nil {
			return apperror.New("invalid_add_collection_field_statement", "AddCollectionFieldStatement with empty collection or field.")
		}
		t.Reset()

		t.W("ALTER TABLE ")
		t.WQ(e.Collection)
		t.WQ(" ADD COLUMN ")
		t.Translate(e.Field)

	case *RenameCollectionFieldStatement:
		if e.Collection == "" || e.Field == "" || e.NewName == "" {
			return apperror.New("invalid_rename_collection_field_statement", "RenameCollectionFieldStatement with empty collection, field or NewName.")
		}
		t.Reset()

		t.W("ALTER TABLE ")
		t.WQ(e.Collection)
		t.W(" RENAME COLUMN ")
		t.WQ(e.Field)
		t.W(" TO ")
		t.WQ(e.NewName)

	case *DropCollectionFieldStatement:

	case *AddIndexStatement:
	case *DropIndexStatement:
	case *SelectStatement:
	case *JoinStatement:
	case *MutationStmt:
	case *CreateStatement:
	case *UpdateStatement:
	case *FieldTypeExpression:
	case *ValueExpression:
	case *IdentifierExpression:
	case *CollectionFieldIdentifierExpression:
	case *NotNullConstraint:
	case *UniqueConstraint:
	case *UniqueFieldsConstraint:
	case *PrimaryKeyConstraint:
	case *AutoIncrementConstraint:
	case *DefaultValueConstraint:
	case *FieldUpdateConstraint:
	case *FieldDeleteConstraint:
	case *IndexConstraint:
	case *CheckConstraint:
	case *ReferenceConstraint:
	case *FieldExpression:
	case *FieldValueExpression:
	case *FunctionExpression:
	case *AndExpression:
	case *OrExpression:
	case *NotExpression:
	case *Filter:
	case *FieldFilter:
	case *FieldValueFilter:
	case *SortExpression:

	default:
		panic(fmt.Sprintf("Unhandled statement type: %v", reflect.TypeOf(expression)))
	}

	return nil
}

func (t *ExpressionTranslator) String() string {
	return t.buffer.String()
}
