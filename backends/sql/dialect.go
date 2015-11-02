package sql

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/theduke/go-apperror"

	db "github.com/theduke/go-dukedb"
	. "github.com/theduke/go-dukedb/expressions"
)

type Dialect interface {
	ExpressionTranslator
	New() Dialect
	DetermineColumnType(attr *db.Attribute) (string, apperror.Error)

	AfterCollectionCreate(info *db.ModelInfo) apperror.Error
}

type baseDialect struct {
	SqlTranslator
	backend   *Backend
	modelInfo db.ModelInfos
}

func (baseDialect) AfterCollectionCreate(info *db.ModelInfo) apperror.Error {
	return nil
}

func (baseDialect) DetermineColumnType(attr *db.Attribute) (string, apperror.Error) {
	if attr.BackendType() != "" {
		return attr.BackendType(), nil
	}
	if attr.BackendMarshal() {
		return "text", nil
	}

	if attr.BackendEmbed() {
		return "", apperror.New("unsupported_embed", "The SQL backend does not support embedding. Use marshalling instead.")
	}

	switch attr.Type().Kind() {
	case reflect.Bool:
		return "boolean", nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uintptr:
		if attr.AutoIncrement() {
			return "serial", nil
		}
		return "integer", nil

	case reflect.Int64, reflect.Uint64:
		if attr.AutoIncrement() {
			return "bigserial", nil
		}
		return "bigint", nil

	case reflect.Float32, reflect.Float64:
		return "numeric", nil

	case reflect.String:
		if attr.Max() > 0 && attr.Max() < 65532 {
			return fmt.Sprintf("varchar(%v)", attr.Max()), nil
		}
		return "text", nil

	case reflect.Struct, reflect.Ptr:
		if attr.StructName() == "time.Time" {
			return "timestamp with time zone", nil
		}

		if strings.HasSuffix(attr.StructName(), "go-dukedb.Point") {
			return "point", nil
		}

	case reflect.Map:
		return "hstore", nil

	case reflect.Slice:
		if attr.StructName() == "byte" {
			return "bytea", nil
		}
	}
	return "", apperror.New("unsupported_column_type",
		fmt.Sprintf("Field %v has unsupported type %v (postgres)", attr.Name(), attr.Type()))
}

func (d *baseDialect) PrepareExpression(e Expression) apperror.Error {
	d.SqlTranslator.PrepareExpression(e)
	return nil
}

type MysqlDialect struct {
	baseDialect
}

func (MysqlDialect) New() Dialect {
	return &MysqlDialect{}
}

type SqliteDialect struct {
	baseDialect
}

func (SqliteDialect) New() Dialect {
	return &SqliteDialect{}
}
