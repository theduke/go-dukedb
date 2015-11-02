package sql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/theduke/go-apperror"

	db "github.com/theduke/go-dukedb"
	. "github.com/theduke/go-dukedb/expressions"
)

type Dialect interface {
	ExpressionTranslator
	New() Dialect
	DetermineColumnType(attr *db.Attribute) (string, apperror.Error)
}

type baseDialect struct {
	SqlTranslator
	backend   *Backend
	modelInfo db.ModelInfos
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

type PostgresDialect struct {
	baseDialect
}

// Ensure PostgresDialect implements Dialect.
var _ Dialect = (*PostgresDialect)(nil)

func NewPostgresDialect(b *Backend) Dialect {
	d := &PostgresDialect{}
	d.SqlTranslator = NewSqlTranslator(d)
	d.backend = b
	d.modelInfo = b.ModelInfos()
	return d
}

func (d *PostgresDialect) New() Dialect {
	return NewPostgresDialect(d.backend)
}

func (d *PostgresDialect) PrepareExpression(expression Expression) apperror.Error {
	switch e := expression.(type) {
	case *CreateStmt:
		// Add RETURNING clause for id.
		info := d.modelInfo.Find(e.Collection())
		if info != nil {
			pk := info.PkAttribute()
			e.AddField(NameExpr(pk.Name(), NewIdExpr(pk.BackendName()), pk.Type()))
		}

	case *SelectStmt:
		if len(e.Fields()) == 0 {
			// If no fields are specified, add all model attributes.
			info := d.modelInfo.Find(e.Collection())
			if info != nil {
				for name, attr := range info.Attributes() {
					e.AddField(NewFieldSelectorExpr(name, info.BackendName(), attr.BackendName(), attr.Type()))
				}
			}
		}
	}

	d.SqlTranslator.PrepareExpression(expression)

	return nil
}

func (d *PostgresDialect) Translate(expression Expression) apperror.Error {
	switch e := expression.(type) {
	case *ConstraintExpr:
		// Ignore auto increment.
		if e.Constraint() == CONSTRAINT_AUTO_INCREMENT {
			return nil
		}
		// Ignore primary key.
		if e.Constraint() == CONSTRAINT_PRIMARY_KEY {
			return nil
		}

	case *CreateStmt:
		d.SqlTranslator.Translate(e)

		// Add returning fields.
		fields := e.GetFields()
		if len(fields) > 0 {
			d.W(" RETURNING ")
			for _, f := range fields {
				d.SqlTranslator.Translate(f)
			}
		}

		return nil
	}

	return d.SqlTranslator.Translate(expression)
}

func (PostgresDialect) Placeholder() string {
	return "${}$"
}

func (PostgresDialect) fixPlaceholders(stmt string) string {
	index := 0
	for {
		pos := strings.Index(stmt, "${}$")
		if pos == -1 {
			break
		}

		index += 1
		stmt = strings.Replace(stmt, "${}$", "$"+strconv.Itoa(index), 1)
	}

	return stmt
}

func (d *PostgresDialect) String() string {
	stmt := d.SqlTranslator.String()
	return d.fixPlaceholders(stmt)
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
