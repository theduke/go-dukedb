package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/theduke/go-apperror"

	db "github.com/theduke/go-dukedb"
	. "github.com/theduke/go-dukedb/expressions"
)

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

func (d *PostgresDialect) AfterCollectionCreate(info *db.ModelInfo) apperror.Error {
	for _, attr := range info.Attributes() {
		// Alter sequences to start at 1 instead of 0.
		if attr.AutoIncrement() {
			stmt := fmt.Sprintf("ALTER SEQUENCE %v_%v_seq RESTART WITH %v", info.BackendName(), attr.BackendName(), 1)
			if _, err := d.backend.SqlExec(stmt); err != nil {
				return apperror.Wrap(err, "sql_error")
			}
		}
	}

	return nil
}

func (d *PostgresDialect) PrepareExpression(expression Expression) apperror.Error {
	switch e := expression.(type) {
	case *CreateStmt:
		// Add RETURNING clause for id.
		info := d.modelInfo.Find(e.Collection())
		if info != nil {
			pk := info.PkAttribute()
			if pk != nil && pk.AutoIncrement() {
				e.AddField(NewFieldSelector(pk.Name(), info.BackendName(), pk.BackendName(), pk.Type()))
			}
		}

	case *SelectStmt:
		if len(e.Fields()) == 0 {
			// If no fields are specified, add all model attributes.
			info := d.modelInfo.Find(e.Collection())
			if info != nil {
				for name, attr := range info.Attributes() {
					e.AddField(NewFieldSelector(name, info.BackendName(), attr.BackendName(), attr.Type()))
				}
			}
		}

		for _, join := range e.Joins() {
			if len(join.Fields()) == 0 {
				name := join.Name()
				// If no fields are specified, add all model attributes.
				info := d.modelInfo.Find(join.Collection())
				if info != nil {
					for attrName, attr := range info.Attributes() {
						if name != "" {
							attrName = name + "." + attrName
						}
						join.AddField(NewFieldSelector(attrName, info.BackendName(), attr.BackendName(), attr.Type()))
					}
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
		fields := e.Fields()
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
