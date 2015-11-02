package sql

import (
	"database/sql"
	"reflect"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/theduke/go-apperror"
	db "github.com/theduke/go-dukedb"
	. "github.com/theduke/go-dukedb/expressions"
)

type Backend struct {
	db.BaseBackend
	dialect Dialect

	sqlProfilingEnabled bool

	Db *sql.DB
	Tx *sql.Tx

	migrationHandler *db.MigrationHandler
}

// Ensure Backend implements dukedb.Backend.
var _ db.Backend = (*Backend)(nil)
var _ db.TransactionBackend = (*Backend)(nil)
var _ db.MigrationBackend = (*Backend)(nil)

func New(driver, driverOptions string) (*Backend, apperror.Error) {
	b := &Backend{}
	b.BaseBackend = db.NewBaseBackend(b)
	b.SetName("sql")

	switch driver {
	case "postgres":
		b.dialect = NewPostgresDialect(b)
	case "mysql":
		b.dialect = &MysqlDialect{}
	case "sqlite3":
		b.dialect = &SqliteDialect{}
	default:
		panic("Unsupported sql driver: " + driver)
	}

	DB, err := sql.Open(driver, driverOptions)
	if err != nil {
		return nil, apperror.Wrap(err, "sql_connection_error")
	}

	b.Db = DB

	b.migrationHandler = db.NewMigrationHandler(b)
	b.RegisterModel(&MigrationAttempt{})

	b.BuildLogger()

	return b, nil
}

func (b *Backend) HasStringIds() bool {
	return false
}

func (b *Backend) IsSqlProfilingEnabled() bool {
	return b.sqlProfilingEnabled
}

func (b *Backend) EnableSqlProfiling() {
	b.sqlProfilingEnabled = true
}

func (b *Backend) DisableSqlProfiling() {
	b.sqlProfilingEnabled = false
}

func (b *Backend) Clone() db.Backend {
	base := b.BaseBackend.Clone()
	return &Backend{
		BaseBackend: *base,
		dialect:     b.dialect,
	}
}

/**
 * Transactions.
 */

func (b *Backend) Begin() (db.Transaction, apperror.Error) {
	if b.Tx != nil {
		panic("Can't call .Begin() on a transaction.")
	}

	copied := b.Clone().(*Backend)
	tx, err := b.Db.Begin()
	if err != nil {
		return nil, apperror.Wrap(err, "begin_transaction_failed")
	}

	copied.Tx = tx
	copied.Db = nil

	return copied, nil
}

func (b *Backend) MustBegin() db.Transaction {
	tx, err := b.Begin()
	if err != nil {
		panic(err)
	}
	return tx
}

func (b *Backend) Rollback() apperror.Error {
	if err := b.Tx.Rollback(); err != nil {
		return apperror.Wrap(err, "transaction_rollback_failed")
	}
	return nil
}

func (b *Backend) Commit() apperror.Error {
	if err := b.Tx.Commit(); err != nil {
		return apperror.Wrap(err, "transaction_commit_failed")
	}
	return nil
}

func (b *Backend) Build() {
	b.BaseBackend.Build()

	for _, info := range b.ModelInfos() {
		for _, attr := range info.Attributes() {
			typ, err := b.dialect.DetermineColumnType(attr)
			if err != nil {
				panic(err)
			}
			attr.SetBackendType(typ)
		}
	}
}

func (b *Backend) SqlExec(query string, args ...interface{}) (sql.Result, error) {
	var res sql.Result
	var err error

	var started time.Time
	if b.sqlProfilingEnabled {
		started = time.Now()
	}

	if b.Tx != nil {
		res, err = b.Tx.Exec(query, args...)
	} else {
		res, err = b.Db.Exec(query, args...)
	}

	if err != nil {
		b.Logger().Errorf("SQL error: %v: %v | %+v", err, query, args)
	} else if b.sqlProfilingEnabled {
		b.Logger().WithFields(logrus.Fields{
			"action": "sql_exec",
			"sql":    query,
			"args":   args,
			"ms":     time.Now().Sub(started).Nanoseconds() / 1000,
		}).Debugf("SQL exec")
	} else if b.Debug() {
		b.Logger().Debugf("SQL exec: %v | %v", query, args)
	}

	return res, err
}

func (b *Backend) SqlQuery(query string, args ...interface{}) (*sql.Rows, error) {
	var rows *sql.Rows
	var err error

	var started time.Time
	if b.sqlProfilingEnabled {
		started = time.Now()
	}

	if b.Tx != nil {
		rows, err = b.Tx.Query(query, args...)
	} else {
		rows, err = b.Db.Query(query, args...)
	}

	if err != nil {
		b.Logger().Errorf("SQL error: %v: %v | %+v", err, query, args)
	} else if b.sqlProfilingEnabled {
		b.Logger().WithFields(logrus.Fields{
			"action": "sql_query",
			"sql":    query,
			"args":   args,
			"ms":     time.Now().Sub(started).Nanoseconds() / 1000,
		}).Debugf("SQL exec")
	} else if b.Debug() {
		b.Logger().Debugf("SQL query: %v | %v", query, args)
	}

	return rows, err
}

func (b *Backend) Exec(statement Expression) apperror.Error {
	if s, ok := statement.(*CreateCollectionStmt); ok {
		if len(s.Constraints()) > 0 {
			b.Logger().Infof("%+v\n", s.Constraints()[0])
		}
	}

	dialect := b.dialect.New()
	if err := dialect.PrepareExpression(statement); err != nil {
		return err
	}
	if err := dialect.Translate(statement); err != nil {
		return err
	}

	sql := dialect.String()
	args := dialect.RawArguments()

	_, err := b.SqlExec(sql, args...)
	if err != nil {
		return apperror.Wrap(err, "sql_error")
	}

	return nil
}

func (b *Backend) ExecQuery(statement FieldedExpression, resultAsMap bool) ([]map[string]interface{}, apperror.Error) {
	dialect := b.dialect.New()
	if err := dialect.PrepareExpression(statement); err != nil {
		return nil, err
	}
	if err := dialect.Translate(statement); err != nil {
		return nil, err
	}

	sql := dialect.String()
	args := dialect.RawArguments()

	rows, err2 := b.SqlQuery(sql, args...)
	if err2 != nil {
		return nil, apperror.Wrap(err2, "sql_error")
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, apperror.Wrap(err, "sql_rows_error")
	}

	colMap := make(map[string]int)
	for i, col := range cols {
		colMap[col] = i
	}

	fieldMap := make(map[string]reflect.Type)
	for _, field := range statement.Fields() {
		if e, ok := field.(NamedTypedExpression); ok && e.Type() != nil && e.Name() != "" {
			if _, ok := colMap[e.Name()]; ok {
				fieldMap[e.Name()] = e.Type()
			}
		}
	}

	result := make([]map[string]interface{}, 0)
	for rows.Next() {
		values := make([]reflect.Value, len(cols), len(cols))
		pointers := make([]interface{}, len(cols), len(cols))

		for i, col := range cols {
			if typ, ok := fieldMap[col]; ok {
				r := reflect.New(typ)
				values[i] = r
				pointers[i] = r.Interface()
			} else {
				r := reflect.ValueOf(pointers[i]).Addr()
				pointers[i] = r.Interface()
			}
		}

		if err := rows.Scan(pointers...); err != nil {
			return nil, apperror.Wrap(err, "sql_scan_error")
		}

		m := make(map[string]interface{})
		for i, col := range cols {
			m[col] = values[i].Elem().Interface()
		}

		result = append(result, m)
	}
	if err := rows.Err(); err != nil {
		return nil, apperror.Wrap(err, "sql_rows_error")
	}

	b.Logger().Infof("result: %+v\n", result)

	return result, nil
}

func (b *Backend) CreateCollection(collections ...string) apperror.Error {
	for _, collection := range collections {
		if err := b.BaseBackend.CreateCollection(collection); err != nil {
			return err
		}

		// Call dialect AfterCollectionCreate hook.
		// Needed because we want auto incrementing fields to start with 1 instead of 0,
		// and this is dialect specific.
		if err := b.dialect.AfterCollectionCreate(b.ModelInfo(collection)); err != nil {
			return err
		}
	}

	return nil
}
