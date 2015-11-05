package orientdb

import (
	"reflect"
	//"time"

	"gopkg.in/istreamdata/orientgo.v2"
	_ "gopkg.in/istreamdata/orientgo.v2/obinary"

	"github.com/Sirupsen/logrus"
	"github.com/theduke/go-apperror"
	db "github.com/theduke/go-dukedb"
	. "github.com/theduke/go-dukedb/expressions"
)

type Backend struct {
	db.BaseBackend

	Db         *orient.Database
	translator *OrientTranslator

	migrationHandler *db.MigrationHandler
}

// Ensure Backend implements dukedb.Backend.
var _ db.Backend = (*Backend)(nil)

//var _ db.TransactionBackend = (*Backend)(nil)
//var _ db.MigrationBackend = (*Backend)(nil)

func New(host, database, user, password string) (*Backend, apperror.Error) {
	b := &Backend{}
	b.BaseBackend = db.NewBaseBackend(b)
	b.SetName("orientdb")

	//b.migrationHandler = db.NewMigrationHandler(b)
	//b.RegisterModel(&MigrationAttempt{})

	b.BuildLogger()

	// Connect to orient.
	client, err := orient.Dial(host)
	if err != nil {
		return nil, apperror.Wrap(err, "orient_dial_error")
	}
	db, err := client.Open(database, orient.DocumentDB, user, password)
	if err != nil {
		return nil, apperror.Wrap(err, "orient_db_open_error")
	}
	b.Db = db

	b.translator = NewTranslator(b)

	return b, nil
}

func (b *Backend) HasStringIds() bool {
	return true
}

func (b *Backend) HasNativeJoins() bool {
	return true
}

func (b *Backend) Clone() db.Backend {
	base := b.BaseBackend.Clone()
	return &Backend{
		BaseBackend:      *base,
		Db:               b.Db,
		translator:       b.translator,
		migrationHandler: b.migrationHandler,
	}
}

func (b *Backend) analyzeAllRelations() apperror.Error {
	for _, info := range b.ModelInfos() {
		if err := b.analyzeRelations(info); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backend) analyzeRelations(info *db.ModelInfo) apperror.Error {
	for _, field := range info.TransientFields() {
		relatedItem := reflect.New(field.StructType())
		relatedInfo, err := b.InfoForModel(relatedItem)
		if err != nil {
			// No collection found, so assume a regular attribute.
			attr := db.BuildAttribute(field)
			info.AddAttribute(attr)
			continue
		}

		relation := db.BuildRelation(field)
		relation.SetModel(info)
		relation.SetRelatedModel(relatedInfo)

		if !relation.IsMany() {
			relation.SetRelationType(db.RELATION_TYPE_HAS_ONE)
		} else {
			relation.SetRelationType(db.RELATION_TYPE_HAS_MANY)
		}
		info.AddRelation(relation)
	}

	info.SetTransientFields(nil)

	return nil
}

func (b *Backend) Build() {
	if err := b.analyzeAllRelations(); err != nil {
		panic(err)
	}

	for _, info := range b.ModelInfos() {
		for _, attr := range info.Attributes() {
			typ, err := b.translator.DetermineColumnType(attr)
			if err != nil {
				panic(err)
			}
			attr.SetBackendType(typ)
		}
	}
}

// SqlExec executes a any SQL statement and returns the result.
// Inspect result.Err() to check for errors, and res.Next() or res.All() to retrieve results.
//
// IMPORTANT: always call res.Close() after you are done with the result.
func (b *Backend) SqlExec(query string, args ...interface{}) orient.Results {
	res := b.Db.Command(orient.NewSQLCommand(query, args...))

	if res.Err() != nil {
		b.Logger().Errorf("SQL error: %v: %v | %+v", res.Err(), query, args)
	} else if b.Debug() {
		b.Logger().WithFields(logrus.Fields{
			"action": "sql_exec",
			"sql":    query,
			"args":   args,
		}).Debugf("SQL exec")
	}

	return res
}

func (b *Backend) Exec(statement Expression) apperror.Error {
	translator := b.translator.New()
	if err := translator.PrepareExpression(statement); err != nil {
		return err
	}
	if err := translator.Translate(statement); err != nil {
		return err
	}

	sql := translator.String()
	args := translator.RawArguments()

	res := b.SqlExec(sql, args...)
	if res.Err() != nil {
		return apperror.Wrap(res.Err(), "orient_error")
	}
	if err := res.Close(); err != nil {
		return apperror.Wrap(err, "orient_close_result_error")
	}

	return nil
}

func (b *Backend) ExecQuery(statement FieldedExpression) ([]interface{}, apperror.Error) {
	translator := b.translator.New()
	if err := translator.PrepareExpression(statement); err != nil {
		return nil, err
	}
	if err := translator.Translate(statement); err != nil {
		return nil, err
	}

	sql := translator.String()
	args := translator.RawArguments()

	res := b.SqlExec(sql, args...)
	if res.Err() != nil {
		return nil, apperror.Wrap(res.Err(), "orient_error")
	}
	defer res.Close()

	var rawData interface{}
	if err := res.All(&rawData); err != nil {
		return nil, apperror.Wrap(err, "orient_result_retrieval_error")
	}

	rows, ok := rawData.([]orient.OIdentifiable)
	if !ok {
		return nil, apperror.New("invalid_non_record_orient_result")
	}

	items := make([]interface{}, len(rows), len(rows))

	var info *db.ModelInfo
	if sel, ok := statement.(*SelectStmt); ok {
		info = b.ModelInfos().Find(sel.Collection())
	}

	for i, rawItem := range rows {
		document, ok := rawItem.(*orient.Document)
		if !ok {
			continue
		}
		data := make(map[string]interface{})

		if info != nil && info.PkAttribute() != nil {
			data[info.PkAttribute().Name()] = document.RId
		}

		for _, docEntry := range document.Fields() {
			data[docEntry.Name] = docEntry.Value
		}

		items[i] = data
	}

	return items, nil
}

func (b *Backend) CreateField(collection, fieldName string) apperror.Error {
	if err := b.BaseBackend.CreateField(collection, fieldName); err != nil {
		return err
	}

	// Info is ensured to exist, since it was checked in BaseBackend.CreateField().
	info := b.ModelInfo(collection)
	attr := info.FindAttribute(fieldName)

	// Property exists. We now need to add all the constraints.
	if attr.Min() > 0 {
		stmt := NewAlterPropertyStmt(info.BackendName(), attr.BackendName(), PROPERTY_ATTR_MIN, attr.Min(), reflect.TypeOf(0))
		if err := b.Exec(stmt); err != nil {
			return err
		}
	}
	if attr.Max() > 0 {
		stmt := NewAlterPropertyStmt(info.BackendName(), attr.BackendName(), PROPERTY_ATTR_MAX, attr.Min(), reflect.TypeOf(0))
		if err := b.Exec(stmt); err != nil {
			return err
		}
	}
	if attr.IsRequired() {
		stmt := NewAlterPropertyStmt(info.BackendName(), attr.BackendName(), PROPERTY_ATTR_MANDATORY, true, reflect.TypeOf(true))
		if err := b.Exec(stmt); err != nil {
			return err
		}
	}
	if attr.DefaultValue() != nil {
		stmt := NewAlterPropertyStmt(info.BackendName(), attr.BackendName(), PROPERTY_ATTR_DEFAULT, attr.DefaultValue(), reflect.TypeOf(attr.DefaultValue()))
		if err := b.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func (b *Backend) CreateCustomField(collection, fieldName, typ string, required bool) apperror.Error {
	typExpr := NewFieldTypeExpr(typ, nil)
	fieldExpr := NewFieldExpr(fieldName, typExpr)
	stmt := NewCreateFieldStmt(collection, fieldExpr)

	if err := b.Exec(stmt); err != nil {
		return err
	}

	if required {
		stmt := NewAlterPropertyStmt(collection, fieldName, PROPERTY_ATTR_MANDATORY, true, reflect.TypeOf(true))
		if err := b.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func (b *Backend) CreateCollection(collections ...string) apperror.Error {
	for _, collection := range collections {
		if err := b.BaseBackend.CreateCollection(collection); err != nil {
			return err
		}

		// Info is ensured to exist, since it was checked in BaseBackend.CreateCollection().
		info := b.ModelInfo(collection)

		// Class is created. Now we need to create all properties.
		for _, attr := range info.Attributes() {
			if err := b.CreateField(info.Collection(), attr.Name()); err != nil {
				return err
			}
		}

		// Create relationship properties.
		for _, rel := range info.Relations() {
			var typ string
			if rel.IsMany() {
				typ = PROPERTY_LINKLIST
			} else {
				typ = PROPERTY_LINK
			}

			if err := b.CreateCustomField(info.BackendName(), rel.BackendName(), typ, rel.IsRequired()); err != nil {
				return err
			}

			// Set related type.
			stmt := NewAlterPropertyStmt(info.BackendName(), rel.BackendName(), PROPERTY_ATTR_LINKEDCLASS, rel.RelatedModel().BackendName())
			if err := b.Exec(stmt); err != nil {
				return err
			}
		}
	}

	return nil
}
