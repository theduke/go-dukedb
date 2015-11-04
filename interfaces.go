package dukedb

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/theduke/go-apperror"
	. "github.com/theduke/go-dukedb/expressions"
)

const (
	HOOK_BEFORE_CREATE = "before_create"
	HOOK_AFTER_CREATE  = "after_create"
	HOOK_BEFORE_UPDATE = "before_update"
	HOOK_AFTER_UPDATE  = "after_update"
	HOOK_BEFORE_DELETE = "before_delete"
	HOOK_AFTER_DELETE  = "after_delete"
)

type Cursor interface {
	// Count returns the total number of items.
	Count() int

	// HasNext returns true if a next item exists.
	HasNext() bool

	// Retrieve the next result item.
	//
	// As with other query methods, you may pass a pointer to the model
	// that should be filled with the data.
	Next(targetModel ...interface{}) (interface{}, apperror.Error)
}

type JoinAssigner func(relation *Relation, joinQ *RelationQuery, objs, joinedModels []interface{})

type Backend interface {
	// Returns the name of the backend.
	Name() string
	SetName(name string)

	// Returns true if the backend uses string IDs like MongoDB.
	HasStringIds() bool

	HasNativeJoins() bool

	// Debug returns true if debugging is enabled.
	Debug() bool

	// SetDebug enables or disables the debug mode.
	// In debug mode, all queries will be logged, and some methods will panic
	// instead of returning an error.
	SetDebug(bool)

	Logger() *logrus.Logger
	SetLogger(*logrus.Logger)

	ProfilingEnabled() bool
	EnableProfiling()
	DisableProfiling()

	// Duplicate the backend.
	Clone() Backend

	/**
	 * Hooks.
	 */

	// RegisterHook registers a hook function that will be called for a model.
	// The available hooks are: (before/after)_(create/update/delete).
	RegisterHook(hook string, handler HookHandler)

	// GetHooks returns a slice with all hooks of the hook type.
	GetHooks(hook string) []HookHandler

	/**
	 * ModelInfo and registration.
	 */

	// Get model info for all registered collections.
	ModelInfos() ModelInfos

	// Get the model info for a collection.
	// Returns nil if not found.
	ModelInfo(collection string) *ModelInfo

	// Retrieve the ModelInfo for a model instance.
	InfoForModel(model interface{}) (*ModelInfo, apperror.Error)

	// Determine if a collection is registered with the backend.
	HasCollection(collection string) bool

	// RegisterModel registers a model type witht the backend.
	//
	// The first argument must be a pointer to an instance of the model,
	// for example: &MyModel{}
	RegisterModel(model interface{}) *ModelInfo

	// Build analyzes the relationships between models and does all neccessary
	// preparations for using the backend.
	//
	// Build MUST be called AFTER all models have been registered with
	// backend.RegisterModel() and BEFORE the backend is used.
	Build()

	// NewModel creates a new model instance of the specified collection.
	NewModel(collection string) (interface{}, apperror.Error)

	// Build a slice of a model for model Collection.
	NewModelSlice(collection string) (interface{}, apperror.Error)

	// ModelToMap converts a model to a map.
	ModelToMap(model interface{}, marshal bool, includeRelations bool) (map[string]interface{}, apperror.Error)

	/**
	 * Statements.
	 */

	// Exec executes an expression.
	// The result will be nil for all statements except a SelectStatement.
	Exec(statement Expression) apperror.Error

	ExecQuery(statement FieldedExpression, resultAsMap bool) (result []map[string]interface{}, err apperror.Error)

	// Create the specified collection in the backend.
	// (eg the table or the mongo collection)
	CreateCollection(collection ...string) apperror.Error

	// RenameCollection renames a collection to a new name.
	RenameCollection(collection, newName string) apperror.Error
	DropCollection(collection string, ifExists, cascade bool) apperror.Error
	DropAllCollections() apperror.Error

	// CreateField creates the specified field on a collection.
	// Note that the field must already be on the model struct, or an error
	// will be returned.
	//
	// If you need to create arbitrary fields that are not on your model,
	// use Exec() with a CreateFieldStatement.
	CreateField(collection, field string) apperror.Error

	// RenameField renames a collection field.
	// The model must already have the new name, or an error will be returned.
	RenameField(collection, field, newName string) apperror.Error

	// DropField drops a field from a collection.
	// The field must be the backend name.
	DropField(collection, field string) apperror.Error

	// Create an index for fields on a collection.
	// If you need more complex indexes, use Exec() with a CreateIndexStatement.
	CreateIndex(collection, indexName string, fields ...string) apperror.Error

	// Drop an index.
	DropIndex(indexName string) apperror.Error

	NewQuery(collection string) (*Query, apperror.Error)
	NewModelQuery(model interface{}) (*Query, apperror.Error)

	// Create a new query.
	//
	// Can be used with different signatures:
	// backend.Q("collection_name") => Get a query for a collection.
	// backend.Q("collection_name", ID) => Get a query for a model in a collection. ID must be numeric or string.
	// backend.Q(&myModel) => Get a query for a model.
	Q(collectionOrModel interface{}, extraModels ...interface{}) *Query

	// Executes a query, fetches ALL results and returns them.
	// If you expect a large number of results, you should use QueryCursor(), which
	// returns an iterable cursor.
	Query(q *Query, targetSlice ...interface{}) ([]interface{}, apperror.Error)

	// Executes a query, and returns a cursor.
	QueryCursor(q *Query) (Cursor, apperror.Error)

	// Perform a query and get the first result.
	QueryOne(q *Query, targetModel ...interface{}) (interface{}, apperror.Error)

	// Perform a query and get the last result.
	Last(q *Query, targetModel ...interface{}) (interface{}, apperror.Error)

	// Find first model with primary key ID.
	FindBy(collection, field string, value interface{}, targetSlice ...interface{}) ([]interface{}, apperror.Error)

	// Find a model in a collection by ID.
	FindOne(collection string, id interface{}, targetModel ...interface{}) (interface{}, apperror.Error)

	// Find a model  in a collection based on a field value.
	FindOneBy(collection, field string, value interface{}, targetModel ...interface{}) (interface{}, apperror.Error)

	// Retrieve the count for a query.
	Count(*Query) (int, apperror.Error)

	// Pluck retrieves all fields specified on a query, and returns them as a
	// map.
	Pluck(q *Query) ([]map[string]interface{}, apperror.Error)

	// Based on a RelationQuery, return a query for the specified
	// relation.
	BuildRelationQuery(q *RelationQuery) (*Query, apperror.Error)

	// Retrieve a query for a relationship.
	Related(model interface{}, name string) (*RelationQuery, apperror.Error)

	// Return a M2MCollection instance for a model, which allows
	// to add/remove/clear items in the m2m relationship.
	M2M(model interface{}, name string) (M2MCollection, apperror.Error)

	// C(r)UD methods.

	// Create creates the model in the backend.
	Create(model ...interface{}) apperror.Error

	CreateByMap(collection string, data map[string]interface{}) (result interface{}, err apperror.Error)

	// Update a model.
	Update(model interface{}) apperror.Error

	// Save is a convenience method that created the passed model if it
	// is new, or updates it otherwise.
	Save(model interface{}) apperror.Error

	// Updat all models matching a query by values in a map.
	UpdateByMap(query *Query, data map[string]interface{}) apperror.Error

	// Delete deletes the model from the backend.
	Delete(model interface{}) apperror.Error

	// DeleteQ deletes all models that match the passed query.
	DeleteMany(*Query) apperror.Error
}

type HookHandler func(backend Backend, obj interface{}) apperror.Error

type M2MCollection interface {
	Add(models ...interface{}) apperror.Error
	Remove(models ...interface{}) apperror.Error
	Clear() apperror.Error
	Replace(models ...interface{}) apperror.Error

	Count() (int, apperror.Error)
	Contains(model interface{}) (bool, apperror.Error)
	ContainsId(id interface{}) (bool, apperror.Error)
	All() ([]interface{}, apperror.Error)

	Q() *Query
}

type Transaction interface {
	Backend
	Rollback() apperror.Error
	Commit() apperror.Error
}

type TransactionBackend interface {
	Backend
	Begin() (Transaction, apperror.Error)
	MustBegin() Transaction
}

type MigrationAttempt interface {
	GetVersion() int
	SetVersion(int)

	GetStartedAt() time.Time
	SetStartedAt(time.Time)

	GetFinishedAt() time.Time
	SetFinishedAt(time.Time)

	GetComplete() bool
	SetComplete(bool)
}

type MigrationBackend interface {
	Backend
	GetMigrationHandler() *MigrationHandler

	MigrationsSetup() apperror.Error

	IsMigrationLocked() (bool, apperror.Error)
	DetermineMigrationVersion() (int, apperror.Error)

	NewMigrationAttempt() MigrationAttempt
}

type ModelCollectionHook interface {
	Collection() string
}

type ModelBackendNameHook interface {
	BackendName() string
}

type ModelMarshalNameHook interface {
	MarshalName() string
}

type ModelIDGetterHook interface {
	GetID() interface{}
}

type ModelIDSetterHook interface {
	SetID(id interface{}) error
}

type ModelStrIDGetterHook interface {
	GetStrID() string
}

type ModelStrIDSetterHook interface {
	SetStrID(id string) error
}

type ModelValidateHook interface {
	Validate() error
}

type ModelBeforeCreateHook interface {
	BeforeCreate(Backend) error
}

type ModelAfterCreateHook interface {
	AfterCreate(Backend)
}

type ModelBeforeUpdateHook interface {
	BeforeUpdate(Backend) error
}

type ModelAfterUpdateHook interface {
	AfterUpdate(Backend)
}

type ModelBeforeDeleteHook interface {
	BeforeDelete(Backend) error
}

type ModelAfterDeleteHook interface {
	AfterDelete(Backend)
}

type ModelAfterQueryHook interface {
	AfterQuery(Backend)
}
