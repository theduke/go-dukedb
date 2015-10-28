package dukedb

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/theduke/go-apperror"
)

type Backend interface {
	// Returns the name of the backend.
	Name() string
	SetName(name string)

	// Returns true if the backend uses string IDs like MongoDB.
	HasStringIDs() bool

	GetDebug() bool
	SetDebug(bool)

	GetLogger() *logrus.Logger
	SetLogger(*logrus.Logger)

	// Duplicate the backend.
	Copy() Backend

	// Register a model type.
	RegisterModel(model interface{})

	// Get the model info for a collection.
	ModelInfo(collection string) *ModelInfo

	// Retrieve the ModelInfo for a model.
	InfoForModel(model interface{}) (*ModelInfo, apperror.Error)
	InfoForCollection(collection string) (*ModelInfo, apperror.Error)

	SetModelInfo(collection string, info *ModelInfo)

	// Get model info for all registered collections.
	AllModelInfo() map[string]*ModelInfo
	SetAllModelInfo(map[string]*ModelInfo)

	// Determine if a model collection is registered with the backend.
	HasCollection(collection string) bool

	ModelToMap(model interface{}, marshal bool, includeRelations bool) (map[string]interface{}, apperror.Error)

	// After all models have been registered, build the relationship
	// info.
	BuildRelationshipInfo()

	// Get a new struct instance to a model struct based on model Collection.
	CreateModel(collection string) (interface{}, apperror.Error)

	// Same as CreateModel(), but panics on error.
	MustCreateModel(collection string) interface{}

	// "Merge" a model that implements Model interface by setting backend and info data.
	MergeModel(model Model)

	// Build a slice of a model for model Collection.
	CreateModelSlice(collection string) (interface{}, apperror.Error)

	// Determine the ID for a model.
	ModelID(model interface{}) (interface{}, apperror.Error)

	// Determine the ID for a model, and panic on error.
	MustModelID(model interface{}) interface{}

	// Set the id field on a model.
	SetModelID(model interface{}, id interface{}) apperror.Error

	// Set the id  field on a model and panic on error.
	MustSetModelID(model interface{}, id interface{})

	// Determine the  ID for a model and convert it to string.
	ModelStrID(model interface{}) (string, apperror.Error)

	// Determine the  ID for a model and convert it to string. Panics on error.
	MustModelStrID(model interface{}) string

	// Create the specified collection in the backend.
	// (eg the table or the mongo collection)
	CreateCollection(collection string) apperror.Error
	CreateCollections(collection ...string) apperror.Error
	DropCollection(collection string) apperror.Error
	DropAllCollections() apperror.Error

	// Create a new query for a collection.
	Q(collection string) *Query

	// Perform a query.
	Query(q *Query, targetSlice ...interface{}) ([]interface{}, apperror.Error)

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

	// Count by a query.
	Count(*Query) (int, apperror.Error)

	// Based on a RelationQuery, return a query for the specified
	// relation.
	// The third skip parameter is true when the base query does not contain any results.
	BuildRelationQuery(q *RelationQuery) (*Query, apperror.Error)

	// Retrieve a query for a relationship.
	Related(model interface{}, name string) (*RelationQuery, apperror.Error)

	// Return a M2MCollection instance for a model, which allows
	// to add/remove/clear items in the m2m relationship.
	M2M(model interface{}, name string) (M2MCollection, apperror.Error)

	// Convenience methods.

	Create(model interface{}) apperror.Error
	Update(model interface{}) apperror.Error
	Save(model interface{}) apperror.Error
	UpdateByMap(model interface{}, data map[string]interface{}) apperror.Error
	Delete(model interface{}) apperror.Error
	DeleteQ(*Query) apperror.Error

	// Hooks.

	// RegisterHook registers a hook function that will be called for a model.
	// The available hooks are: (before/after)_(create/update/delete).
	RegisterHook(hook string, handler HookHandler)

	// GetHooks returns a slice with all hooks of the hook type.
	GetHooks(hook string) []HookHandler
}

type HookHandler func(backend Backend, obj interface{}) apperror.Error

type M2MCollection interface {
	Add(models ...interface{}) apperror.Error
	Delete(models ...interface{}) apperror.Error
	Clear() apperror.Error
	Replace(models []interface{}) apperror.Error

	Count() int
	Contains(model interface{}) bool
	ContainsID(id interface{}) bool
	GetByID(id interface{}) interface{}
	All() []interface{}
}

type Transaction interface {
	Backend
	Rollback() apperror.Error
	Commit() apperror.Error
}

type TransactionBackend interface {
	Backend
	Begin() Transaction
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

type Model interface {
	ModelCollectionHook
	ModelBackendNameHook
	ModelIDGetterHook
	ModelIDSetterHook
	ModelStrIDGetterHook
	ModelStrIDSetterHook

	Info() *ModelInfo
	SetInfo(info *ModelInfo)

	Backend() Backend
	SetBackend(backend Backend)
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
