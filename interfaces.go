package dukedb

import (
	"time"

	"github.com/Sirupsen/logrus"
)

type DbError interface {
	GetCode() string
	GetMessage() string
	GetData() interface{}
	GetErrors() []error
	AddError(error)
	Error() string
}

type Backend interface {
	GetName() string

	GetDebug() bool
	SetDebug(bool)

	GetLogger() *logrus.Logger
	SetLogger(*logrus.Logger)

	// Duplicate the backend.
	Copy() Backend

	RegisterModel(Model) error
	GetModelInfo(string) *ModelInfo
	GetAllModelInfo() map[string]*ModelInfo
	// Determine if a model type is registered with the backend.
	HasModel(string) bool

	// After all models have been registered, build the relationship
	// info.
	BuildRelationshipInfo()

	// Get a new struct instance to a model struct.
	NewModel(string) (interface{}, DbError)
	// Build a slice of a model.
	NewModelSlice(string) (interface{}, DbError)

	// Create the specified collection in the backend.
	// (eg the table or the mongo collection)
	CreateCollection(name string) DbError
	DropCollection(name string) DbError
	DropAllCollections() DbError

	// Return a new query connected to the backend.
	Q(modelType string) *Query

	// Perform a query.
	Query(*Query) ([]Model, DbError)
	QueryOne(*Query) (Model, DbError)

	Last(*Query) (Model, DbError)
	Count(*Query) (int, DbError)

	// Based on a RelationQuery, return a query for the specified
	// relation.
	BuildRelationQuery(q *RelationQuery) (*Query, DbError)

	// Return a M2MCollection instance for a model, which allows
	// to add/remove/clear items in the m2m relationship.
	M2M(obj Model, name string) (M2MCollection, DbError)

	// Convenience methods.

	// Find first model with primary key ID.
	FindOne(modelType string, id string) (Model, DbError)

	FindBy(modelType, field string, value interface{}) ([]Model, DbError)
	FindOneBy(modelType, field string, value interface{}) (Model, DbError)

	Create(Model) DbError
	Update(Model) DbError
	UpdateByMap(Model, map[string]interface{}) DbError
	Delete(Model) DbError
	DeleteMany(*Query) DbError
}

type M2MCollection interface {
	Add(...Model) DbError
	Delete(...Model) DbError
	Clear() DbError
	Replace([]Model) DbError

	Count() int
	Contains(Model) bool
	ContainsID(string) bool
	GetByID(string) Model
	All() []Model
}

type Transaction interface {
	Backend
	Rollback() DbError
	Commit() DbError
}

type TransactionBackend interface {
	Backend
	Begin() Transaction
}

type MigrationAttempt interface {
	Model
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

	MigrationsSetup() DbError

	IsMigrationLocked() (bool, DbError)
	DetermineMigrationVersion() (int, DbError)

	NewMigrationAttempt() MigrationAttempt
}

type Model interface {
	Collection() string
	GetID() string
	SetID(string) error
}
