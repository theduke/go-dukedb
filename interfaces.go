package dukedb

import (
	"time"
)

type DbError interface {
	GetCode() string
	GetMessage() string
	GetData() interface{}
	Error() string
}

type Backend interface {
	GetName() string

	Debug() bool
	SetDebug(bool)

	// Duplicate the backend.
	Copy() Backend

	RegisterModel(Model) error
	GetModelInfo(string) *ModelInfo
	GetAllModelInfo() map[string]*ModelInfo

	// Determine if a model type is registered with the backend.
	HasModel(string) bool

	// Get a new struct instance to a model struct.
	NewModel(string) (interface{}, DbError)
	// Build a slice of a model.
	NewModelSlice(string) (interface{}, DbError)

	// Return a new query connected to the backend.
	Q(modelType string) *Query
	
	// Perform a query.	
	Query(*Query) ([]Model, DbError)
	QueryOne(*Query) (Model, DbError)

	// Convenience methods.
	 
	// Find first model with primary key ID.
	FindOne(modelType string, id string) (Model, DbError)

	FindBy(modelType, field string, value interface{}) ([]Model, DbError)
	FindOneBy(modelType, field string, value interface{}) (Model, DbError)

	Create(Model) DbError
	Update(Model) DbError
	Delete(Model) DbError
}

type Transaction interface {
	Backend
	Rollback() DbError
	Commit() DbError
}

type TransactionBackend interface {
	Backend
	BeginTransaction() Transaction
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
	GetCollection() string
	GetID() string
	SetID(string) error
}
