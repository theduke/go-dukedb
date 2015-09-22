package dukedb

import (
	"time"

	"github.com/Sirupsen/logrus"
)

type DbError interface {
	GetCode() string
	GetMessage() string
	GetData() interface{}
	IsInternal() bool
	GetErrors() []error
	AddError(error)
	Error() string
}

type BackendQueryMixin interface {
	GetBackend() Backend
	SetBackend(Backend)

	Find(targetSlice ...interface{}) ([]interface{}, DbError)
	First(targetModel ...interface{}) (interface{}, DbError)
	Last(targetModel ...interface{}) (interface{}, DbError)
	Count() (int, DbError)
	Delete() DbError
}

type Query interface {
	BackendQueryMixin

	GetCollection() string

	Limit(int) Query
	GetLimit() int

	Offset(int) Query
	GetOffset() int

	Fields(...string) Query
	AddFields(...string) Query
	LimitFields(...string) Query
	GetFields() []string

	Order(name string, asc bool) Query
	SetOrders(...OrderSpec) Query
	GetOrders() []OrderSpec

	// Filters.

	FilterQ(...Filter) Query
	Filter(field string, val interface{}) Query
	FilterCond(field string, condition string, val interface{}) Query

	AndQ(filters ...Filter) Query
	And(field string, val interface{}) Query
	AndCond(field, condition string, val interface{}) Query

	OrQ(filters ...Filter) Query
	Or(field string, val interface{}) Query
	OrCond(field string, condition string, val interface{}) Query

	NotQ(...Filter) Query
	Not(field string, val interface{}) Query
	NotCond(field string, condition string, val interface{}) Query

	GetFilters() []Filter
	SetFilters(f ...Filter) Query

	// Joins.

	JoinQ(jq RelationQuery) Query
	Join(fieldName string) Query
	GetJoin(field string) RelationQuery
	GetJoins() []RelationQuery

	// Related.

	Related(name string) RelationQuery
	RelatedCustom(name, collection, joinKey, foreignKey, typ string) RelationQuery
}

type RelationQuery interface {
	// RelationQuery specific methods.

	GetCollection() string

	GetBaseQuery() Query
	SetBaseQuery(Query)

	GetRelationName() string
	GetJoinType() string

	GetJoinFieldName() string
	SetJoinFieldName(string)

	GetForeignFieldName() string
	SetForeignFieldName(string)

	Build() (Query, DbError)

	// BackendQuery methods.
	BackendQueryMixin

	Limit(int) RelationQuery
	GetLimit() int

	Offset(int) RelationQuery
	GetOffset() int

	Fields(...string) RelationQuery
	AddFields(...string) RelationQuery
	LimitFields(...string) RelationQuery
	GetFields() []string

	Order(name string, asc bool) RelationQuery
	SetOrders(...OrderSpec) RelationQuery
	GetOrders() []OrderSpec

	// Filters.

	FilterQ(...Filter) RelationQuery
	Filter(field string, val interface{}) RelationQuery
	FilterCond(field string, condition string, val interface{}) RelationQuery

	AndQ(filters ...Filter) RelationQuery
	And(field string, val interface{}) RelationQuery
	AndCond(field, condition string, val interface{}) RelationQuery

	OrQ(filters ...Filter) RelationQuery
	Or(field string, val interface{}) RelationQuery
	OrCond(field string, condition string, val interface{}) RelationQuery

	NotQ(...Filter) RelationQuery
	Not(field string, val interface{}) RelationQuery
	NotCond(field string, condition string, val interface{}) RelationQuery

	GetFilters() []Filter
	SetFilters(f ...Filter) RelationQuery

	// Joins.

	JoinQ(jq RelationQuery) RelationQuery
	Join(fieldName string) RelationQuery
	GetJoin(field string) RelationQuery
	GetJoins() []RelationQuery

	// Related.

	Related(name string) RelationQuery
	RelatedCustom(name, collection, joinKey, foreignKey, typ string) RelationQuery
}

type Backend interface {
	// Returns the name of the backend.
	Name() string

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

	InfoForModel(model interface{}) (*ModelInfo, DbError)

	SetModelInfo(collection string, info *ModelInfo)

	// Get model info for all registered collections.
	AllModelInfo() map[string]*ModelInfo
	SetAllModelInfo(map[string]*ModelInfo)

	// Determine if a model collection is registered with the backend.
	HasCollection(collection string) bool

	ModelToMap(model interface{}, marshal bool) (map[string]interface{}, DbError)

	// After all models have been registered, build the relationship
	// info.
	BuildRelationshipInfo()

	// Get a new struct instance to a model struct based on model Collection.
	NewModel(collection string) (interface{}, DbError)

	// Build a slice of a model for model Collection.
	NewModelSlice(collection string) (interface{}, DbError)

	// Determine the ID for a model.
	ModelID(model interface{}) (interface{}, DbError)

	// Determine the ID for a model, and panic on error.
	MustModelID(model interface{}) interface{}

	// Set the id field on a model.
	SetModelID(model interface{}, id interface{}) DbError

	// Set the id  field on a model and panic on error.
	MustSetModelID(model interface{}, id interface{})

	// Determine the  ID for a model and convert it to string.
	ModelStrID(model interface{}) (string, DbError)

	// Determine the  ID for a model and convert it to string. Panics on error.
	MustModelStrID(model interface{}) string

	// Create the specified collection in the backend.
	// (eg the table or the mongo collection)
	CreateCollection(collection string) DbError
	CreateCollections(collection ...string) DbError
	DropCollection(collection string) DbError
	DropAllCollections() DbError

	// Create a new query for a collection.
	Q(collection string) Query

	// Perform a query.
	Query(q Query, targetSlice ...interface{}) ([]interface{}, DbError)

	// Perform a query and get the first result.
	QueryOne(q Query, targetModel ...interface{}) (interface{}, DbError)

	// Perform a query and get the last result.
	Last(q Query, targetModel ...interface{}) (interface{}, DbError)

	// Find first model with primary key ID.
	FindBy(collection, field string, value interface{}, targetSlice ...interface{}) ([]interface{}, DbError)

	// Find a model in a collection by ID.
	FindOne(collection string, id interface{}, targetModel ...interface{}) (interface{}, DbError)

	// Find a model  in a collection based on a field value.
	FindOneBy(collection, field string, value interface{}, targetModel ...interface{}) (interface{}, DbError)

	// Count by a query.
	Count(Query) (int, DbError)

	// Based on a RelationQuery, return a query for the specified
	// relation.
	BuildRelationQuery(q RelationQuery) (Query, DbError)

	// Return a M2MCollection instance for a model, which allows
	// to add/remove/clear items in the m2m relationship.
	M2M(model interface{}, name string) (M2MCollection, DbError)

	// Convenience methods.

	Create(model interface{}) DbError
	Update(model interface{}) DbError
	UpdateByMap(model interface{}, data map[string]interface{}) DbError
	Delete(model interface{}) DbError
	DeleteMany(Query) DbError
}

type M2MCollection interface {
	Add(models ...interface{}) DbError
	Delete(models ...interface{}) DbError
	Clear() DbError
	Replace(models []interface{}) DbError

	Count() int
	Contains(model interface{}) bool
	ContainsID(id interface{}) bool
	GetByID(id interface{}) interface{}
	All() []interface{}
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

	Create() DbError
	Update() DbError
	Delete() DbError
}

type ModelCollectionHook interface {
	Collection() string
}

type ModelBackendNameHook interface {
	BackendName() string
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
	Validate() DbError
}

type ModelBeforeCreateHook interface {
	BeforeCreate(Backend) DbError
}

type ModelAfterCreateHook interface {
	AfterCreate(Backend)
}

type ModelBeforeUpdateHook interface {
	BeforeUpdate(Backend) DbError
}

type ModelAfterUpdateHook interface {
	AfterUpdate(Backend)
}

type ModelBeforeDeleteHook interface {
	BeforeDelete(Backend) DbError
}

type ModelAfterDeleteHook interface {
	AfterDelete(Backend)
}

type ModelAfterQueryHook interface {
	AfterQuery(Backend)
}
