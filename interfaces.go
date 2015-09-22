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
	First(targetModel ...interface{}) (Model, DbError)
	Last(targetModel ...interface{}) (Model, DbError)
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
	GetName() string

	// Returns true if the backend uses string IDs like MongoDB.
	HasStringIDs() bool

	GetDebug() bool
	SetDebug(bool)

	GetLogger() *logrus.Logger
	SetLogger(*logrus.Logger)

	// Duplicate the backend.
	Copy() Backend

	RegisterModel(Model)
	GetModelInfo(string) *ModelInfo
	GetAllModelInfo() map[string]*ModelInfo
	// Determine if a model type is registered with the backend.
	HasModel(string) bool

	ModelToMap(m Model, marshal bool) (map[string]interface{}, DbError)

	// After all models have been registered, build the relationship
	// info.
	BuildRelationshipInfo()

	// Get a new struct instance to a model struct.
	NewModel(string) (Model, DbError)
	// Build a slice of a model.
	NewModelSlice(string) (interface{}, DbError)

	// Create the specified collection in the backend.
	// (eg the table or the mongo collection)
	CreateCollection(name string) DbError
	CreateCollections(name ...string) DbError
	DropCollection(name string) DbError
	DropAllCollections() DbError

	// Return a new query connected to the backend.
	Q(modelType string) Query

	// Perform a query.
	Query(q Query, targetSlice ...interface{}) ([]interface{}, DbError)
	QueryOne(q Query, targetModel ...interface{}) (Model, DbError)

	Last(q Query, targetModel ...interface{}) (Model, DbError)

	// Find first model with primary key ID.
	FindBy(modelType, field string, value interface{}, targetSlice ...interface{}) ([]interface{}, DbError)

	FindOne(modelType string, id interface{}, targetModel ...interface{}) (Model, DbError)
	FindOneBy(modelType, field string, value interface{}, targetModel ...interface{}) (Model, DbError)

	Count(Query) (int, DbError)

	// Based on a RelationQuery, return a query for the specified
	// relation.
	BuildRelationQuery(q RelationQuery) (Query, DbError)

	// Return a M2MCollection instance for a model, which allows
	// to add/remove/clear items in the m2m relationship.
	M2M(obj Model, name string) (M2MCollection, DbError)

	// Convenience methods.

	Create(Model) DbError
	Update(Model) DbError
	UpdateByMap(Model, map[string]interface{}) DbError
	Delete(Model) DbError
	DeleteMany(Query) DbError
}

type M2MCollection interface {
	Add(...interface{}) DbError
	Delete(...Model) DbError
	Clear() DbError
	Replace([]interface{}) DbError

	Count() int
	Contains(Model) bool
	ContainsID(string) bool
	GetByID(string) Model
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
