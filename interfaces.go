package dukedb

type Backend interface {
	GetName() string

	Debug() bool
	SetDebug(bool)

	RegisterModel(Model) error
	GetModelInfo(string) *ModelInfo

	// Determine if a model type is registered with the backend.
	HasModel(string) bool

	// Get a new struct instance to a model struct.
	NewModel(string) (interface{}, error)
	// Build a slice of a model.
	NewModelSlice(string) (interface{}, error)

	// Return a new query connected to the backend.
	Q(modelType string) *Query
	
	// Perform a query.	
	Query(*Query) ([]Model, error)
	QueryOne(*Query) (Model, error)

	// Convenience methods.
	 
	// Find first model with primary key ID.
	FindOne(modelType string, id string) (Model, error)

	FindBy(modelType, field string, value interface{}) ([]Model, error)
	FindOneBy(modelType, field string, value interface{}) (Model, error)

	Create(Model) error
	Update(Model) error
	Delete(Model) error
}

type Model interface {
	GetCollection() string
	GetID() string
	SetID(string)
}
