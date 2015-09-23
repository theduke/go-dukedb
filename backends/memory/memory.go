package memory

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/theduke/go-apperror"

	db "github.com/theduke/go-dukedb"
)

type Backend struct {
	db.BaseBackend

	data map[string]map[string]interface{}

	MigrationHandler *db.MigrationHandler
	MigrationVersion int
}

// Ensure that Backend implements the db.Backend interface at compile time.
var _ db.Backend = (*Backend)(nil)
var _ db.MigrationBackend = (*Backend)(nil)

func New() *Backend {
	b := Backend{}

	b.SetAllModelInfo(make(map[string]*db.ModelInfo))
	b.data = make(map[string]map[string]interface{})

	b.MigrationHandler = db.NewMigrationHandler(&b)
	b.MigrationVersion = 0

	b.RegisterModel(&MigrationAttempt{})

	return &b
}

func (b *Backend) HasStringIDs() bool {
	return false
}

func (b *Backend) Name() string {
	return "memory"
}

func (b *Backend) SetDebug(d bool) {
	b.Debug = d
}

func (b *Backend) Copy() db.Backend {
	copied := Backend{}

	copied.SetAllModelInfo(b.AllModelInfo())
	copied.data = b.data
	copied.SetDebug(b.GetDebug())
	return &copied
}

func (b *Backend) RegisterModel(m interface{}) {
	b.BaseBackend.RegisterModel(m)
	collection := db.MustGetModelCollection(m)
	b.data[collection] = make(map[string]interface{})
}

func (b *Backend) CreateModel(collection string) (interface{}, apperror.Error) {
	return db.BackendCreateModel(b, collection)
}

func (b *Backend) MustCreateModel(collection string) interface{} {
	return db.BackendMustCreateModel(b, collection)
}

func (b *Backend) MergeModel(model db.Model) {
	db.BackendMergeModel(b, model)
}

func (b *Backend) ModelToMap(m interface{}, marshal bool) (map[string]interface{}, apperror.Error) {
	info, err := b.InfoForModel(m)
	if err != nil {
		return nil, err
	}

	return db.ModelToMap(info, m, false, marshal)
}

func (b *Backend) CreateCollection(collection string) apperror.Error {
	_, err := b.InfoForCollection(collection)
	if err != nil {
		return err
	}

	b.data[collection] = make(map[string]interface{})

	return nil
}

func (b *Backend) CreateCollections(names ...string) apperror.Error {
	for _, name := range names {
		if err := b.CreateCollection(name); err != nil {
			return &apperror.AppError{
				Code:    "create_collection_error",
				Message: fmt.Sprintf("Could not create collection %v: %v", name, err),
			}
		}
	}

	return nil
}

func (b *Backend) DropCollection(collection string) apperror.Error {
	_, err := b.InfoForCollection(collection)
	if err != nil {
		return err
	}

	if _, ok := b.data[collection]; ok {
		b.data[collection] = make(map[string]interface{})
	}

	return nil
}

func (b *Backend) DropAllCollections() apperror.Error {
	info := b.AllModelInfo()
	for name := range info {
		if err := b.DropCollection(name); err != nil {
			return err
		}
	}

	return nil
}

func (b *Backend) Q(model string) db.Query {
	q := db.Q(model)
	q.SetBackend(b)
	return q
}

func filterStruct(info *db.ModelInfo, item interface{}, filter db.Filter) (bool, apperror.Error) {
	filterType := filter.Type()

	if filterType == "and" {
		and := filter.(*db.AndCondition)

		// Check each and filter.
		for _, andFilter := range and.Filters {
			if ok, err := filterStruct(info, item, andFilter); err != nil {
				// Error occurred, return it.
				return false, err
			} else if !ok {
				// No match, return false.
				return false, nil
			}
		}

		// All filters matched, return true.
		return true, nil
	}

	if filterType == "or" {
		or := filter.(*db.OrCondition)

		// Check each or filter.
		for _, orFilter := range or.Filters {
			if ok, err := filterStruct(info, item, orFilter); err != nil {
				// Error occurred, return it.
				return false, err
			} else if ok {
				// One positivie match is enough. Return true.
				return true, nil
			}
		}

		// No or clause matched, return false.
		return false, nil
	}

	if filterType == "not" {
		not := filter.(*db.NotCondition)

		for _, notFilter := range not.Filters {
			if ok, err := filterStruct(info, item, notFilter); err != nil {
				// Error occurred, return it.
				return false, err
			} else if ok {
				// One positivie match means a NOT condition is true, so return false
				return false, nil
			}
		}

		return true, nil
	}

	if condition, ok := filter.(*db.FieldCondition); ok {
		val := condition.Value
		// The actual value for the filtered field.

		fieldName := condition.Field
		if mappedName := info.MapFieldName(fieldName); mappedName != "" {
			fieldName = mappedName
		}

		structVal, err := db.GetStructFieldValue(item, fieldName)
		if err != nil {
			return false, err
		}

		match, err := db.CompareValues(condition.Type(), structVal, val)
		if err != nil {
			return false, err
		}

		return match, nil
	}

	// If execution comes here, filter type is unsupported.
	return false, &apperror.AppError{
		Code:    "unsupported_filter",
		Message: fmt.Sprintf("The filter %v is not supported by the memory backend", filter.Type()),
	}
}

func (b *Backend) executeQuery(q db.Query) ([]interface{}, apperror.Error) {
	info, err := b.InfoForCollection(q.GetCollection())
	if err != nil {
		return nil, err
	}

	items := make([]interface{}, 0)

	for _, item := range b.data[q.GetCollection()] {
		isMatched := true

		// Filter items.
		if q.GetFilters() != nil {
			for _, filter := range q.GetFilters() {
				if ok, err := filterStruct(info, item, filter); err != nil {
					return nil, err
				} else if !ok {
					isMatched = false
					break
				}
			}
		}

		if isMatched {
			items = append(items, item)
		}
	}

	// Handle field specificiaton.
	if len(q.GetFields()) > 0 {
		return nil, &apperror.AppError{
			Code:    "memory_backend_unsupported_feature_fieldspec",
			Message: "The memory backend does not support limiting fields",
		}
	}

	// Ordering.
	if len(q.GetOrders()) == 0 {
		q.Order(info.PkField, true)
	}

	// Set default order.
	if len(q.GetOrders()) > 0 {
		if len(q.GetOrders()) > 1 {
			return nil, &apperror.AppError{
				Code:    "memory_backend_unsupported_feature_multiple_orders",
				Message: "The memory backend does not support multiple orderings",
			}
		}

		// Ensure the field exists.
		field := q.GetOrders()[0].Field
		if !info.HasField(field) {
			field = info.MapFieldName(field)
		}
		if !info.HasField(field) {
			return nil, &apperror.AppError{
				Code:    "cant_sort_on_inexistant_field",
				Message: fmt.Sprintf("Trying to sort on non-existant field %v", field),
			}
		}

		db.SortStructSlice(items, field, q.GetOrders()[0].Ascending)
	}

	// Limit & Offset.

	if q.GetOffset() != 0 {
		items = items[q.GetOffset():]
	}
	if q.GetLimit() != 0 {
		items = items[:q.GetLimit()]
	}

	return items, nil
}

func (b *Backend) BuildRelationQuery(q db.RelationQuery) (db.Query, apperror.Error) {
	return db.BuildRelationQuery(b, nil, q)
}

func (b *Backend) doQuery(q db.Query) ([]interface{}, apperror.Error) {
	res, err := b.executeQuery(q)
	if err != nil {
		return nil, err
	}
	slice, err2 := db.ConvertInterfaceToSlice(res)
	if err2 != nil {
		return nil, apperror.Wrap(err2, "interface_conversion_error")
	}
	return slice, nil
}

// Perform a query.
func (b *Backend) Query(q db.Query, targetSlices ...interface{}) ([]interface{}, apperror.Error) {
	res, err := b.doQuery(q)
	return db.BackendQuery(b, q, targetSlices, res, err)
}

func (b *Backend) QueryOne(q db.Query, targetModel ...interface{}) (interface{}, apperror.Error) {
	return db.BackendQueryOne(b, q, targetModel)
}

func (b *Backend) Count(q db.Query) (int, apperror.Error) {
	result, err := b.executeQuery(q)
	if err != nil {
		return 0, apperror.Wrap(err, "memory_count_error")
	}

	return len(result), nil
}

func (b *Backend) Last(q db.Query, targetModel ...interface{}) (interface{}, apperror.Error) {
	return db.BackendLast(b, q, targetModel)
}

// Find first model with primary key ID.
func (b *Backend) FindOne(modelType string, id interface{}, targetModel ...interface{}) (interface{}, apperror.Error) {
	return db.BackendFindOne(b, modelType, id, targetModel)
}

func (b *Backend) FindBy(modelType, field string, value interface{}, targetSlice ...interface{}) ([]interface{}, apperror.Error) {
	return db.BackendFindBy(b, modelType, field, value, targetSlice)
}

func (b *Backend) FindOneBy(modelType, field string, value interface{}, targetModel ...interface{}) (interface{}, apperror.Error) {
	return db.BackendFindOneBy(b, modelType, field, value, targetModel)
}

// Convenience methods.

// Store the model.
// Fails if the model type was not registered, or if the primary key already
// exists.
func (b *Backend) Create(m interface{}) apperror.Error {
	return db.BackendCreate(b, m, func(info *db.ModelInfo, m interface{}) apperror.Error {
		collection := info.Collection

		id := b.MustModelStrID(m)
		if !db.IsZero(id) {
			if _, ok := b.data[collection][id]; ok {
				return &apperror.AppError{
					Code:    "pk_exists",
					Message: fmt.Sprintf("A model of type %v with id %v already exists", collection, id),
				}
			}
		} else {
			// Generate new id.
			id = strconv.Itoa(len(b.data[collection]) + 1)
			if err := b.SetModelID(m, id); err != nil {
				return &apperror.AppError{
					Code:    "set_id_error",
					Message: fmt.Sprintf("Error while setting the id %v on model %v", id, collection),
				}
			}
		}

		b.data[collection][id] = m
		return nil
	})
}

func (b *Backend) Update(m interface{}) apperror.Error {
	return db.BackendUpdate(b, m, func(info *db.ModelInfo, m interface{}) apperror.Error {
		b.data[info.Collection][b.MustModelStrID(m)] = m
		return nil
	})
}

func (b *Backend) UpdateByMap(m interface{}, data map[string]interface{}) apperror.Error {
	info, err := b.InfoForModel(m)
	if err != nil {
		return err
	}

	if err := db.UpdateModelFromData(info, m, data); err != nil {
		return err
	}

	b.data[info.Collection][b.MustModelStrID(m)] = m
	return nil
}

func (b *Backend) Delete(m interface{}) apperror.Error {
	return db.BackendDelete(b, m, func(info *db.ModelInfo, m interface{}) apperror.Error {
		id := b.MustModelStrID(m)
		collection := info.Collection

		if _, ok := b.data[collection][id]; !ok {
			return &apperror.AppError{
				Code:    "not_found",
				Message: fmt.Sprintf("A model of type %v with id %v does not exists", collection, id),
			}
		}

		delete(b.data[collection], id)
		return nil
	})
}

func (b *Backend) DeleteMany(q db.Query) apperror.Error {
	result, err := b.executeQuery(q)
	if err != nil {
		return err
	}

	for _, item := range result {
		if err := b.Delete(item); err != nil {
			return err
		}
	}

	return nil
}

/**
 * M2M
 */

func (b *Backend) M2M(obj interface{}, name string) (db.M2MCollection, apperror.Error) {
	collection, err := db.GetModelCollection(obj)
	if err != nil {
		return nil, err
	}

	info := b.ModelInfo(collection)
	fieldInfo, hasField := info.FieldInfo[name]

	if !hasField {
		return nil, &apperror.AppError{
			Code:    "unknown_field",
			Message: fmt.Sprintf("The model %v has no field %v", collection, name),
		}
	}

	if !fieldInfo.M2M {
		return nil, &apperror.AppError{
			Code:    "no_m2m_field",
			Message: fmt.Sprintf("The %v on model %v is not m2m", name, collection),
		}
	}

	items, _ := db.GetStructFieldValue(obj, name)

	col := M2MCollection{
		Backend: b,
		items:   reflect.ValueOf(items),
	}

	return &col, nil
}

type M2MCollection struct {
	Backend db.Backend
	Name    string
	items   reflect.Value
}

// Ensure that M2MCollection implements the db.M2MCollection interface at compile time.
var _ db.M2MCollection = (*M2MCollection)(nil)

func (c M2MCollection) Count() int {
	return c.items.Len()
}

func (c M2MCollection) Contains(m interface{}) bool {
	return c.GetByID(c.Backend.MustModelID(m)) != nil
}

func (c M2MCollection) ContainsID(id interface{}) bool {
	return c.GetByID(id) != nil
}

func (c M2MCollection) All() []interface{} {
	slice, _ := db.ConvertInterfaceToSlice(c.items.Interface())
	return slice
}

func (c M2MCollection) GetByID(id interface{}) interface{} {
	for i := 0; i < c.items.Len(); i++ {
		model := c.items.Index(i).Interface()
		if c.Backend.MustModelID(model) == id {
			return model
		}
	}

	return nil
}

func (c *M2MCollection) Add(items ...interface{}) apperror.Error {
	for _, item := range items {
		if !c.Contains(item) {
			reflect.Append(c.items, reflect.ValueOf(item))
		}
	}

	return nil
}

func (c *M2MCollection) Delete(items ...interface{}) apperror.Error {
	for _, item := range items {
		itemId := c.Backend.MustModelID(item)

		for i := 0; i < c.items.Len(); i++ {
			curItem := c.items.Index(i).Elem().Interface()
			curItemId := c.Backend.MustModelID(curItem)

			if curItemId == itemId {
				// Replace all items after the one to delete.
				for j := i + 1; j < c.items.Len(); j++ {
					c.items.Index(j - 1).Set(c.items.Index(j).Elem())
				}
				// Decrement length. One extra-item will remain in the slice,
				// but will be overwritten on the next append.
				c.items.SetLen(c.items.Len() - 1)
				break
			}
		}
	}
	return nil
}

func (c *M2MCollection) Clear() apperror.Error {
	c.items.SetLen(0)
	return nil
}

func (c *M2MCollection) Replace(items []interface{}) apperror.Error {
	for i, item := range items {
		c.items.Index(i).Set(reflect.ValueOf(item))
	}
	c.items.SetLen(len(items))
	return nil
}
