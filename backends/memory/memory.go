package memory

import (
	"fmt"
	"reflect"
	"strconv"

	"github.com/theduke/go-apperror"
	"github.com/theduke/go-reflector"

	db "github.com/theduke/go-dukedb"
	. "github.com/theduke/go-dukedb/expressions"
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
	b := &Backend{}
	b.BaseBackend = db.NewBaseBackend(b)
	b.SetName("memory")

	b.data = make(map[string]map[string]interface{})

	b.MigrationHandler = db.NewMigrationHandler(b)
	b.MigrationVersion = 0
	b.RegisterModel(&MigrationAttempt{})

	b.BuildLogger()

	return b
}

func (b *Backend) HasStringIds() bool {
	return false
}

func (b *Backend) HasNativeJoins() bool {
	return false
}

func (b *Backend) Clone() db.Backend {
	copied := &Backend{
		BaseBackend:      b.BaseBackend,
		data:             b.data,
		MigrationHandler: b.MigrationHandler,
		MigrationVersion: b.MigrationVersion,
	}

	return copied
}

func (b *Backend) RegisterModel(m interface{}) *db.ModelInfo {
	info := b.BaseBackend.RegisterModel(m)
	b.data[info.Collection()] = make(map[string]interface{})
	return info
}

func (b *Backend) Build() {
	b.BaseBackend.Build()

	// Add an id field to m2m collections.
	for _, info := range b.ModelInfos() {
		for _, relation := range info.Relations() {
			if relation.RelationType() != db.RELATION_TYPE_M2M {
				continue
			}

			m2mCol := b.ModelInfo(relation.BackendName())
			attr := &db.Attribute{}
			attr.SetName("id")
			attr.SetBackendName("id")
			attr.SetMarshalName("id")
			attr.SetIsPrimaryKey(true)
			attr.SetIsUnique(true)
			attr.SetType(reflect.TypeOf(""))
			m2mCol.AddAttribute(attr)

			b.data[relation.BackendName()] = make(map[string]interface{})
		}
	}
}

func (b *Backend) sort(info *db.ModelInfo, items *reflector.SliceReflector, field Expression, asc bool) apperror.Error {
	fieldName := ""
	if id, ok := field.(*IdentifierExpr); ok {
		fieldName = id.Identifier()
	} else if id, ok := field.(*ColFieldIdentifierExpr); ok {
		if id.Collection() != info.Collection() {
			return apperror.New("unsupported_sort", fmt.Sprint("The memory backend does not support sorting with joined collections"))
		}
		fieldName = id.Field()
	} else {
		return apperror.New("unsupported_sort", fmt.Sprintf("The memory backend does not support sorting with custom field expressions"))
	}

	attr := info.FindAttribute(fieldName)
	if attr == nil {
		return apperror.New("invalid_sort", fmt.Sprintf("Invalid sort for inexistant field %v", fieldName))
	}

	if err := items.SortByField(attr.Name(), asc); err != nil {
		return apperror.Wrap(err, "sort_error")
	}

	return nil
}

func (b *Backend) filter(info *db.ModelInfo, items *reflector.SliceReflector, filter Expression) (*reflector.SliceReflector, apperror.Error) {
	filtered, err := items.FilterBy(func(item *reflector.Reflector) (bool, error) {
		return b.filterItem(info, item, filter)
	})

	if err != nil {
		return nil, apperror.Wrap(err, "filter_error")
	}
	return filtered, nil
}

func (b *Backend) filterItem(info *db.ModelInfo, item *reflector.Reflector, filter Expression) (bool, apperror.Error) {
	switch f := filter.(type) {
	case *AndExpr:
		// Check each filter.
		for _, e := range f.Expressions() {
			flag, err := b.filterItem(info, item, e)
			if err != nil {
				return false, err
			} else if !flag {
				// One filter does not match, so return false.
				return false, nil
			}
		}
		// All filters matched, we can return true.
		return true, nil

	case *OrExpr:
		// Check each filter.
		for _, e := range f.Expressions() {
			flag, err := b.filterItem(info, item, e)
			if err != nil {
				return false, err
			} else if flag {
				// One match is enough, so return true.
				return true, nil
			}
		}
		// No filters matched, so return false.
		return false, nil

	case *NotExpr:
		// Reverse nested filter.
		flag, err := b.filterItem(info, item, f.Not())
		if err != nil {
			return false, err
		} else {
			return !flag, nil
		}

	case FilterExpression:
		field := f.Field()

		fieldName := ""

		if id, ok := field.(*IdentifierExpr); ok {
			fieldName = id.Identifier()
		} else if id, ok := field.(*ColFieldIdentifierExpr); ok {
			if id.Collection() != info.Collection() {
				return false, apperror.New("unsupported_filter", fmt.Sprint("The memory backend does not support filtering with joined collections"))
			}
			fieldName = id.Field()
		} else {
			return false, apperror.New("unsupported_filter", fmt.Sprintf("The memory backend does not support filtering with custom field expressions"))
		}

		attr := info.FindAttribute(fieldName)
		if attr == nil {
			return false, apperror.New("invalid_filter", fmt.Sprintf("Invalid filter for inexistant field %v", fieldName))
		}

		operator := f.Operator()

		var clauseValue interface{}

		if valExpr, ok := f.Clause().(*ValueExpr); ok {
			clauseValue = valExpr.Value()
		} else {
			return false, apperror.New("unsupported_filter_clause", fmt.Sprintf("The memory backend does not support filtering with custom clause expressions"))
		}

		if info.StructName() != "" {
			b.Logger().Infof("filtering item with %v %v %v", fieldName, operator, clauseValue)
			s, err := item.Struct()
			if err != nil {
				return false, apperror.Wrap(err, "invalid_model_error")
			}
			flag, err := s.Field(attr.Name()).CompareTo(clauseValue, operator)
			if err != nil {
				return false, apperror.Wrap(err, "compare_error")
			}
			return flag, nil
		} else {
			// Assume a map.
			if !item.IsMap() {
				return false, apperror.New("filter_invalid_model", "Could not filter because model value is neither struct nor map.")
			}
			flag, err := reflector.R(item.Value().MapIndex(reflect.ValueOf(attr.BackendName()))).CompareTo(clauseValue, operator)
			if err != nil {
				return false, apperror.Wrap(err, "compare_error")
			}
			return flag, nil
		}

	default:
		b.Logger().Panicf("Unhandled filter expression: %v", reflect.TypeOf(filter))
	}

	return false, nil
}

func (b *Backend) exec(statement Expression) ([]interface{}, apperror.Error) {
	switch s := statement.(type) {
	case *CreateCollectionStmt:
		col := s.Collection()

		if _, ok := b.data[col]; !ok {
			b.data[col] = make(map[string]interface{})
		}

	case *RenameCollectionStmt:
		b.data[s.NewName()] = b.data[s.Collection()]
		delete(b.data, s.Collection())

	case *DropFieldStmt:
		// No-op.

	case *DropCollectionStmt:
		delete(b.data, s.Collection())

	case *CreateFieldStmt:
		// No-op.

	case *RenameFieldStmt:
		// No-op.

	case *CreateIndexStmt:
		// No-op.

	case *DropIndexStmt:
		// No-op.

	case *SelectStmt:
		info := b.ModelInfos().Find(s.Collection())
		if info == nil {
			return nil, apperror.New("unknown_collection", fmt.Sprintf("Collection %v was not registered with backend", s.Collection()))
		}

		collection := info.Collection()
		b.Logger().Infof("all data: %+v", b.data)
		allData := b.data[collection]
		items := reflector.R(info.Item()).NewSlice()
		for _, item := range allData {
			if err := items.AppendValue(item); err != nil {
				return nil, apperror.Wrap(err, "slice_append_error")
			}
		}

		b.Logger().Infof("Executing select with all data: %v", len(allData))

		if filter := s.Filter(); filter != nil {
			b.Logger().Infof("filtering with %+v", filter)
			if filteredItems, err := b.filter(info, items, filter); err != nil {
				return nil, err
			} else {
				items = filteredItems
			}
		}

		if sorts := s.Sorts(); len(sorts) > 1 {
			panic("Memory backend does not support sorting by more than one field")
		} else if len(sorts) == 1 {
			b.Logger().Infof("Sorting with %+v", sorts[0])
			if err := b.sort(info, items, sorts[0].Expression(), sorts[0].Ascending()); err != nil {
				return nil, err
			}
		}

		if offset := s.Offset(); offset > 0 {
			if offset > items.Len() {
				offset = items.Len()
			}
			var err error
			items, err = items.Slice(offset, -1)
			if err != nil {
				// Should never happen, just be save.
				panic(err)
			}
		}

		if limit := s.Limit(); limit > 0 {
			if limit > items.Len() {
				limit = items.Len()
			}
			var err error
			items, err = items.Slice(0, limit)
			if err != nil {
				// Should never happen, just be save.
				panic(err)
			}
		}

		b.Logger().Infof("select result: %+v", items.Len())

		if len(s.Joins()) > 0 {
			panic("Memory backend does not support native joins.")
		}

		ifSlice := make([]interface{}, items.Len(), items.Len())
		for i, item := range items.Items() {
			ifSlice[i] = item.Interface()
		}
		b.Logger().Infof("if slice %+v", ifSlice)
		return ifSlice, nil

	case *JoinStmt:
		panic("Memory backend does not support native joins.")

	case *CreateStmt:
		collection := s.Collection()
		info := b.ModelInfos().Find(collection)
		if info == nil {
			return nil, apperror.New("unknown_collection", fmt.Sprintf("Collection %v was not registered with backend", s.Collection()))
		}
		collection = info.Collection()

		obj := s.RawValue()

		collection = info.Collection()

		b.Logger().Infof("Creating with alldata: %+v", b.data[collection])

		var newId string

		if info.HasStruct() {
			id, err := info.DetermineModelStrId(obj)
			if err != nil {
				return nil, err
			}
			if id == "" {
				// Empty id, so create a new one and update the model.
				intId := len(b.data[collection]) + 1
				id = strconv.Itoa(intId)
				if err := info.SetModelId(obj, id); err != nil {
					return nil, err
				}
			}
			newId = id
		} else {
			// Map instead of struct.
			mapObj := obj.(map[string]interface{})
			rawId := mapObj[info.PkAttribute().BackendName()]
			idRefl := reflector.R(rawId)

			id := ""
			if idRefl.IsZero() {
				intId := len(b.data[collection]) + 1
				id = strconv.Itoa(intId)
				mapObj[info.PkAttribute().BackendName()] = id
			} else {
				strId, err := idRefl.ConvertTo("")
				if err != nil {
					return nil, apperror.Wrap(err, "id_conversion_error")
				}
				mapObj[info.PkAttribute().BackendName()] = strId.(string)
			}

			obj = mapObj
			newId = id
		}

		b.data[collection][newId] = obj
		b.Logger().Infof("created model %+v", obj)

	case *UpdateStmt:

		obj := s.RawValue()
		info, err := b.InfoForModel(obj)
		if err == nil {
			// Direct update for one model.
			// So just update the model in the data.
			id, err := info.DetermineModelStrId(obj)
			if err != nil {
				return nil, err
			}
			b.data[info.Collection()][id] = obj

			// All done.
			return nil, nil
		}

		// Must be a custom update with a select.

		info = b.ModelInfos().Find(s.Collection())
		if info == nil {
			return nil, apperror.New("unknown_collection", fmt.Sprintf("Collection %v was not registered with backend", s.Collection()))
		}

		// Execute select query to find items.
		items, err := b.exec(s.Select())
		if err != nil {
			return nil, err
		}

		slice := reflector.R(items).MustSlice()

		// Determine update data.
		var data map[string]interface{}
		if d, ok := s.RawValue().(map[string]interface{}); ok {
			// Data supplied as raw value.
			data = d
		} else {
			// Build a map with the fields values to update.
			data = make(map[string]interface{})
			for _, field := range s.Values() {
				expr, ok := field.Field().(*IdentifierExpr)
				if !ok {
					return nil, apperror.New("unsupported_field_expression",
						"The memory backend does not support custom field expressions")
				}
				valExpr, ok := field.Value().(*ValueExpr)
				if !ok {
					return nil, apperror.New("unsupported_field_value_expression",
						"The memory backend does not support custom field value expressions")
				}

				attr := info.FindAttribute(expr.Identifier())
				if attr == nil {
					return nil, apperror.New("unknown_field",
						fmt.Sprintf("The collection %v does not have a field %v", info.Collection(), expr.Identifier()))
				}

				data[attr.Name()] = valExpr.Value()
			}
		}

		// Update each item with the new data.
		for _, item := range slice.Items() {
			if item.IsStruct() || item.IsStructPtr() {
				s := item.MustStruct()

				for key, val := range data {
					if err := s.Field(key).SetValue(val); err != nil {
						return nil, apperror.Wrap(err, "struct_field_update_error")
					}
				}
			} else if item.IsMap() {
				for key, val := range data {
					if err := item.SetStrMapKeyValue(key, val, true); err != nil {
						return nil, apperror.Wrap(err, "struct_field_update_error")
					}
				}
			}
		}

	case *DeleteStmt:
		info := b.ModelInfos().Find(s.Collection())

		// Execute select query to find items.
		items, err := b.exec(s.SelectStmt())
		if err != nil {
			return nil, err
		}

		slice := reflector.R(items).MustSlice()
		for _, item := range slice.Items() {
			id, err := info.DetermineModelStrId(item.Interface())
			if err != nil {
				return nil, err
			}

			delete(b.data[info.Collection()], id)
		}

	default:
		panic(fmt.Sprintf("Unhandled statement type: %v", reflect.TypeOf(statement)))
	}

	return nil, nil
}

func (b *Backend) Exec(statement Expression) apperror.Error {
	_, err := b.exec(statement)
	return err
}

func (b *Backend) ExecQuery(statement FieldedExpression) ([]interface{}, apperror.Error) {
	return b.exec(statement)
}

func (b *Backend) Count(q *db.Query) (int, apperror.Error) {
	items, err := b.Query(q)
	if err != nil {
		return 0, err
	}
	return len(items), nil
}

/*
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
				return &apperror.Err{
					Code:    "pk_exists",
					Message: fmt.Sprintf("A model of type %v with id %v already exists", collection, id),
				}
			}
		} else {
			// Generate new id.
			id = strconv.Itoa(len(b.data[collection]) + 1)
			if err := b.SetModelID(m, id); err != nil {
				return &apperror.Err{
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

func (b *Backend) Save(m interface{}) apperror.Error {
	return db.BackendSave(b, m)
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
			return &apperror.Err{
				Code:    "not_found",
				Message: fmt.Sprintf("A model of type %v with id %v does not exists", collection, id),
			}
		}

		delete(b.data[collection], id)
		return nil
	})
}

func (b *Backend) Related(model interface{}, name string) (db.RelationQuery, apperror.Error) {
	return db.BackendRelated(b, model, name)
}

*/

/**
 * M2M
 */

/*

func (b *Backend) M2M(obj interface{}, name string) (db.M2MCollection, apperror.Error) {
	collection, err := db.GetModelCollection(obj)
	if err != nil {
		return nil, err
	}

	info := b.ModelInfo(collection)
	fieldInfo, hasField := info.FieldInfo[name]

	if !hasField {
		return nil, &apperror.Err{
			Code:    "unknown_field",
			Message: fmt.Sprintf("The model %v has no field %v", collection, name),
		}
	}

	if !fieldInfo.M2M {
		return nil, &apperror.Err{
			Code:    "no_m2m_field",
			Message: fmt.Sprintf("The %v on model %v is not m2m", name, collection),
		}
	}

	objVal := reflect.ValueOf(obj).Elem()
	items := objVal.FieldByName(name)

	col := M2MCollection{
		Backend:  b,
		object:   objVal,
		itemType: fieldInfo.Type.Elem(),
		items:    items,
	}

	return &col, nil
}

type M2MCollection struct {
	Backend db.Backend
	Name    string

	object   reflect.Value
	itemType reflect.Type
	items    reflect.Value
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
	slice := db.InterfaceToTypedSlice(c.itemType, items)
	c.items.Set(reflect.ValueOf(slice).Convert(reflect.SliceOf(c.itemType)))
	return nil
}
*/
