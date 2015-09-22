package dukedb

import (
	"fmt"
	"reflect"

	"github.com/Sirupsen/logrus"
)

type BaseM2MCollection struct {
	Backend Backend
	Name    string
	Items   []interface{}
}

func (c BaseM2MCollection) Count() int {
	return len(c.Items)
}

func (c BaseM2MCollection) Contains(model interface{}) bool {
	return c.GetByID(c.Backend.MustModelID(model)) != nil
}

func (c BaseM2MCollection) ContainsID(id interface{}) bool {
	return c.GetByID(id) != nil
}

func (c BaseM2MCollection) All() []interface{} {
	return c.Items
}

func (c BaseM2MCollection) GetByID(id interface{}) interface{} {
	for _, item := range c.Items {
		if c.Backend.MustModelID(item) == id {
			return item
		}
	}

	return nil
}

type BaseBackend struct {
	Debug     bool
	Logger    *logrus.Logger
	modelInfo map[string]*ModelInfo
}

func (b *BaseBackend) GetDebug() bool {
	return b.Debug
}

func (b *BaseBackend) SetDebug(x bool) {
	b.Debug = x
}

func (b *BaseBackend) GetLogger() *logrus.Logger {
	return b.Logger
}

func (b *BaseBackend) SetLogger(x *logrus.Logger) {
	b.Logger = x
}

func (b *BaseBackend) RegisterModel(model interface{}) {
	info, err := NewModelInfo(model)
	if err != nil {
		panic(fmt.Sprintf("Could not register model '%v': %v\n", reflect.TypeOf(model).Name(), err))
	}

	b.modelInfo[info.Collection] = info
}

func (b *BaseBackend) ModelInfo(collection string) *ModelInfo {
	info, _ := b.modelInfo[collection]
	return info
}

func (b *BaseBackend) ModelInfoForModel(model interface{}) (*ModelInfo, DbError) {
	collection, err := GetModelCollection(model)
	if err != nil {
		return nil, err
	}

	info := b.ModelInfo(collection)
	if info == nil {
		return nil, Error{
			Code:    "unknown_model",
			Message: fmt.Sprintf("Collection '%v' of type %v was not registered with backend", collection, reflect.TypeOf(model)),
		}
	}

	return info, nil
}

func (b *BaseBackend) SetModelInfo(collection string, info *ModelInfo) {
	b.modelInfo[collection] = info
}

func (b *BaseBackend) AllModelInfo() map[string]*ModelInfo {
	return b.modelInfo
}

func (b *BaseBackend) SetAllModelInfo(info map[string]*ModelInfo) {
	b.modelInfo = info
}

func (b *BaseBackend) BuildRelationshipInfo() {
	if err := BuildAllRelationInfo(b.modelInfo); err != nil {
		panic(fmt.Sprintf("Building relationship info failed: %v", err))
	}
}

func (b *BaseBackend) HasCollection(collection string) bool {
	_, ok := b.modelInfo[collection]
	return ok
}

func (b *BaseBackend) NewModel(collection string) (interface{}, DbError) {
	info, ok := b.modelInfo[collection]
	if !ok {
		return nil, Error{
			Code:    "unknown_collection",
			Message: fmt.Sprintf("Collection '%v' not registered with backend", collection),
		}
	}

	item, err := NewStruct(info.Item)
	if err != nil {
		return nil, Error{Code: err.Error(), Message: "Could not build new struct"}
	}

	return item, nil
}

func (b *BaseBackend) NewModelSlice(collection string) (interface{}, DbError) {
	info, ok := b.modelInfo[collection]
	if !ok {
		return nil, Error{
			Code:    "unknown_collection",
			Message: fmt.Sprintf("Collection '%v' not registered with backend", collection),
		}
	}

	return NewSlice(info.Item), nil
}

// Determine the ID for a model.
func (b *BaseBackend) ModelID(model interface{}) (interface{}, DbError) {
	if hook, ok := model.(ModelIDGetterHook); ok {
		return hook.GetID(), nil
	}

	collection, err := GetModelCollection(model)
	if err != nil {
		return nil, err
	}

	info := b.ModelInfo(collection)
	id, err := GetModelID(info, model)
	if err != nil {
		return nil, err
	}

	return id, nil
}

// Determine the ID for a model, and panic on error.
func (b *BaseBackend) MustModelID(model interface{}) interface{} {
	id, err := b.ModelID(model)
	if err != nil {
		panic(fmt.Sprintf("Could not determine ID for model: %v", err))
	}

	return id
}

// Determine the  ID for a model and convert it to string.
func (b *BaseBackend) ModelStrID(model interface{}) (string, DbError) {
	if hook, ok := model.(ModelStrIDGetterHook); ok {
		return hook.GetStrID(), nil
	}

	id, err := b.ModelID(model)
	if err != nil {
		return "", err
	}

	if IsZero(id) {
		return "", nil
	}

	return fmt.Sprint(id), nil
}

// Determine the  ID for a model and convert it to string. Panics on error.
func (b *BaseBackend) MustModelStrID(model interface{}) string {
	id, err := b.ModelStrID(model)
	if err != nil {
		panic(fmt.Sprintf("Could not determine id for model: %v", err))
	}

	return id
}

// Set the id field on a model.
func (b *BaseBackend) SetModelID(model interface{}, id interface{}) DbError {
	// Check if model implements SetStrID.
	// If so, use it.
	if strId, ok := id.(string); ok {
		if hook, ok := model.(ModelStrIDSetterHook); ok {
			err := hook.SetStrID(strId)
			if err != nil {
				return Error{
					Code:    "model_set_id_error",
					Message: err.Error(),
				}
			}
		}
	}

	// Check if model  implements SetID.
	// If so, use it.
	if hook, ok := model.(ModelIDSetterHook); ok {
		err := hook.SetID(id)
		if err != nil {
			return Error{
				Code:    "model_set_id_error",
				Message: err.Error(),
			}
		}
	}

	collection, err := GetModelCollection(model)
	if err != nil {
		return err
	}

	info := b.ModelInfo(collection)

	convertedId, err2 := Convert(id, info.GetField(info.PkField).Type)
	if err2 != nil {
		return Error{
			Code:    "id_conversion_error",
			Message: err.Error(),
		}
	}

	if err := SetStructField(model, info.PkField, convertedId); err != nil {
		return Error{
			Code:    err.Error(),
			Message: fmt.Sprintf("Could not set %v.%v to value %v: %v", collection, info.PkField, id),
		}
	}

	return nil
}

// Set the id  field on a model and panic on error.
func (b *BaseBackend) MustSetModelID(model interface{}, id interface{}) {
	if err := b.SetModelID(model, id); err != nil {
		panic(err.GetMessage())
	}
}

// Relationship stuff.
func BuildRelationQuery(b Backend, baseModels []interface{}, q RelationQuery) (Query, DbError) {
	baseQ := q.GetBaseQuery()
	baseInfo := b.ModelInfo(baseQ.GetCollection())
	if baseInfo == nil {
		panic(fmt.Sprintf("Model %v not registered with backend", baseQ.GetCollection()))
	}

	if baseModels == nil {
		var err DbError
		baseModels, err = baseQ.Find()
		if err != nil {
			return nil, err
		}

		if len(baseModels) == 0 {
			return nil, Error{
				Code:    "relation_on_empty_result",
				Message: "Called .Related() or .Join() on a query without result",
			}
		}
		if len(baseModels) > 1 {
			return nil, Error{
				Message: ".Related() called on query with more than one result",
			}
		}
	}

	var targetModelName, joinField, foreignFieldName string

	var newQuery Query

	if q.GetCollection() != "" && q.GetJoinFieldName() != "" {
		// Custom relation query.
		targetModelName = q.GetCollection()
		newQuery = b.Q(targetModelName)

		joinField = baseInfo.MapFieldName(q.GetJoinFieldName())
		foreignFieldName = q.GetForeignFieldName()

		if foreignFieldName == "" {
			// No foreign key specified, use primary key.
			relatedInfo := b.ModelInfo(targetModelName)
			foreignFieldName = relatedInfo.PkField
		}
	} else if q.GetRelationName() != "" {
		// query for a known relationship reflectet in the model struct.
		relInfo, ok := baseInfo.FieldInfo[q.GetRelationName()]
		if !ok {
			panic(fmt.Sprintf("Model %v has no relation to %v", baseQ.GetCollection(), q.GetRelationName()))
		}

		relatedInfo := b.ModelInfo(relInfo.RelationCollection)
		targetModelName = relatedInfo.Collection
		newQuery = b.Q(targetModelName)

		if relInfo.HasOne {
			joinField = relInfo.HasOneField
			foreignFieldName = baseInfo.FieldInfo[relInfo.HasOneForeignField].Name

			// Set field names on query so Join logic can use it.
			// Attention: ususally these fields contain the converted names,
			// but join logic requires the field names so we abuse those
			// RelatedQuery fields here.
			q.SetJoinFieldName(joinField)
			q.SetForeignFieldName(relInfo.HasOneForeignField)
		} else if relInfo.BelongsTo {
			joinField = relInfo.BelongsToField
			foreignFieldName = relatedInfo.FieldInfo[relInfo.BelongsToForeignField].Name

			// Set field names on query so Join logic can use it.
			// Attention: ususally these fields contain the converted names,
			// but join logic requires the field names so we abuse those
			// RelatedQuery fields here.
			q.SetJoinFieldName(joinField)
			q.SetForeignFieldName(relInfo.BelongsToForeignField)
		} else if relInfo.M2M {
			joinField = baseInfo.PkField
			foreignFieldName = relInfo.M2MCollection + "." + baseInfo.BackendName + "_" + baseInfo.GetPkField().BackendName

			joinTableRelationColumn := relatedInfo.BackendName + "_" + relatedInfo.GetPkField().BackendName
			relationColumn := relatedInfo.GetPkField().BackendName
			joinQ := RelQCustom(newQuery, q.GetRelationName(), relInfo.M2MCollection, joinTableRelationColumn, relationColumn, InnerJoin)
			newQuery.JoinQ(joinQ)
		}
	}

	vals := make([]interface{}, 0)
	for _, m := range baseModels {
		val, _ := GetStructFieldValue(m, joinField)
		vals = append(vals, val)
	}

	if len(vals) > 1 {
		newQuery = newQuery.FilterCond(foreignFieldName, "in", vals)
	} else {
		newQuery = newQuery.Filter(foreignFieldName, vals[0])
	}

	return newQuery, nil
}

/**
 * Convenience functions.
 */

func BackendPersistRelations(b Backend, info *ModelInfo, m interface{}) DbError {
	modelVal := reflect.ValueOf(m)
	if modelVal.Type().Kind() == reflect.Ptr {
		modelVal = modelVal.Elem()
	}

	for name := range info.FieldInfo {
		fieldInfo := info.FieldInfo[name]

		// Handle has-one.
		if fieldInfo.HasOne {
			relationVal := modelVal.FieldByName(name)
			relationKind := relationVal.Type().Kind()

			if !relationVal.IsValid() || (relationKind == reflect.Ptr && relationVal.IsNil()) {
				continue
			}

			if relationKind == reflect.Ptr {
				relationVal = relationVal.Elem()
			}

			relation := relationVal.Addr().Interface()

			if IsZero(relation) {
				continue
			}

			// Auto-persist related model if neccessary.
			if IsZero(b.MustModelID(relation)) {
				err := b.Create(relation)
				if err != nil {
					return err
				}
			}

			// Update the hasOne field.
			key, _ := GetStructFieldValue(relation, fieldInfo.HasOneForeignField)
			modelVal.FieldByName(fieldInfo.HasOneField).Set(reflect.ValueOf(key))
		}

		// Handle belongs-to.
		if fieldInfo.BelongsTo {
			belongsToKey, _ := GetStructFieldValue(m, fieldInfo.BelongsToField)

			// belongsto relationships can only be handled if the model itself has an id
			// already. So skip if otherwise.
			if IsZero(belongsToKey) {
				continue
			}

			relationCollection := fieldInfo.RelationCollection
			foreignFieldName := b.ModelInfo(relationCollection).GetField(fieldInfo.BelongsToForeignField).BackendName

			items := make([]reflect.Value, 0)

			if !fieldInfo.RelationIsMany {
				items = append(items, modelVal.FieldByName(name))
			} else {
				// Code is same as above, but handling each relation item in the slice.
				sliceVal := modelVal.FieldByName(name)
				if !sliceVal.IsValid() {
					continue
				}

				for i := 0; i < sliceVal.Len(); i++ {
					items = append(items, sliceVal.Index(i))
				}
			}

			for _, relationVal := range items {
				relationKind := relationVal.Type().Kind()
				if !relationVal.IsValid() || (relationKind == reflect.Ptr && relationVal.IsNil()) {
					continue
				}

				if relationKind == reflect.Ptr {
					relationVal = relationVal.Elem()
				}

				relation := relationVal.Addr().Interface()
				relationId := b.MustModelID(relation)

				foreignField := relationVal.FieldByName(fieldInfo.BelongsToForeignField)
				if reflect.DeepEqual(foreignField.Interface(), belongsToKey) && !IsZero(relationId) {
					// Relation is persisted and has the right id, so nothing to do.
					continue
				}

				// Update key on relation.
				foreignField.Set(reflect.ValueOf(belongsToKey))

				// Auto-persist related model if neccessary.
				if IsZero(relationId) {
					err := b.Create(relation)
					if err != nil {
						return err
					}
				} else {
					// Relation already exists! Just update the foreign field.
					err := b.UpdateByMap(relation, map[string]interface{}{
						foreignFieldName: belongsToKey,
					})
					if err != nil {
						return err
					}
				}
			}
		} else if fieldInfo.M2M {
			// m2m relationships can only be handled if the model itself has an id
			// already. So skip if otherwise.
			if IsZero(b.MustModelID(m)) {
				continue
			}

			val, _ := GetStructFieldValue(m, fieldInfo.Name)
			if IsZero(val) {
				continue
			}

			models, err := ConvertInterfaceToSlice(val)
			if err != nil {
				return Error{
					Code:    "invalid_m2m_slice",
					Message: err.Error(),
				}
			}

			// First, persist all unpersisted m2m models.
			for _, model := range models {
				id := b.MustModelID(model)
				if IsZero(id) {
					if err := b.Create(model); err != nil {
						return err
					}
				}
			}

			m2m, err2 := b.M2M(m, fieldInfo.Name)
			if err2 != nil {
				return err2
			}
			err2 = m2m.Add(models...)
			if err2 != nil {
				return err2
			}
		}
	}

	return nil
}

func BackendQuery(b Backend, query Query, targetSlice []interface{}, models []interface{}, err DbError) ([]interface{}, DbError) {
	if err != nil {
		return nil, err
	}

	if len(models) < 1 {
		return nil, nil
	}

	// Call AfterQuery hook.
	for _, m := range models {
		CallModelHook(b, m, "AfterQuery")
	}

	// If a target slice was specified, build up a new slice
	// with the correct type, fill it, and set the pointer.
	if len(targetSlice) > 0 {
		SetSlicePointer(targetSlice[0], models)
	}

	return models, nil
}

func BackendQueryOne(b Backend, q Query, targetModels []interface{}) (interface{}, DbError) {
	res, err := b.Query(q)
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, nil
	}

	m := res[0]

	if len(targetModels) > 0 {
		SetPointer(targetModels[0], m)
	}

	return m, nil
}

func BackendLast(b Backend, q Query, targetModels []interface{}) (interface{}, DbError) {
	orders := q.GetOrders()
	orderLen := len(q.GetOrders())
	if orderLen > 0 {
		for i := 0; i < orderLen; i++ {
			orders[i].Ascending = !orders[i].Ascending
		}
	} else {
		info := b.ModelInfo(q.GetCollection())
		q = q.Order(info.GetPkName(), false)
	}

	model, err := b.QueryOne(q.Limit(1))
	if err != nil {
		return nil, err
	}

	if len(targetModels) > 0 {
		SetPointer(targetModels[0], model)
	}

	return model, nil
}

func BackendFindOne(b Backend, modelType string, id interface{}, targetModel []interface{}) (interface{}, DbError) {
	info := b.ModelInfo(modelType)
	if info == nil {
		return nil, Error{
			Code:    "model_type_not_found",
			Message: fmt.Sprintf("Model type '%v' not registered with backend GORM", modelType),
		}
	}

	pkField := info.FieldInfo[info.PkField]

	if reflect.TypeOf(id) != pkField.Type {
		converted, err := Convert(id, pkField.Type)
		if err != nil {
			return nil, Error{
				Code:    "id_conversion_error",
				Message: err.Error(),
			}
		}
		id = converted
	}

	model, err := b.Q(modelType).Filter(info.PkField, id).First()
	if err != nil {
		return nil, err
	}

	if len(targetModel) > 0 {
		SetPointer(targetModel[0], model)
	}

	return model, nil
}

func BackendFindBy(b Backend, modelType, field string, value interface{}, targetSlice []interface{}) ([]interface{}, DbError) {
	return b.Q(modelType).Filter(field, value).Find(targetSlice...)
}

func BackendFindOneBy(b Backend, modelType, field string, value interface{}, targetModel []interface{}) (interface{}, DbError) {
	res, err := b.Q(modelType).Filter(field, value).First()
	if err != nil {
		return nil, err
	}

	if len(targetModel) > 0 {
		SetPointer(targetModel[0], res)
	}

	return res, nil
}

/**
 * Join logic.
 */

func BackendDoJoins(b Backend, model string, objs []interface{}, joins []RelationQuery) DbError {
	for _, joinQ := range joins {
		// With a specific join type, joins should be handled by the backend itself.
		if joinQ.GetJoinType() != "" {
			continue
		}

		err := doJoin(b, model, objs, joinQ)
		if err != nil {
			return err
		}
	}

	return nil
}

func doJoin(b Backend, model string, objs []interface{}, joinQ RelationQuery) DbError {
	resultQuery, err := BuildRelationQuery(b, objs, joinQ)
	if err != nil {
		return err
	}

	res, err := resultQuery.Find()
	if err != nil {
		return err
	}

	if len(res) > 0 {
		assignJoinModels(objs, res, joinQ.GetRelationName(), joinQ.GetJoinFieldName(), joinQ.GetForeignFieldName())
	}

	return nil
}

func assignJoinModels(objs, joinedModels []interface{}, targetField, joinedField, joinField string) {
	mapper := make(map[interface{}][]interface{})
	for _, model := range joinedModels {
		val, _ := GetStructFieldValue(model, joinField)
		mapper[val] = append(mapper[val], model)
	}

	for _, model := range objs {
		val, _ := GetStructFieldValue(model, joinedField)
		if joins, ok := mapper[val]; ok && len(joins) > 0 {
			SetStructModelField(model, targetField, joins)
		}
	}
}
