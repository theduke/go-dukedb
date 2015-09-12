package dukedb

import (
	"fmt"
	"reflect"
)

type BaseM2MCollection struct {
	Name string
	Items []Model	
}


func (c BaseM2MCollection) Count() int {
	return len(c.Items)
}

func (c BaseM2MCollection) Contains(m Model) bool {
	return c.GetByID(m.GetID()) != nil
}

func (c BaseM2MCollection) ContainsID(id string) bool {
	return c.GetByID(id) != nil
}

func (c BaseM2MCollection) All() []Model {
	return c.Items
}

func (c BaseM2MCollection) GetByID(id string) Model {
	for _, item := range c.Items {
		if item.GetID() == id {
			return item
		}
	}

	return nil
}

type BaseBackend struct {
	Debug bool
	ModelInfo map[string]*ModelInfo
}

func(b *BaseBackend) GetDebug() bool {
	return b.Debug
}

func(b *BaseBackend) SetDebug(x bool) {
	fmt.Printf("setting debuguu in base to: %v\n", x)
	b.Debug = x
}

func (b *BaseBackend) RegisterModel(m Model) error {
	info, err := NewModelInfo(m)
	if err != nil {
		panic(fmt.Sprintf("Could not register model '%v': %v\n", m.Collection(), err))
	}

	b.ModelInfo[m.Collection()] = info
	return nil
}

func (b BaseBackend) GetModelInfo(name string) *ModelInfo {
	info, _ := b.ModelInfo[name]
	return info
}

func (b BaseBackend) GetAllModelInfo() map[string]*ModelInfo {
	return b.ModelInfo
}

func (b BaseBackend) BuildRelationshipInfo() {
	BuildAllRelationInfo(b.ModelInfo)
}

func (b BaseBackend) HasModel(name string) bool {
	_, ok := b.ModelInfo[name]
	return ok
}

func (b BaseBackend) NewModel(name string) (interface{}, DbError) {
	info, ok := b.ModelInfo[name]
	if !ok {
		return nil, Error{
			Code: "model_type_not_found", 
			Message: fmt.Sprintf("Model type '%v' not registered with backend GORM", name),
		}
	}

	item, err := NewStruct(info.Item)
	if err != nil {
		return nil, Error{Code: err.Error(), Message: "Could not build new struct"}
	}

	return item, nil
}

func (b BaseBackend) NewModelSlice(name string) (interface{}, DbError) {
	info, ok := b.ModelInfo[name]
	if !ok {
		return nil, Error{
			Code: "model_type_not_found", 
			Message: fmt.Sprintf("Model type '%v' not registered with backend GORM", name),
		}
	}

	return NewSlice(info.Item), nil
}

// Relationship stuff.
func BuildRelationQuery(b Backend, baseModels []Model, q *RelationQuery) (*Query, DbError) {
	baseQ := q.BaseQuery
	baseInfo := b.GetModelInfo(baseQ.Model)
	if baseInfo == nil {
		panic(fmt.Sprintf("Model %v not registered with backend", baseQ.Model))
	}

	if baseModels == nil {
		var err DbError
		baseModels, err = baseQ.Find()
		if err != nil {
			return nil, err
		}

		if len(baseModels) == 0 {
			return nil, Error{
				Code: "relation_on_empty_result",
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

	if q.Model != "" && q.JoinFieldName != "" {
		// Custom relation query.
		targetModelName = q.Model
		joinField = baseInfo.MapFieldName(q.JoinFieldName)
		foreignFieldName = q.ForeignFieldName

		if foreignFieldName == "" {
			// No foreign key specified, use primary key.			
			relatedInfo := b.GetModelInfo(targetModelName)
			foreignFieldName = relatedInfo.PkField
		}
	} else if q.RelationName != "" {
		// query for a known relationship reflectet in the model struct.
		relInfo, ok := baseInfo.FieldInfo[q.RelationName]
		if !ok {
			panic(fmt.Sprintf("Model %v has no relation to %v", baseQ.Model, q.RelationName))
		}
		targetModelName = relInfo.RelationItem.Collection()

		relatedInfo := b.GetModelInfo(targetModelName)

		if relInfo.HasOne {
			joinField = relInfo.HasOneField
			foreignFieldName = baseInfo.FieldInfo[relInfo.HasOneForeignField].Name

			// Set field names on query so Join logic can use it.
			// Attention: ususally these fields contain the converted names,
			// but join logic requires the field names so we abuse those 
			// RelatedQuery fields here.
			q.JoinFieldName = joinField
			q.ForeignFieldName = relInfo.HasOneForeignField
		} else if relInfo.BelongsTo {
			joinField = relInfo.BelongsToField
			foreignFieldName = relatedInfo.FieldInfo[relInfo.BelongsToForeignField].Name

			// Set field names on query so Join logic can use it.
			// Attention: ususally these fields contain the converted names,
			// but join logic requires the field names so we abuse those 
			// RelatedQuery fields here.
			q.JoinFieldName = joinField
			q.ForeignFieldName = relInfo.BelongsToForeignField
		}
	}

	vals := make([]interface{}, 0)
	for _, m := range baseModels {
		val, _ := GetStructFieldValue(m, joinField)
		vals = append(vals, val)
	}
	
	newQuery := b.Q(targetModelName)

	if len(vals) > 1 {
		newQuery = newQuery.FilterCond(foreignFieldName, "in", vals)
	}	else {
		newQuery = newQuery.Filter(foreignFieldName, vals[0])
	}

	return newQuery, nil
}

/**
 * Convenience functions.
 */

func BackendPersistRelations(b Backend, info *ModelInfo, m Model) DbError {
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

			relation := relationVal.Addr().Interface().(Model)

			// Auto-persist related model if neccessary.
			if IsZero(relation.GetID()) {
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

			foreignFieldName := b.GetModelInfo(fieldInfo.RelationItem.Collection()).FieldInfo[fieldInfo.BelongsToForeignField].BackendName

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

				relation := relationVal.Addr().Interface().(Model)

				foreignField := relationVal.FieldByName(fieldInfo.BelongsToForeignField)
				if reflect.DeepEqual(foreignField.Interface(), belongsToKey) && !IsZero(relation.GetID()) {
					// Relation is persisted and has the right id, so nothing to do.
					continue
				}

				// Update key on relation.
				foreignField.Set(reflect.ValueOf(belongsToKey))

				// Auto-persist related model if neccessary.
				if IsZero(relation.GetID()) {
					err := b.Create(relation)
					if err != nil {
						return err
					}
				} else {
					// Relation already exists! Just update the foreign field.
					err := b.UpdateByMap(relation, map[string]interface{}{
						foreignFieldName: belongsToKey ,
					})
					if err != nil {
						return err
					}
				}
			}
		} else if fieldInfo.M2M {
			// m2m relationships can only be handled if the model itself has an id
			// already. So skip if otherwise.
			if IsZero(m.GetID()) {
				continue
			}

			items, _ := GetStructFieldValue(m, fieldInfo.Name)
			if items == nil {
				continue
			}

			models, _ := InterfaceToModelSlice(items)
			if len(models) == 0 {
				continue
			}

			// First, persist all unpersisted m2m models.
			for _, model := range models {
				if IsZero(model.GetID()) {
					if err := b.Create(model); err != nil {
						return err
					}
				}
			}

			m2m, err := b.M2M(m, fieldInfo.Name)
			if err != nil {
				return err
			}
			err = m2m.Add(models...)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func BackendQueryOne(b Backend, q *Query) (Model, DbError) {
	res, err := b.Query(q)
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, nil
	}

	m := res[0]
	return m, nil
}

func BackendLast(b Backend, q *Query) (Model, DbError) {
	orders := len(q.Orders)
	if orders > 0 {
		for i := 0; i < orders; i++ {
			q.Orders[i].Ascending = !q.Orders[i].Ascending
		}
	} else {
		info := b.GetModelInfo(q.Model)
		q = q.Order(info.GetPkName(), false)
	}

	return b.QueryOne(q.Limit(1))
}

func BackendFindOne(b Backend, modelType string, id string) (Model, DbError) {
	info := b.GetModelInfo(modelType)
	if info == nil {
		return nil, Error{
			Code: "model_type_not_found", 
			Message: fmt.Sprintf("Model type '%v' not registered with backend GORM", modelType),
		}
	}

	val, err := ConvertStringToType(id, info.FieldInfo[info.PkField].Type)
	if err != nil {
		return nil, Error{Code: err.Error()}
	}

	return b.Q(modelType).Filter(info.FieldInfo[info.PkField].BackendName, val).First()
}

/**
 * Join logic.
 */

func BackendDoJoins(b Backend, model string, objs []Model, joins []*RelationQuery) DbError {
	for _, joinQ := range joins {
		// With a specific join type, joins should be handled by the backend itself.
		if joinQ.JoinType != "" {
			continue
		}

		err := doJoin(b, model, objs, joinQ)
		if err != nil {
			return err
		}
	}

	return nil
}

func doJoin(b Backend, model string, objs []Model, joinQ *RelationQuery) DbError {
	resultQuery, err := BuildRelationQuery(b, objs, joinQ)
	if err != nil {
		return err
	}

	res, err := resultQuery.Find()
	if err != nil {
		return err
	}

	if len(res) > 0 {
		assignJoinModels(objs, res, joinQ.RelationName, joinQ.JoinFieldName, joinQ.ForeignFieldName)
	}

	return nil
}

func assignJoinModels(objs, joinedModels []Model, targetField, joinedField, joinField string) {
	mapper := make(map[interface{}][]Model)
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

