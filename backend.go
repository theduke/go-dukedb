package dukedb

import (
	"fmt"
)

type BaseBackend struct {
	debug bool
	ModelInfo map[string]*ModelInfo
}

func(b *BaseBackend) Debug() bool {
	return b.debug
}

func(b *BaseBackend) SetDebug(x bool) {
	b.debug = x
}

func (b *BaseBackend) RegisterModel(m Model) error {
	info, err := NewModelInfo(m)
	if err != nil {
		panic(fmt.Sprintf("Could not register model '%v': %v\n", m.GetCollection(), err))
	}

	b.ModelInfo[m.GetCollection()] = info
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

	return NewStruct(info.Item), nil
}

func (b BaseBackend) NewModelSlice(name string) (interface{}, DbError) {
	info, ok := b.ModelInfo[name]
	if !ok {
		return nil, Error{
			Code: "model_type_not_found", 
			Message: fmt.Sprintf("Model type '%v' not registered with backend GORM", name),
		}
	}

	return NewStructSlice(info.Item), nil
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
		targetModelName = relInfo.RelationItem.GetCollection()

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

func BackendFindOne(b Backend, modelType string, id string) (Model, DbError) {
	info := b.GetModelInfo(modelType)
	if info == nil {
		return nil, Error{
			Code: "model_type_not_found", 
			Message: fmt.Sprintf("Model type '%v' not registered with backend GORM", modelType),
		}
	}

	val, err := ConvertToType(id, info.FieldInfo[info.PkField].Type)
	if err != nil {
		return nil, Error{Code: err.Error()}
	}

	return b.Q(modelType).Filter(info.FieldInfo[info.PkField].Name, val).First()
}

/**
 * Join logic.
 */

func BackendDoJoins(b Backend, model string, objs []Model, joins []*RelationQuery) DbError {
	for _, joinQ := range joins {
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

