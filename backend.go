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

func BackendDoJoins(b Backend, model string, objs []Model, joins []*Query) error {
	for _, joinQ := range joins {
		err := doJoin(b, model, objs, joinQ)
		if err != nil {
			return err
		}
	}

	return nil
}



func doJoin(b Backend, model string, objs []Model, joinQ *Query) error {
	info := b.GetModelInfo(model)
	joinInfo := b.GetModelInfo(joinQ.Model)
	if joinInfo == nil {
		return Error{
			Code: "invalid_join_unknown_model",
			Message: "The model " + joinQ.Model + " was not registered with backend GORM",
		}
	}

	// Determine field names for join.
	if joinQ.JoinField == "" {
		joinQ.JoinField = joinInfo.PkField
	}
	joinField := joinInfo.MapFieldName(joinQ.JoinField)
	
	if joinQ.JoinedField == "" {
		joinQ.JoinedField = info.PkField
	}	
	joinedField := info.MapFieldName(joinQ.JoinedField)

	vals, err := GetModelSliceFieldValues(objs, joinQ.JoinedField)
	if err != nil {
		return err
	}

	query := b.Q(joinQ.Model).FilterCond(joinInfo.FieldInfo[joinQ.JoinField].Name, "in", vals)
	if joinQ.Filters != nil {
		for _, filter := range joinQ.Filters {
			query = query.Query(filter)
		}
	}

	res, err := query.Find()
	if err != nil {
		return err
	}

	if len(res) > 0 {
		assignJoinModels(objs, res, joinQ.JoinTargetField, joinedField, joinField)
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
