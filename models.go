package dukedb

import (
	"fmt"
)

type BaseModel struct {
	backend Backend
	info    *ModelInfo
}

// Ensure  BaseModel implements Model interface.
var _ Model = (*BaseModel)(nil)

func (m *BaseModel) Info() *ModelInfo {
	return m.info
}

func (m *BaseModel) SetInfo(info *ModelInfo) {
	m.info = info
}

func (m *BaseModel) Backend() Backend {
	return m.backend
}

func (m *BaseModel) SetBackend(backend Backend) {
	m.backend = backend
}

func (m *BaseModel) Collection() string {
	return m.info.Collection
}

func (m *BaseModel) BackendName() string {
	return m.info.BackendName
}

func (m *BaseModel) GetID() interface{} {
	id, _ := GetModelID(m.info, m)
	return id
}

func (m *BaseModel) SetID(id interface{}) error {
	return SetStructField(m, m.info.PkField, id)
}

func (m *BaseModel) GetStrID() string {
	return fmt.Sprint(m.GetID())
}

func (m *BaseModel) SetStrID(id string) error {
	convertedId, err := Convert(id, m.info.GetField(m.info.PkField).Type)
	if err != nil {
		return err
	}
	return m.SetID(convertedId)
}

func (m *BaseModel) Create() DbError {
	return m.backend.Create(m)
}

func (m *BaseModel) Update() DbError {
	return m.backend.Update(m)
}

func (m *BaseModel) Delete() DbError {
	return m.backend.Delete(m)
}

type ModelWrap struct {
	BaseModel

	model interface{}
}

func (m *ModelWrap) Model() interface{} {
	return m.model
}

func (m *ModelWrap) SetModel(model interface{}) {
	m.model = model
}

func (m *ModelWrap) GetID() interface{} {
	id, _ := GetModelID(m.info, m.model)
	return id
}

func (m *ModelWrap) SetID(id interface{}) error {
	return SetStructField(m.model, m.info.PkField, id)
}

func (m *ModelWrap) GetStrID() string {
	return fmt.Sprint(m.GetID())
}

func (m *ModelWrap) SetStrID(id string) error {
	convertedId, err := Convert(id, m.info.GetField(m.info.PkField).Type)
	if err != nil {
		return err
	}
	return m.SetID(convertedId)
}
