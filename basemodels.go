package dukedb

import (
	"fmt"
	"strconv"
	"time"
)

/**
 * Base model.
 */

// Base model that can be embedded into your own models.
// Embedding improves performance, and enables to call .Create(),
// .Update() and .Delete() directly on the model.
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
	if m.info != nil {
		return m.info.Collection
	}
	return ""
}

func (m *BaseModel) BackendName() string {
	if m.info != nil {
		return m.info.BackendName
	}
	return ""
}

func (m *BaseModel) GetID() interface{} {
	if m.info == nil {
		panic("Model.info is not set")
	}

	id, _ := GetModelID(m.info, m)
	return id
}

func (m *BaseModel) SetID(id interface{}) error {
	if m.info == nil {
		panic("Model.info is not set")
	}

	convertedId, err := Convert(id, m.info.GetField(m.info.PkField).Type)
	if err != nil {
		return err
	}

	return SetStructField(m, m.info.PkField, convertedId)
}

func (m *BaseModel) GetStrID() string {
	return fmt.Sprint(m.GetID())
}

func (m *BaseModel) SetStrID(id string) error {
	if m.info == nil {
		panic("Model.info is not set")
	}
	return m.SetID(id)
}

/**
 * StrIDModel.
 */

// Base model with a string ID.
type StrIDModel struct {
	BaseModel
	ID string
}

func (m *StrIDModel) GetID() interface{} {
	return m.ID
}

func (m *StrIDModel) SetID(id interface{}) error {
	if strId, ok := id.(string); ok {
		m.ID = strId
		return nil
	}

	convertedId, err := Convert(id, "")
	if err != nil {
		return err
	}
	m.ID = convertedId.(string)
	return nil
}

func (m *StrIDModel) GetStrID() string {
	return m.ID
}

func (m *StrIDModel) SetStrID(rawId string) error {
	m.ID = rawId
	return nil
}

/**
 * IntIDModel.
 */

// Base model with a integer ID.
type IntIDModel struct {
	BaseModel
	ID uint64
}

func (m *IntIDModel) GetID() interface{} {
	return m.ID
}

func (m *IntIDModel) SetID(id interface{}) error {
	if intId, ok := id.(uint64); ok {
		m.ID = intId
		return nil
	}

	convertedId, err := Convert(id, uint64(0))
	if err != nil {
		return err
	}
	m.ID = convertedId.(uint64)
	return nil
}

func (m *IntIDModel) GetStrID() string {
	if m.ID == 0 {
		return ""
	}
	return strconv.FormatUint(m.ID, 10)
}

func (m *IntIDModel) SetStrID(rawId string) error {
	id, err := strconv.ParseUint(rawId, 10, 64)
	if err != nil {
		return err
	}

	m.ID = id
	return nil
}

/**
 * Timestamped model with createdAt and UpdatedAt.
 */

type TimeStampedModel struct {
	CreatedAt time.Time `db:"not-null"`
	UpdatedAt time.Time `db:"not-null"`
}

func (m *TimeStampedModel) BeforeCreate(b Backend) error {
	m.CreatedAt = time.Now()
	m.UpdatedAt = time.Now()
	return nil
}

func (m *TimeStampedModel) BeforeUpdate(b Backend) error {
	m.UpdatedAt = time.Now()
	return nil
}
