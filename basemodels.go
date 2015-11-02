package dukedb

import (
	"strconv"
	"time"

	"github.com/theduke/go-reflector"
)

/**
 * StrIDModel.
 */

// Base model with a string ID.
type StrIdModel struct {
	Id string
}

func (m *StrIdModel) GetID() interface{} {
	return m.Id
}

func (m *StrIdModel) SetID(id interface{}) error {
	if strId, ok := id.(string); ok {
		m.Id = strId
		return nil
	}

	convertedId, err := reflector.Reflect(id).ConvertTo("")
	if err != nil {
		return err
	}
	m.Id = convertedId.(string)
	return nil
}

func (m *StrIdModel) GetStrID() string {
	return m.Id
}

func (m *StrIdModel) SetStrID(rawId string) error {
	m.Id = rawId
	return nil
}

/**
 * IntIDModel.
 */

// Base model with a integer ID.
type IntIdModel struct {
	Id uint64
}

func (m *IntIdModel) GetID() interface{} {
	return m.Id
}

func (m *IntIdModel) SetID(id interface{}) error {
	if intId, ok := id.(uint64); ok {
		m.Id = intId
		return nil
	}

	convertedId, err := reflector.Reflect(id).ConvertTo(uint64(0))
	if err != nil {
		return err
	}
	m.Id = convertedId.(uint64)
	return nil
}

func (m *IntIdModel) GetStrID() string {
	if m.Id == 0 {
		return ""
	}
	return strconv.FormatUint(m.Id, 10)
}

func (m *IntIdModel) SetStrID(rawId string) error {
	id, err := strconv.ParseUint(rawId, 10, 64)
	if err != nil {
		return err
	}

	m.Id = id
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
