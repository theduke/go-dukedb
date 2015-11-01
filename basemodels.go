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
type StrIDModel struct {
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

	convertedId, err := reflector.Reflect(id).ConvertTo("")
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

	convertedId, err := reflector.Reflect(id).ConvertTo(uint64(0))
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
