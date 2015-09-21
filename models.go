package dukedb

import (
	"strconv"
)

// A base model with a string id that can be embedded.
type BaseModel struct {
	ID string
}

func (m *BaseModel) GetID() string {
	return m.ID
}

func (m *BaseModel) SetID(x string) error {
	m.ID = x
	return nil
}

// A base model with an integer id that can be embedded.
type BaseIntModel struct {
	ID uint64
}

func (b BaseIntModel) GetID() string {
	if b.ID == 0 {
		return ""
	}
	return strconv.FormatUint(b.ID, 10)
}

func (b *BaseIntModel) SetID(rawId string) error {
	if rawId == "" {
		b.ID = 0
		return nil
	}
	id, err := strconv.ParseUint(rawId, 10, 64)
	if err != nil {
		return err
	}
	b.ID = id
	return nil
}
