package dukedb

import (
	"strconv"
)

// A base model with a string id that can be embedded.
type BaseModelStrID struct {
	ID string
}

func(m *BaseModelStrID) GetID() string {
	return m.ID
}

func(m *BaseModelStrID) SetID(x string) error {
	m.ID = x
	return nil
}


// A base model with an integer id that can be embedded.
type BaseModelIntID struct {
	ID uint64
}

func (b BaseModelIntID) GetID() string {
	if b.ID == 0 {
		return ""
	}
	return strconv.FormatUint(b.ID, 10)
}

func (b *BaseModelIntID) SetID(rawId string) error {
	id, err := strconv.ParseUint(rawId, 10, 64)
	if err != nil {
		return err
	}
	b.ID = id
	return nil
}
