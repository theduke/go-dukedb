package dukedb

import (
	"strconv"
)

type BaseModel struct {
	ID uint64
}

func (b BaseModel) GetID() string {
	return strconv.FormatUint(b.ID, 64)
}

func (b BaseModel) SetID(rawId string) error {
	id, err := strconv.ParseUint(rawId, 10, 64)
	if err != nil {
		return err
	}
	b.ID = id
	return nil
}
