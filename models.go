package dukedb

import (
	"strconv"
)

type BaseModelIntID struct {
	ID uint64
}

func (b BaseModelIntID) GetID() string {
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
