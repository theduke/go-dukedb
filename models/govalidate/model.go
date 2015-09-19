package govalidate

import (
	"github.com/asaskevich/govalidator"

	db "github.com/theduke/go-dukedb"
)

type Model struct{}

func (m Model) Validate() db.DbError {
	ok, err := govalidator.ValidateStruct(m)
	if ok {
		return nil
	}

	if errs, ok := err.(govalidator.Errors); ok {
		return db.Error{
			Code:   "validation_error",
			Errors: errs,
		}
	}

	return db.Error{
		Code:    "validation_error",
		Message: err.Error(),
		Errors:  []error{err},
	}
}
