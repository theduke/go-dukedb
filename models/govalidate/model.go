package govalidate

import (
	"github.com/asaskevich/govalidator"

	"github.com/theduke/go-apperror"
)

type Model struct{}

func (m Model) Validate() apperror.Error {
	ok, err := govalidator.ValidateStruct(m)
	if ok {
		return nil
	}

	if errs, ok := err.(govalidator.Errors); ok {
		return &apperror.Err{
			Code:   "validation_error",
			Errors: errs,
		}
	}

	return &apperror.Err{
		Code:    "validation_error",
		Message: err.Error(),
		Errors:  []error{err},
	}
}
