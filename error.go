package dukedb

import (
	"fmt"
)

type Error struct {
	Code    string      `json:"code,omitempty"`
	Message string      `json:"title,omitempty"`
	Data    interface{} `json:"-"`
	Internal bool
	Errors []error `json:"-"`
}

func (e Error) GetCode() string {
	return e.Code
}

func (e Error) GetMessage() string {
	return e.Message
}

func (e Error) GetData() interface{} {
	return e.Data
}

func (e Error) IsInternal() bool {
	return e.Internal
}

func (e Error) GetErrors() []error {
	return e.Errors
}

func (e Error) AddError(err error) {
	e.Errors = append(e.Errors, err)
}

func (e Error) Error() string {
	s := e.Code
	if e.Message != "" {
		s += ": " + e.Message
	}

	if e.Data != nil {
		s += "\n" + fmt.Sprintf("%+v", e.Data)
	}

	return s
}
