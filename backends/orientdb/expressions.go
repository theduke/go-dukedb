package orientdb

import (
	"reflect"

	"github.com/theduke/go-apperror"
	. "github.com/theduke/go-dukedb/expressions"
)

const (
	PROPERTY_ATTR_LINKEDCLASS = "LINKEDCLASS"
	PROPERTY_ATTR_LINKEDTYPE  = "LINKEDTYPE"
	PROPERTY_ATTR_MIN         = "MIN"
	PROPERTY_ATTR_MAX         = "MAX"
	PROPERTY_ATTR_MANDATORY   = "MANDATORY"
	PROPERTY_ATTR_NAME        = "NAME"
	PROPERTY_ATTR_NOTNULL     = "NOTNULL"
	PROPERTY_ATTR_REGEXP      = "REGEXP"
	PROPERTY_ATTR_TYPE        = "TYPE"
	PROPERTY_ATTR_COLLATE     = "COLLATE"
	PROPERTY_ATTR_READONLY    = "READONLY"
	PROPERTY_ATTR_CUSTOM      = "CUSTOM"
	PROPERTY_ATTR_DEFAULT     = "DEFAULT"
)

var propertyAttrMap map[string]bool = map[string]bool{
	"LINKEDCLASS": true,
	"LINKEDTYPE":  true,
	"MIN":         true,
	"MANDATORY":   true,
	"MAX":         true,
	"NAME":        true,
	"NOTNULL":     true,
	"REGEXP":      true,
	"TYPE":        true,
	"COLLATE":     true,
	"READONLY":    true,
	"CUSTOM":      true,
	"DEFAULT":     true,
}

type AlterPropertyStmt struct {
	collection string
	field      string
	attribute  string
	value      *ValueExpr
}

func (s *AlterPropertyStmt) Collection() string {
	return s.collection
}

func (s *AlterPropertyStmt) SetCollection(x string) {
	s.collection = x
}

func (s *AlterPropertyStmt) Field() string {
	return s.field
}

func (s *AlterPropertyStmt) SetField(x string) {
	s.field = x
}

func (s *AlterPropertyStmt) Attribute() string {
	return s.attribute
}

func (s *AlterPropertyStmt) SetAttribute(x string) {
	s.attribute = x
}

func (s *AlterPropertyStmt) Value() *ValueExpr {
	return s.value
}

func (s *AlterPropertyStmt) SetValue(x *ValueExpr) {
	s.value = x
}

func (s *AlterPropertyStmt) Validate() apperror.Error {
	if s.collection == "" {
		return apperror.New("empty_collection")
	} else if s.field == "" {
		return apperror.New("emtpy_field")
	} else if s.attribute == "" {
		return apperror.New("empty_attribute")
	} else if _, ok := propertyAttrMap[s.attribute]; !ok {
		return apperror.New("unknown_property_attribute", "Unknown property attribute %v", s.attribute)
	} else if s.value == nil {
		return apperror.New("empty_value")
	}

	return nil
}

func NewAlterPropertyStmt(collection, field, attribute string, value interface{}, typ ...reflect.Type) *AlterPropertyStmt {
	return &AlterPropertyStmt{
		collection: collection,
		field:      field,
		attribute:  attribute,
		value:      NewValueExpr(value, typ...),
	}
}
