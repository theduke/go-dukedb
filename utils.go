package dukedb

import (
	//"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/theduke/go-apperror"
)

/**
 * String utils.
 */

func Pluralize(str string) string {
	if str[len(str)-1] == 'y' {
		str = str[1:len(str)-1] + "ie"
	}

	if str[len(str)-1] != 's' {
		str += "s"
	}

	return str
}

func StrBeforeFirst(str, separator string) string {
	pos := strings.Index(str, separator)
	if pos != -1 {
		return str[:pos]
	}
	return str
}

func StrAfterFirst(str, separator string) string {
	pos := strings.Index(str, separator)
	if pos != -1 {
		return str[pos+1:]
	}
	return str
}

func StrBeforeLast(str, separator string) string {
	pos := strings.LastIndex(str, separator)
	if pos != -1 {
		return str[:pos]
	}
	return str
}

func StrAfterLast(str, separator string) string {
	pos := strings.LastIndex(str, separator)
	if pos != -1 {
		return str[pos+1:]
	}
	return str
}

// Convert a CamelCase string to underscore version, eg camel_case.
func CamelCaseToUnderscore(str string) string {
	u := ""

	didChange := false
	for i, c := range str {
		if c >= 65 && c <= 90 {
			if i == 0 {
				u += string(byte(c + 32))
				didChange = true
				continue
			}

			if !didChange {
				u += "_"
				didChange = true
			}
			u += string(byte(c + 32))
		} else {
			u += string(byte(c))
			didChange = false
		}
	}

	return u
}

func LowerCaseFirst(str string) string {
	if len(str) == 0 {
		return ""
	}

	newStr := ""

	doReplace := true
	for _, c := range str {
		x := int(c)
		if doReplace {
			if x >= 65 && x <= 90 {
				newStr += string(x + 32)
			} else {
				doReplace = false
				newStr += string(c)
			}
		} else {
			newStr += string(c)
		}
	}

	return newStr
}

/**
 * Helpers for creating and converting structs and slices.
 */

// Set a pointer to a value with reflect.
// If the value is a pointer to a type, and the the pointer target is not,
// the value is automatically dereferenced.
func SetPointer(ptr, val interface{}) {
	ptrVal := reflect.ValueOf(ptr)
	ptrType := ptrVal.Type()
	if ptrType.Kind() != reflect.Ptr {
		panic("Pointer expected")
	}

	target := ptrVal.Elem()
	targetType := target.Type()

	value := reflect.ValueOf(val)
	valueType := value.Type()

	if valueType.Kind() == reflect.Ptr && targetType.Kind() != reflect.Ptr {
		value = value.Elem()
	}

	target.Set(value)
}

func SetSlicePointer(ptr interface{}, values []interface{}) {
	target := reflect.ValueOf(ptr)
	if target.Type().Kind() != reflect.Ptr {
		panic("Must  supply pointer to slice")
	}

	slice := target.Elem()
	sliceType := slice.Type()
	if sliceType.Kind() != reflect.Slice {
		panic("Must supply pointer to slice")
	}

	usePtr := sliceType.Elem().Kind() == reflect.Ptr

	for _, val := range values {
		if usePtr {
			slice = reflect.Append(slice, reflect.ValueOf(val))
		} else {
			slice = reflect.Append(slice, reflect.ValueOf(val).Elem())
		}
	}

	target.Elem().Set(slice)
}

func GetModelCollection(model interface{}) (string, apperror.Error) {
	// If the model  implements .Collection(), call it.
	if hook, ok := model.(ModelCollectionHook); ok {
		collection := hook.Collection()
		if collection != "" {
			return collection, nil
		}
	}

	typ := reflect.TypeOf(model)

	// Dereference pointer.
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	// Check if it is a struct.
	if typ.Kind() != reflect.Struct {
		return "", &apperror.Err{
			Code:    "invalid_model",
			Message: fmt.Sprintf("Expected model struct or pointer to struct, got %v", typ),
		}
	}

	collection := CamelCaseToUnderscore(typ.Name())
	collection = Pluralize(collection)

	return collection, nil
}

func MustGetModelCollection(model interface{}) string {
	collection, err := GetModelCollection(model)
	if err != nil {
		panic("Could not determine collection: " + err.Error())
	}

	return collection
}

/*
func GetModelSliceFieldValues(models []interface{}, fieldName string) ([]interface{}, apperror.Error) {
	vals := make([]interface{}, 0)

	for _, model := range models {
		val, err := GetStructFieldValue(model, fieldName)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}

	return vals, nil
}


// Given a struct, set the specified field that contains either a single Model
// or a model slice to the given models.
// If the target field type is struct or pointer to struct, it will be set to
// the first model in []models.
// If it is a slice, it will be set to the models with the correct type.
func SetStructModelField(obj interface{}, fieldName string, models []interface{}) error {
	objVal := reflect.ValueOf(obj)

	if objVal.Type().Kind() != reflect.Ptr {
		return errors.New("pointer_expected")
	}
	if objVal.Elem().Type().Kind() != reflect.Struct {
		return errors.New("pointer_to_struct_expected")
	}

	field := objVal.Elem().FieldByName(fieldName)
	if !field.IsValid() {
		return errors.New("unknown_field")
	}

	fieldType := field.Type().Kind()

	// TODO: in each clause, check that the target field conforms to the vale to set.
	if fieldType == reflect.Struct {
		field.Set(reflect.ValueOf(models[0]).Elem())
	} else if fieldType == reflect.Ptr {
		ptr := reflect.New(reflect.ValueOf(models[0]).Type())
		ptr.Elem().Set(reflect.ValueOf(models[0]))
		field.Set(ptr.Elem())
	} else if fieldType == reflect.Slice {
		sliceType := field.Type().Elem()
		slice := reflect.MakeSlice(reflect.SliceOf(sliceType), 0, 0)

		for _, model := range models {
			val := reflect.ValueOf(model)
			if val.Type().Kind() == reflect.Ptr {
				val = val.Elem()
			}

			if sliceType.Kind() == reflect.Ptr {
				slice = reflect.Append(slice, val.Addr())
			} else {
				slice = reflect.Append(slice, val)
			}
		}

		field.Set(slice)
	} else {
		return errors.New("unsupported_field_type")
	}

	return nil
}

func ModelToJson(info ModelInfo, model interface{}, includeRelations bool) ([]byte, apperror.Error) {
	if info == nil {
		var err apperror.Error
		info, err = BuildModelInfo(model)
		if err != nil {
			return nil, err
		}
	}

	data, err := ModelToMap(info, model, false, true, includeRelations)
	if err != nil {
		return nil, err
	}

	js, err2 := json.Marshal(data)
	if err2 != nil {
		return nil, &apperror.Err{
			Code:    "json_marshal_error",
			Message: err2.Error(),
		}
	}

	return js, nil
}

// ModelFieldDiff compares two models and returns a list of fields that are different.
func ModelFieldDiff(info ModelInfo, m1, m2 interface{}) []string {
	m1Data, _ := ModelToMap(info, m1, false, false, false)
	m2Data, _ := ModelToMap(info, m2, false, false, false)

	diff := make([]string, 0)
	for key, m1Val := range m1Data {
		if m2Val, ok := m2Data[key]; ok {
			if m1Val != m2Val {
				diff = append(diff, key)
			}
		}
	}

	return diff
}

func BuildModelFromMap(info ModelInfo, data map[string]interface{}) (interface{}, apperror.Error) {
	model, err := NewStruct(info.Item)
	if err != nil {
		return nil, &apperror.Err{
			Code:    "model_build_error",
			Message: err.Error(),
		}
	}

	err = UpdateModelFromData(info, model, data)
	if err != nil {
		return nil, &apperror.Err{
			Code:    "model_update_error",
			Message: err.Error(),
		}
	}

	return model, nil
}

func UpdateModelFromData(info ModelInfo, obj interface{}, data map[string]interface{}) apperror.Error {
	ptrVal := reflect.ValueOf(obj)
	if ptrVal.Type().Kind() != reflect.Ptr {
		return &apperror.Err{
			Code: "pointer_expected",
		}
	}
	val := ptrVal.Elem()
	if val.Type().Kind() != reflect.Struct {
		return &apperror.Err{
			Code: "pointer_to_struct_expected",
		}
	}

	for key := range data {
		// Try to find field by backend name.
		fieldInfo := info.FindAttribute(key)

		if fieldInfo == nil {
			continue
		}

		// Need special handling for point type.
		if strings.HasSuffix(fieldInfo.StructName(), "go-dukedb.Point") {
			p := new(Point)
			_, err := fmt.Sscanf(data[key].(string), "(%f,%f)", &p.Lat, &p.Lon)
			if err != nil {
				return &apperror.Err{
					Code:    "point_conversion_error",
					Message: fmt.Sprintf("Could not parse point specification: %v", data[key]),
				}
			}
			if fieldInfo.Type().Kind() == reflect.Ptr {
				data[key] = p
			} else {
				data[key] = *p
			}
		}

		// Handle marshalled fields.
		if fieldInfo.BackendMarshal() {
			var marshalledData []byte

			if strVal, ok := data[key].(string); ok {
				if strVal != "" {
					marshalledData = []byte(strVal)
				}
			} else if bytes, ok := data[key].([]byte); ok {
				if len(bytes) > 0 {
					marshalledData = bytes
				}
			}

			if marshalledData != nil {
				itemVal := reflect.New(fieldInfo.Type())
				itemPtr := itemVal.Interface()

				if err := json.Unmarshal(marshalledData, itemPtr); err != nil {
					return apperror.Wrap(err,
						"marshal_field_unmarshal_error",
						fmt.Sprintf("Could not unmarshal the content of field %v", fieldInfo.Name))
				}

				data[key] = itemVal.Elem().Interface()
			} else {
				continue
			}
		}

		SetModelValue(fieldInfo, val.FieldByName(fieldInfo.Name()), data[key])
	}

	return nil
}

func BuildModelSliceFromMap(info ModelInfo, items []map[string]interface{}) (interface{}, apperror.Error) {
	slice := NewSlice(info.Item())

	sliceVal := reflect.ValueOf(slice)

	for _, data := range items {
		model, err := BuildModelFromMap(info, data)
		if err != nil {
			return nil, err
		}
		sliceVal = reflect.Append(sliceVal, reflect.ValueOf(model))
	}

	return sliceVal.Interface(), nil
}
*/

/**
 * Model hooks.
 */
/*

 */

func CallModelHook(b Backend, m interface{}, hook string) apperror.Error {
	switch hook {
	case "Validate":
		if h, ok := m.(ModelValidateHook); ok {
			err := h.Validate()
			if err == nil {
				return nil
			} else if apperr, ok := err.(apperror.Error); ok {
				return apperr
			} else {
				return apperror.Wrap(err, "validation_error")
			}
		}
		return nil
	case "BeforeCreate":
		if h, ok := m.(ModelBeforeCreateHook); ok {
			err := h.BeforeCreate(b)
			if err == nil {
				return nil
			} else if apperr, ok := err.(apperror.Error); ok {
				return apperr
			} else {
				return apperror.Wrap(err, "before_create_error")
			}
		}
		return nil
	case "AfterCreate":
		if h, ok := m.(ModelAfterCreateHook); ok {
			h.AfterCreate(b)
		}
		return nil
	case "BeforeUpdate":
		if h, ok := m.(ModelBeforeUpdateHook); ok {
			err := h.BeforeUpdate(b)
			if err == nil {
				return nil
			} else if apperr, ok := err.(apperror.Error); ok {
				return apperr
			} else {
				return apperror.Wrap(err, "before_update_error")
			}
		}
		return nil
	case "AfterUpdate":
		if h, ok := m.(ModelAfterUpdateHook); ok {
			h.AfterUpdate(b)
		}
		return nil
	case "BeforeDelete":
		if h, ok := m.(ModelBeforeDeleteHook); ok {
			err := h.BeforeDelete(b)
			if err == nil {
				return nil
			} else if apperr, ok := err.(apperror.Error); ok {
				return apperr
			} else {
				return apperror.Wrap(err, "before_create_error")
			}
		}
		return nil
	case "AfterDelete":
		if h, ok := m.(ModelAfterDeleteHook); ok {
			h.AfterDelete(b)
		}
		return nil
	case "AfterQuery":
		if h, ok := m.(ModelAfterQueryHook); ok {
			h.AfterQuery(b)
		}
		return nil
	default:
		return &apperror.Err{
			Code:    "invalid_hook",
			Message: fmt.Sprintf("Unknown hook %v", hook),
		}
	}
}
