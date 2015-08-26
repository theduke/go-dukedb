package dukedb

import(
	"reflect"
	"errors"
	"strings"
	"fmt"
	"strconv"
)


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

func GetStructFieldValue(s interface{}, fieldName string) (interface{}, error) {
	v := reflect.ValueOf(s).Elem()
	if v.Type().Kind() != reflect.Struct {
		return nil, errors.New("pointer_to_struct_expected")
	}

	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return nil, Error{
			Code: "field_not_found", 
			Message: fmt.Sprintf("struct does not have field '%v'", fieldName),
		}
	}	

	return field.Interface(), nil
}

func GetModelSliceFieldValues(models []Model, fieldName string) ([]interface{}, error) {
	vals := make([]interface{}, 0)

	for _, model := range models {
		val, err := GetStructFieldValue(model.(interface{}), fieldName)
		if err != nil {
			return nil, err
		}
		vals = append(vals, val)
	}

	return vals, nil
}

func FilterToCondition(filter string) string {
	typ := ""

	switch filter {
	case "eq":
		typ = "="
	case "neq":
		typ = "!="
	case "lt":
		typ = "<"
	case "lte":
		typ = "<="
	case "gt":
		typ = ">"
	case "gte":
		typ = ">="
	case "like":
		typ = "LIKE"
	case "in":
		typ = "in"
	default:
		panic(fmt.Sprintf("Unknown filter: '%v'", filter))
	}
	
	return typ	
}

func InterfaceToModelSlice(slicePtr interface{}) []Model {
	reflSlice := reflect.ValueOf(slicePtr).Elem()
	result := make([]Model, 0)

	for i := 0; i < reflSlice.Len(); i ++ {
		item := reflSlice.Index(i).Interface()
		result = append(result, item.(Model))
	}

	return result
}

func NewStruct(typ interface{}) interface{} {
	// Build new struct.
	item := reflect.ValueOf(typ).Elem().Interface()
	return reflect.New(reflect.TypeOf(item)).Interface()
}

func NewStructSlice(typ interface{}) interface{} {
	// Build new array.
	// See http://stackoverflow.com/questions/25384640/why-golang-reflect-makeslice-returns-un-addressable-value
	
	// Create a slice to begin with
	myType := reflect.TypeOf(typ)
	slice := reflect.MakeSlice(reflect.SliceOf(myType), 0, 0)

	// Create a pointer to a slice value and set it to the slice
	x := reflect.New(slice.Type())
	x.Elem().Set(slice)

	slicePointer := x.Interface()

	return slicePointer
}

func SetStructModelField(obj interface{}, fieldName string, models []Model) {
	objVal := reflect.ValueOf(obj)
	field := objVal.Elem().FieldByName(fieldName)
	fieldType := field.Type().Kind()

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
			if sliceType.Kind() == reflect.Struct {
				slice = reflect.Append(slice, reflect.ValueOf(model).Elem())	
			} else {
				slice = reflect.Append(slice, reflect.ValueOf(model))	
			}
		}

		field.Set(slice)
	}
}

func ModelFindPrimaryKey(model interface{}) (string, error) {
	if reflect.TypeOf(model).Kind() != reflect.Ptr {
		return "", errors.New("pointer_expected")
	}
	if reflect.ValueOf(model).Elem().Kind() != reflect.Struct {
		return "", errors.New("pointer_to_struct_expected")
	}

	item := reflect.ValueOf(model).Elem()
	typ := item.Type()

	var pkName string

	for i := 0; i < item.NumField(); i++ {
		//field := item.Field(i)
		fieldType := typ.Field(i)

		name := fieldType.Name
		lowerName := strings.ToLower(name)

		if lowerName == "id" {
			pkName = name
		}

		tag := fieldType.Tag.Get("db")
		if strings.Contains(tag, "primary_key") {
			pkName = name
			break
		}
	}

	if pkName == "" {
		// Try To Find pk in embedded structs.
		for i := 0; i < item.NumField(); i++ {
			fieldType := typ.Field(i)
			if fieldType.Type.Kind() == reflect.Struct && fieldType.Anonymous {
				return ModelFindPrimaryKey(item.Field(i).Addr().Interface())
			}
		}
		return "", errors.New("no_id_field_found")
	}

	return pkName, nil
}

func ConvertToType(value string, typ reflect.Kind) (interface{}, error) {
	switch typ {
	case reflect.Int:
		x, err := strconv.Atoi(value) 
		return interface{}(x), err
	case reflect.Int64:
		x, err := strconv.ParseInt(value, 10, 64)
		return interface{}(x), err
	case reflect.Uint64:
		x, err := strconv.ParseUint(value, 10, 64)
		return interface{}(x), err
	case reflect.String:
		return interface{}(value), nil
	default:
		return nil, errors.New(fmt.Sprintf("cannot_convert_to_%v", typ))
	}
}

type FieldInfo struct {
	PrimaryKey bool
	Ignore bool
	Name string
	Type reflect.Kind
}

func ParseFieldTag(tag string) *FieldInfo  {
	info := FieldInfo{}

	parts := strings.Split(tag, ";")
	for _, part := range parts {
		if part == "primary_key" {
			info.PrimaryKey = true
			continue
		} else if part == "-" {
			info.Ignore = true
			continue
		}

		itemParts := strings.Split(part, "=")
		if len(itemParts) == 2 {
			switch itemParts[0] {
			case "name":
				info.Name =  itemParts[1]
			}
		}
	}

	return &info
}

type ModelInfo struct {
	// Name of the struct field.
	PkField string

	Item interface{}

	FieldInfo map[string]*FieldInfo
}

func (m ModelInfo) GetPkName() string {
	return m.FieldInfo[m.PkField].Name
}

/**
 * Given a database field name, return the struct field name.
 */
func (m ModelInfo) MapFieldName(name string) string {
	for key := range m.FieldInfo {
		if key == name {
			return m.FieldInfo[key].Name
		}
	}

	return ""
}

func NewModelInfo(model Model) (*ModelInfo, error) {
	pkName, err := ModelFindPrimaryKey(model)
	if err != nil {
		return nil, err
	}

	info := ModelInfo{
		PkField: pkName,
		Item: model,
		FieldInfo: make(map[string]*FieldInfo),
	}

	modelVal := reflect.ValueOf(model).Elem()
	buildFieldInfo(&info, modelVal)

	return &info, nil
}

func buildFieldInfo(info *ModelInfo, modelVal reflect.Value) {
	modelType := modelVal.Type()

	for i := 0; i < modelVal.NumField(); i++ {
		field := modelVal.Field(i)
		fieldType := modelType.Field(i)

		if fieldType.Type.Kind() == reflect.Struct && fieldType.Anonymous {
			// Embedded struct. Find nested fields.
			buildFieldInfo(info, field)
			continue
		}

		fieldInfo := ParseFieldTag(fieldType.Tag.Get("db"))	
		if fieldInfo.Name == "" {
			fieldInfo.Name = CamelCaseToUnderscore(fieldType.Name)
		}

		fieldInfo.Type = fieldType.Type.Kind()

		info.FieldInfo[fieldType.Name] = fieldInfo
	}
}