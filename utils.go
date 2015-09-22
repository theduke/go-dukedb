package dukedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

/**
 * String utils.
 */

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

	lc := strings.ToLower(string(str[0]))
	if len(str) > 1 {
		lc += str[1:]
	}

	return lc
}

// Given the internal name of a filter like "eq" or "lte", return a SQL operator like = or <.
// WARNING: panics if an unsupported filter is given.
func FilterToSqlCondition(filter string) (string, DbError) {
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
		typ = "IN"
	default:
		return "", Error{
			Code:    "unknown_filter",
			Message: "Unknown filter '" + filter + "'",
		}
	}

	return typ, nil
}

/**
 * Generic interface variable handling/comparison functions.
 */

func IsNumericKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func IsZero(val interface{}) bool {
	reflVal := reflect.ValueOf(val)
	reflType := reflVal.Type()

	if reflType.Kind() == reflect.Slice {
		return reflVal.Len() < 1
	}

	return val == reflect.Zero(reflType).Interface()
}

func SaveConvert(val interface{}, typ reflect.Type) interface{} {
	defer func() {
		recover()
	}()

	return reflect.ValueOf(val).Convert(typ).Interface()
}

func Convert(value interface{}, typ reflect.Type) (interface{}, error) {
	kind := typ.Kind()

	valType := reflect.TypeOf(value)

	if valType.Kind() == kind {
		// Same kind, nothing to convert.
		return value, nil
	}

	// If target is string, just use fmt.
	if kind == reflect.String {
		return fmt.Sprintf("%v", value), nil
	}

	// If value is string, and target type is numeric,
	// parse to float and then convert with reflect.
	if valType.Kind() == reflect.String && IsNumericKind(kind) {
		num, err := strconv.ParseFloat(value.(string), 64)
		if err != nil {
			return nil, err
		}
		return reflect.ValueOf(num).Convert(typ).Interface(), nil
	}

	// No custom handling worked, so try to convert with reflect.
	// We have to accept the panic.
	converted := SaveConvert(value, typ)
	if converted == nil {
		return nil, errors.New(fmt.Sprintf("Cannot convert %v to %v", valType.String(), kind))
	}

	return converted, nil
}

// Convert a string value to the specified type if possible.
// Returns an error for unsupported types.
func ConvertStringToType(value string, typ reflect.Kind) (interface{}, error) {
	switch typ {
	case reflect.Int:
		x, err := strconv.Atoi(value)
		return interface{}(x), err
	case reflect.Int64:
		x, err := strconv.ParseInt(value, 10, 64)
		return interface{}(x), err
	case reflect.Uint:
		x, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return nil, err
		}
		return uint(x), nil
	case reflect.Uint64:
		x, err := strconv.ParseUint(value, 10, 64)
		return interface{}(x), err
	case reflect.String:
		return interface{}(value), nil
	default:
		return nil, errors.New(fmt.Sprintf("cannot_convert_to_%v", typ))
	}
}

func CompareValues(condition string, a, b interface{}) (bool, DbError) {
	typ := reflect.TypeOf(a).Kind()

	switch typ {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return CompareNumericValues(condition, a, b)
	case reflect.String:
		return CompareStringValues(condition, a, b)
	default:
		return false, Error{
			Code:    "unsupported_comparison_type",
			Message: fmt.Sprintf("Type %v can not be compared", typ),
		}
	}
}

func CompareStringValues(condition string, a, b interface{}) (bool, DbError) {
	aVal := a.(string)
	bVal := b.(string)

	// Check different possible filters.
	switch condition {
	case "eq":
		return aVal == bVal, nil
	case "neq":
		return aVal != bVal, nil
	case "like":
		return strings.Contains(aVal, bVal), nil
	case "lt":
		return aVal < bVal, nil
	case "lte":
		return aVal <= bVal, nil
	case "gt":
		return aVal > bVal, nil
	case "gte":
		return aVal >= bVal, nil

	default:
		return false, Error{
			Code:    "unknown_filter",
			Message: fmt.Sprintf("Unknown filter type '%v'", condition),
		}
	}
}

func CompareNumericValues(condition string, a, b interface{}) (bool, DbError) {
	typ := reflect.TypeOf(a).Kind()
	switch typ {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return CompareIntValues(condition, a, b)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return CompareUintValues(condition, a, b)
	case reflect.Float32, reflect.Float64:
		return CompareFloatValues(condition, a, b)
	default:
		return false, Error{
			Code: "unsupported_type_for_numeric_comparison",
			Message: fmt.Sprintf(
				"For a numeric comparision with %v, a numeric type is expected. Got: %v",
				condition, typ),
		}
	}
}

func NumericToInt64(x interface{}) (int64, DbError) {
	var val int64

	switch reflect.TypeOf(x).Kind() {
	case reflect.Int:
		val = int64(x.(int))
	case reflect.Int8:
		val = int64(x.(int8))
	case reflect.Int16:
		val = int64(x.(int16))
	case reflect.Int32:
		val = int64(x.(int32))
	case reflect.Int64:
		val = x.(int64)
	case reflect.Uint:
		val = int64(x.(uint))
	case reflect.Uint8:
		val = int64(x.(uint8))
	case reflect.Uint16:
		val = int64(x.(uint16))
	case reflect.Uint32:
		val = int64(x.(uint32))
	case reflect.Uint64:
		val = int64(x.(uint64))
	case reflect.Float32:
		val = int64(x.(float32))
	case reflect.Float64:
		val = int64(x.(float64))
	case reflect.String:
		x, err := strconv.ParseInt(x.(string), 10, 64)
		if err != nil {
			return int64(0), Error{Code: "non_numeric_string"}
		}
		val = x
	default:
		return int64(0), Error{Code: "non_numeric_type"}
	}

	return val, nil
}

func NumericToUint64(x interface{}) (uint64, DbError) {
	var val uint64

	switch reflect.TypeOf(x).Kind() {
	case reflect.Int:
		val = uint64(x.(int))
	case reflect.Int8:
		val = uint64(x.(int8))
	case reflect.Int16:
		val = uint64(x.(int16))
	case reflect.Int32:
		val = uint64(x.(int32))
	case reflect.Int64:
		val = uint64(x.(int64))
	case reflect.Uint:
		val = uint64(x.(uint))
	case reflect.Uint8:
		val = uint64(x.(uint8))
	case reflect.Uint16:
		val = uint64(x.(uint16))
	case reflect.Uint32:
		val = uint64(x.(uint32))
	case reflect.Uint64:
		val = x.(uint64)
	case reflect.Float32:
		val = uint64(x.(float32))
	case reflect.Float64:
		val = uint64(x.(float64))
	case reflect.String:
		x, err := strconv.ParseInt(x.(string), 10, 64)
		if err != nil {
			return uint64(0), Error{Code: "non_numeric_string"}
		}
		val = uint64(x)
	default:
		panic("nonnumeric")
		return uint64(0), Error{Code: "non_numeric_type"}
	}

	return val, nil
}

func NumericToFloat64(x interface{}) (float64, DbError) {
	var val float64

	switch reflect.TypeOf(x).Kind() {
	case reflect.Int:
		val = float64(x.(int))
	case reflect.Int8:
		val = float64(x.(int8))
	case reflect.Int16:
		val = float64(x.(int16))
	case reflect.Int32:
		val = float64(x.(int32))
	case reflect.Int64:
		val = float64(x.(int64))
	case reflect.Uint:
		val = float64(x.(uint))
	case reflect.Uint8:
		val = float64(x.(uint8))
	case reflect.Uint16:
		val = float64(x.(uint16))
	case reflect.Uint32:
		val = float64(x.(uint32))
	case reflect.Uint64:
		val = float64(x.(uint64))
	case reflect.Float32:
		val = float64(x.(float32))
	case reflect.Float64:
		val = x.(float64)
	case reflect.String:
		x, err := strconv.ParseFloat(x.(string), 64)
		if err != nil {
			return val, Error{Code: "non_numeric_string"}
		}
		val = x
	default:
		return float64(0), Error{Code: "non_numeric_type"}
	}

	return val, nil
}

func CompareIntValues(condition string, a, b interface{}) (bool, DbError) {
	aVal, errA := NumericToInt64(a)
	bVal, errB := NumericToInt64(b)

	if errA != nil {
		return false, errA
	}
	if errB != nil {
		return false, errB
	}

	switch condition {
	case "eq":
		return aVal == bVal, nil
	case "neq":
		return aVal != bVal, nil
	case "lt":
		return aVal < bVal, nil
	case "lte":
		return aVal <= bVal, nil
	case "gt":
		return aVal > bVal, nil
	case "gte":
		return aVal >= bVal, nil
	default:
		return false, Error{
			Code:    "unknown_filter",
			Message: "Unknown filter type: " + condition,
		}
	}
}

func CompareUintValues(condition string, a, b interface{}) (bool, DbError) {
	aVal, errA := NumericToUint64(a)
	bVal, errB := NumericToUint64(b)

	if errA != nil {
		return false, errA
	}
	if errB != nil {
		return false, errB
	}

	switch condition {
	case "eq":
		return aVal == bVal, nil
	case "neq":
		return aVal != bVal, nil
	case "lt":
		return aVal < bVal, nil
	case "lte":
		return aVal <= bVal, nil
	case "gt":
		return aVal > bVal, nil
	case "gte":
		return aVal >= bVal, nil
	default:
		return false, Error{
			Code:    "unknown_filter",
			Message: "Unknown filter type: " + condition,
		}
	}
}

func CompareFloatValues(condition string, a, b interface{}) (bool, DbError) {
	aVal, errA := NumericToFloat64(a)
	bVal, errB := NumericToFloat64(b)

	if errA != nil {
		return false, errA
	}
	if errB != nil {
		return false, errB
	}

	switch condition {
	case "eq":
		return aVal == bVal, nil
	case "neq":
		return aVal != bVal, nil
	case "lt":
		return aVal < bVal, nil
	case "lte":
		return aVal <= bVal, nil
	case "gt":
		return aVal > bVal, nil
	case "gte":
		return aVal >= bVal, nil
	default:
		return false, Error{
			Code:    "unknown_filter",
			Message: "Unknown filter type: " + condition,
		}
	}
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

// Returns pointer to a new struct with the same type as the given struct.
func NewStruct(typ interface{}) (interface{}, error) {
	// Build new struct.
	item := reflect.ValueOf(typ)
	if item.Type().Kind() == reflect.Ptr {
		item = item.Elem()
	}
	if item.Type().Kind() != reflect.Struct {
		return nil, errors.New("struct_expected")
	}

	return reflect.New(reflect.TypeOf(item.Interface())).Interface(), nil
}

// Build a new slice that can contain elements of the given type.
func NewSlice(typ interface{}) interface{} {
	// Build new array.
	// See http://stackoverflow.com/questions/25384640/why-golang-reflect-makeslice-returns-un-addressable-value
	// Create a slice to begin with
	myType := reflect.TypeOf(typ)
	slice := reflect.MakeSlice(reflect.SliceOf(myType), 0, 0)

	// Create a pointer to a slice value and set it to the slice
	x := reflect.New(slice.Type())
	x.Elem().Set(slice)

	return x.Elem().Interface()
}

func ConvertInterfaceToSlice(slice interface{}) ([]interface{}, error) {
	reflSlice := reflect.ValueOf(slice)

	if reflSlice.Type().Kind() == reflect.Ptr {
		reflSlice = reflSlice.Elem()
	}
	if reflSlice.Type().Kind() != reflect.Slice {
		return nil, errors.New("slice_expected")
	}

	result := make([]interface{}, 0)

	for i := 0; i < reflSlice.Len(); i++ {
		itemVal := reflSlice.Index(i)
		if itemVal.Type().Kind() == reflect.Struct {
			itemVal = itemVal.Addr()
		}
		result = append(result, itemVal.Interface())
	}

	return result, nil
}

// Convert a slice of type interface{} to a []Model slice.
func InterfaceToModelSlice(slice interface{}) ([]Model, error) {
	reflSlice := reflect.ValueOf(slice)

	if reflSlice.Type().Kind() == reflect.Ptr {
		reflSlice = reflSlice.Elem()
	}
	if reflSlice.Type().Kind() != reflect.Slice {
		return nil, errors.New("slice_expected")
	}

	result := make([]Model, 0)

	for i := 0; i < reflSlice.Len(); i++ {
		itemVal := reflSlice.Index(i)
		if itemVal.Type().Kind() == reflect.Struct {
			itemVal = itemVal.Addr()
		}
		item := itemVal.Interface()

		// Check that slice items actually implement model interface.
		// Only needed once.
		modelItem, ok := item.(Model)
		if i == 0 && !ok {
			return nil, errors.New("slice_values_do_not_implement_model_if")
		}

		result = append(result, modelItem)
	}

	return result, nil
}

// Convert a slice of type []Model to []interface{}.
func ModelToInterfaceSlice(models []Model) []interface{} {
	slice := make([]interface{}, 0)
	for _, m := range models {
		slice = append(slice, m.(interface{}))
	}

	return slice
}

/**
 * Sorter for sorting structs by field.
 */

type structFieldSorter struct {
	items     []interface{}
	field     string
	ascending bool
}

func (s structFieldSorter) Len() int {
	return len(s.items)
}

func (s structFieldSorter) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
}

func (s structFieldSorter) Less(i, j int) bool {
	valA, err := GetStructFieldValue(s.items[i], s.field)
	if err != nil {
		panic("Sorting failure: " + err.Error())
	}

	valB, err := GetStructFieldValue(s.items[j], s.field)
	if err != nil {
		panic("Sorting failure: " + err.Error())
	}

	less, err := CompareValues("lt", valA, valB)
	if err != nil {
		panic("Sorting failure: " + err.Error())
	}

	if s.ascending {
		return less
	} else {
		return !less
	}
}

func StructFieldSorter(items []interface{}, field string, asc bool) structFieldSorter {
	return structFieldSorter{
		items:     items,
		field:     field,
		ascending: asc,
	}
}

func SortStructSlice(items []interface{}, field string, ascending bool) {
	sort.Sort(StructFieldSorter(items, field, ascending))
}

/**
 * Setting and getting fields from a struct with reflect.
 */

// Given a struct or a pointer to a struct, retrieve the value of a field from
// the struct with reflection.
func GetStructFieldValue(s interface{}, fieldName string) (interface{}, DbError) {
	// Check if struct is valid.
	if s == nil {
		return nil, Error{Code: "pointer_or_struct_expected"}
	}

	// Check if it is a pointer, and if so, dereference it.
	v := reflect.ValueOf(s)
	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Type().Kind() != reflect.Struct {
		return nil, Error{Code: "struct_expected"}
	}

	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return nil, Error{
			Code:    "field_not_found",
			Message: fmt.Sprintf("struct does not have field '%v'", fieldName),
		}
	}

	return field.Interface(), nil
}

func GetStructField(s interface{}, fieldName string) (reflect.Value, DbError) {
	// Check if struct is valid.
	if s == nil {
		return reflect.Value{}, Error{Code: "pointer_or_struct_expected"}
	}

	// Check if it is a pointer, and if so, dereference it.
	v := reflect.ValueOf(s)
	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Type().Kind() != reflect.Struct {
		return reflect.Value{}, Error{Code: "struct_expected"}
	}

	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return reflect.Value{}, Error{
			Code:    "field_not_found",
			Message: fmt.Sprintf("struct does not have field '%v'", fieldName),
		}
	}

	return field, nil
}

// Given a pointer to a struct, set the given field to the given value.
// If the target value is not a string, it will be automatically converted
// to the proper type.
// Returns an error if no pointer to a struct is given, if the field does not
// exist, or if the string value can not be converted to the actual type.
func SetStructFieldValueFromString(obj interface{}, fieldName string, val string) DbError {
	objVal := reflect.ValueOf(obj)
	if objVal.Type().Kind() != reflect.Ptr {
		return Error{Code: "pointer_expected"}
	}

	objVal = objVal.Elem()
	if objVal.Type().Kind() != reflect.Struct {
		return Error{Code: "pointer_to_struct_expected"}
	}

	field := objVal.FieldByName(fieldName)
	if !field.IsValid() {
		return Error{
			Code:    "unknown_field",
			Message: fmt.Sprintf("Field %v does not exist on %v", fieldName, objVal),
		}
	}

	//fieldType, _ := objType.FieldByName(fieldName)
	convertedVal, err := ConvertStringToType(val, field.Type().Kind())
	if err != nil {
		return Error{Code: err.Error()}
	}

	field.Set(reflect.ValueOf(convertedVal))

	return nil
}

func GetModelCollection(model interface{}) (string, DbError) {
	if hook, ok := model.(ModelCollectionHook); ok {
		return hook.Collection(), nil
	}

	typ := reflect.TypeOf(model)

	// Dereference pointer.
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	// Check if it is a struct.
	if typ.Kind() != reflect.Struct {
		return "", Error{
			Code:    "invalid_model",
			Message: fmt.Sprintf("Expected model struct or pointer to struct, got %v", typ),
		}
	}

	collection := CamelCaseToUnderscore(typ.Name())
	if collection[len(collection)-1] != 's' {
		collection += "s"
	}

	return collection, nil
}

func MustGetModelCollection(model interface{}) string {
	collection, err := GetModelCollection(model)
	if err != nil {
		panic("Could not determine collection: " + err.Error())
	}

	return collection
}

func GetModelID(info *ModelInfo, m interface{}) (interface{}, DbError) {
	val, err := GetStructFieldValue(m, info.PkField)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func GetModelSliceFieldValues(models []Model, fieldName string) ([]interface{}, DbError) {
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

// Set a struct field.
// Returns an error if the object is not a struct or a pointer to a struct, or if
// the field does not exist.
func SetStructField(obj interface{}, fieldName string, value interface{}) error {
	val := reflect.ValueOf(obj)

	// Make sure obj is a pointer.
	if val.Type().Kind() != reflect.Ptr {
		return errors.New("pointer_to_struct_expected")
	}

	// Dereference pointer.
	val = val.Elem()

	// Make surre obj points to a struct.
	if val.Type().Kind() != reflect.Struct {
		return errors.New("struct_expected")
	}

	field := val.FieldByName(fieldName)
	if !field.IsValid() {
		return errors.New("unknown_field")
	}

	field.Set(reflect.ValueOf(value))

	return nil
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

func ModelToMap(info *ModelInfo, model interface{}, forBackend, marshal bool) (map[string]interface{}, DbError) {
	data := make(map[string]interface{})

	for fieldName := range info.FieldInfo {
		field := info.FieldInfo[fieldName]
		if field.Ignore || field.IsRelation() {
			continue
		}

		// Todo: avoid repeated work by GetStructFieldValue()
		val, err := GetStructFieldValue(model, fieldName)
		if err != nil {
			return nil, err
		}

		// Ignore zero values if specified.
		if field.IgnoreIfZero && IsZero(val) {
			continue
		}

		if (forBackend || marshal) && field.Marshal {
			js, err := json.Marshal(val)
			if err != nil {
				return nil, Error{
					Code:    "marshal_error",
					Message: fmt.Sprintf("Could not marshal %v.%v to json: %v", info.Name, fieldName, err),
				}
			}
			val = js
		}

		name := fieldName
		if forBackend {
			name = field.BackendName
		} else if marshal {
			name = field.MarshalName
		}

		data[name] = val
	}

	return data, nil
}

func ModelToJson(info *ModelInfo, model Model) ([]byte, DbError) {
	if info == nil {
		var err DbError
		info, err = NewModelInfo(model)
		if err != nil {
			return nil, err
		}
	}

	data, err := ModelToMap(info, model, false, true)
	if err != nil {
		return nil, err
	}

	js, err2 := json.Marshal(data)
	if err2 != nil {
		return nil, Error{
			Code:    "json_marshal_error",
			Message: err2.Error(),
		}
	}

	return js, nil
}

func BuildModelFromMap(info *ModelInfo, data map[string]interface{}) (interface{}, DbError) {
	model, err := NewStruct(info.Item)
	if err != nil {
		return nil, Error{
			Code:    "model_build_error",
			Message: err.Error(),
		}
	}

	err = UpdateModelFromData(info, model, data)
	if err != nil {
		return nil, Error{
			Code:    "model_update_error",
			Message: err.Error(),
		}
	}

	return model, nil
}

func UpdateModelFromData(info *ModelInfo, obj interface{}, data map[string]interface{}) DbError {
	ptrVal := reflect.ValueOf(obj)
	if ptrVal.Type().Kind() != reflect.Ptr {
		return Error{
			Code: "pointer_expected",
		}
	}
	val := ptrVal.Elem()
	if val.Type().Kind() != reflect.Struct {
		return Error{
			Code: "pointer_to_struct_expected",
		}
	}

	for key := range data {
		fieldInfo := info.FieldByBackendName(key)
		if fieldInfo == nil {
			fieldInfo = info.FieldInfo[key]
		}

		if fieldInfo == nil {
			continue
		}
		if fieldInfo.Ignore {
			continue
		}

		SetModelValue(fieldInfo, val.FieldByName(fieldInfo.Name), data[key])
	}

	return nil
}

func SetModelValue(info *FieldInfo, field reflect.Value, rawValue interface{}) {
	val := reflect.ValueOf(rawValue)

	// Skip invalid.
	if !val.IsValid() {
		return
	}

	valKind := val.Type().Kind()
	fieldKind := info.Type.Kind()

	// For the same type, skip complicated comparison/casting.
	if valKind == fieldKind {
		field.Set(val)
		return
	}

	switch fieldKind {
	case reflect.Bool:
		switch valKind {
		case reflect.String:
			x := rawValue.(string)
			flag := x == "1" || x == "yes"
			field.Set(reflect.ValueOf(flag))
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			x, err := NumericToInt64(rawValue)
			if err == nil {
				field.Set(reflect.ValueOf(x > 0))
			}
		}

	case reflect.String:
		bytes, ok := rawValue.([]byte)
		if ok {
			field.Set(reflect.ValueOf(string(bytes)))
		} else {
			x := fmt.Sprintf("%v", rawValue)
			field.Set(reflect.ValueOf(x))
		}

	case reflect.Int:
		switch valKind {
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			x, _ := NumericToInt64(rawValue)
			field.Set(reflect.ValueOf(int(x)))
		}

	case reflect.Int8:
		switch valKind {
		case reflect.Int, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			x, _ := NumericToInt64(rawValue)
			field.Set(reflect.ValueOf(int8(x)))
		}

	case reflect.Int16:
		switch valKind {
		case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			x, _ := NumericToInt64(rawValue)
			field.Set(reflect.ValueOf(int16(x)))
		}

	case reflect.Int32:
		switch valKind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			x, _ := NumericToInt64(rawValue)
			field.Set(reflect.ValueOf(int32(x)))
		}
	case reflect.Int64:
		switch valKind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			x, _ := NumericToInt64(rawValue)
			field.Set(reflect.ValueOf(x))
		}

	case reflect.Uint:
		switch valKind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			x, _ := NumericToUint64(rawValue)
			field.Set(reflect.ValueOf(uint(x)))
		}

	case reflect.Uint8:
		switch valKind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			x, _ := NumericToUint64(rawValue)
			field.Set(reflect.ValueOf(uint8(x)))
		}

	case reflect.Uint16:
		switch valKind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			x, _ := NumericToUint64(rawValue)
			field.Set(reflect.ValueOf(uint16(x)))
		}

	case reflect.Uint32:
		switch valKind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint64, reflect.Float32, reflect.Float64:
			x, _ := NumericToUint64(rawValue)
			field.Set(reflect.ValueOf(uint32(x)))
		}

	case reflect.Uint64:
		switch valKind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Float32, reflect.Float64:
			x, _ := NumericToUint64(rawValue)
			field.Set(reflect.ValueOf(x))
		}

	case reflect.Float32:
		switch valKind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float64:
			x, _ := NumericToFloat64(rawValue)
			field.Set(reflect.ValueOf(float32(x)))
		}

	case reflect.Float64:
		switch valKind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32:
			x, _ := NumericToFloat64(rawValue)
			field.Set(reflect.ValueOf(x))
		}
	}
}

func BuildModelSliceFromMap(info *ModelInfo, items []map[string]interface{}) (interface{}, DbError) {
	slice := NewSlice(info.Item)

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

/**
 * Model hooks.
 */

func CallModelHook(b Backend, m interface{}, hook string) DbError {
	switch hook {
	case "Validate":
		if h, ok := m.(ModelValidateHook); ok {
			return h.Validate()
		}
		return nil
	case "BeforeCreate":
		if h, ok := m.(ModelBeforeCreateHook); ok {
			return h.BeforeCreate(b)
		}
		return nil
	case "AfterCreate":
		if h, ok := m.(ModelAfterCreateHook); ok {
			h.AfterCreate(b)
		}
		return nil
	case "BeforeUpdate":
		if h, ok := m.(ModelBeforeUpdateHook); ok {
			return h.BeforeUpdate(b)
		}
		return nil
	case "AfterUpdate":
		if h, ok := m.(ModelAfterUpdateHook); ok {
			h.AfterUpdate(b)
		}
		return nil
	case "BeforeDelete":
		if h, ok := m.(ModelBeforeDeleteHook); ok {
			return h.BeforeDelete(b)
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
		return Error{
			Code:    "invalid_hook",
			Message: fmt.Sprintf("Unknown hook %v", hook),
		}
	}
}

/**
 * ModelInfo struct and methods
 */

// Contains information about a single field of a Model.
type FieldInfo struct {
	Name       string
	Type       reflect.Type
	StructType string

	// Specifies whether the field should be of type string and the raw value should
	// be marshalled before persisting to backend.
	Marshal bool

	// The field name to use for marshalling to json.
	MarshalName string

	// Specifies the name of the embedded struct that holds the field.
	// "" if not embedded.
	Embedded string

	PrimaryKey    bool
	AutoIncrement bool
	Ignore        bool
	IgnoreIfZero  bool
	NotNull       bool
	Default       string

	Unique     bool
	UniqueWith []string
	Index      string

	Min float64
	Max float64

	BackendName       string
	BackendConstraint string

	/**
	 * Relationship related fields
	 */

	// True if this field is a foreign key for a has one/belongs to relationship.
	M2M           bool
	M2MCollection string

	HasOne             bool
	HasOneField        string
	HasOneForeignField string

	BelongsTo             bool
	BelongsToField        string
	BelongsToForeignField string

	RelationItem       interface{}
	RelationCollection string
	RelationIsMany     bool
}

func (f FieldInfo) IsRelation() bool {
	return f.RelationItem != nil
}

// Contains information about a Model, including field info.
type ModelInfo struct {
	// Name of the struct field.
	PkField string

	Item       interface{}
	FullName   string
	Name       string
	Collection string

	BackendName string

	FieldInfo map[string]*FieldInfo
}

// Builds the ModelInfo for a model and returns it.
// Returns an error for all failures.
func NewModelInfo(model interface{}) (*ModelInfo, DbError) {
	typ := reflect.TypeOf(model)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil, Error{
			Code:    "invalid_model_argument",
			Message: fmt.Sprintf("Must use pointer to struct or struct, got %v", typ),
		}
	}

	collection, err := GetModelCollection(model)
	if err != nil {
		return nil, err
	}

	info := ModelInfo{
		Item:       reflect.New(typ).Interface(),
		FullName:   typ.PkgPath() + "." + typ.Name(),
		Name:       typ.Name(),
		Collection: collection,
		FieldInfo:  make(map[string]*FieldInfo),
	}

	info.BackendName = info.Collection

	// If model implements .BackendName() call it to determine backend name.
	if nameHook, ok := model.(ModelBackendNameHook); ok {
		name := nameHook.BackendName()
		if name == "" {
			return nil, Error{
				Code:    "invalid_backend_name_result",
				Message: fmt.Sprintf("Model %v.BackendName() returned empty string", info.FullName),
			}
		}
		info.BackendName = name
	}

	err = info.buildFieldInfo(reflect.ValueOf(model).Elem(), "")
	if err != nil {
		return nil, Error{
			Code:    "build_field_info_error",
			Message: fmt.Sprintf("Could not build field info for %v: %v", info.Name, err.GetMessage()),
			Data:    err,
		}
	}

	// Ensure primary key exists.
	if info.PkField == "" {
		// No explicit primary key found, check for ID field.
		if field, ok := info.FieldInfo["ID"]; ok {
			info.PkField = "ID"
			field.PrimaryKey = true
		}
	}

	for name := range info.FieldInfo {
		fieldInfo := info.FieldInfo[name]
		if fieldInfo.PrimaryKey {
			fieldInfo.NotNull = true
			fieldInfo.IgnoreIfZero = true
			fieldInfo.Unique = true

			// On numeric fields, activate autoincrement.
			// TODO: allow a way to disable autoincrement with a tag.
			switch fieldInfo.Type.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				fieldInfo.AutoIncrement = true
			}

			if info.PkField == "" {
				info.PkField = name
			}
		}
	}

	if info.PkField == "" {
		return nil, Error{
			Code:    "primary_key_not_found",
			Message: fmt.Sprintf("Primary key could not be determined for model %v", info.Name),
		}
	}

	return &info, nil
}

func (m ModelInfo) HasField(name string) bool {
	_, ok := m.FieldInfo[name]
	return ok
}

func (m ModelInfo) GetField(name string) *FieldInfo {
	return m.FieldInfo[name]
}

func (m ModelInfo) GetPkField() *FieldInfo {
	return m.FieldInfo[m.PkField]
}

func (m ModelInfo) GetPkName() string {
	return m.FieldInfo[m.PkField].Name
}

// Given a database field name, return the struct field name.
func (m ModelInfo) MapFieldName(name string) string {
	for key := range m.FieldInfo {
		if m.FieldInfo[key].BackendName == name {
			return key
		}
	}

	return ""
}

// Given a the field.MarshalName, return the struct field name.
func (m ModelInfo) MapMarshalName(name string) string {
	for key := range m.FieldInfo {
		if m.FieldInfo[key].MarshalName == name {
			return key
		}
	}

	return ""
}

// Return the field info for a given name.
func (m ModelInfo) FieldByBackendName(name string) *FieldInfo {
	for key := range m.FieldInfo {
		if m.FieldInfo[key].BackendName == name {
			return m.FieldInfo[key]
		}
	}

	return nil
}

// Parse the information contained in a 'db:"xxx"' field tag.
func ParseFieldTag(tag string) (*FieldInfo, DbError) {
	info := FieldInfo{}

	parts := strings.Split(strings.TrimSpace(tag), ";")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		itemParts := strings.Split(part, ":")

		specifier := part
		var value string
		if len(itemParts) > 1 {
			specifier = itemParts[0]
			value = itemParts[1]
		}

		switch specifier {
		case "name":
			if value == "" {
				return nil, Error{
					Code:    "invalid_name",
					Message: "name specifier must be in format name:the_name",
				}
			}

			info.BackendName = value

		case "marshal-name":
			if value == "" {
				return nil, Error{
					Code:    "invalid_name",
					Message: "name specifier must be in format marshal-name:the_name",
				}
			}
			info.MarshalName = value

		case "marshal":
			info.Marshal = true

		case "primary-key":
			info.PrimaryKey = true

		case "-":
			info.Ignore = true

		case "ignore-zero":
			info.IgnoreIfZero = true

		case "auto-increment":
			info.AutoIncrement = true

		case "unique":
			info.Unique = true

		case "not-null":
			info.NotNull = true
			info.IgnoreIfZero = true

		case "min":
			x, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, Error{
					Code:    "invalid_min",
					Message: "min:xx must be a valid number",
				}
			}
			info.Min = x

		case "max":
			x, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, Error{
					Code:    "invalid_max",
					Message: "max:xx must be a valid number",
				}
			}
			if x == -1 {
				info.Max = 1000000000000
			} else {
				info.Max = x
			}

		case "unique-with":
			parts := strings.Split(value, ",")
			if parts[0] == "" {
				return nil, Error{
					Code:    "invalid_unique_with",
					Message: "unique-with must be a comma-separated list of fields",
				}
			}
			info.UniqueWith = parts

		case "m2m":
			info.M2M = true
			if value != "" {
				info.M2MCollection = value
			}

		case "has-one":
			info.HasOne = true
			if value != "" {
				if len(itemParts) < 3 {
					return nil, Error{
						Code:    "invalid_has_one",
						Message: "Explicit has-one needs to be in format 'has-one:localField:foreignKey'",
					}
				}
				info.HasOneField = itemParts[1]
				info.HasOneForeignField = itemParts[2]
			}

		case "belongs-to":
			info.BelongsTo = true
			if value != "" {
				if len(itemParts) < 3 {
					return nil, Error{
						Code:    "invalid_belongs_to",
						Message: "Explicit belongs-to needs to be in format 'belongs-to:localField:foreignKey'",
					}
				}
				info.BelongsToField = itemParts[1]
				info.BelongsToForeignField = itemParts[2]
			}
		}
	}

	return &info, nil
}

// Build the field information for the model.
func (info *ModelInfo) buildFieldInfo(modelVal reflect.Value, embeddedName string) DbError {
	modelType := modelVal.Type()

	for i := 0; i < modelVal.NumField(); i++ {
		field := modelVal.Field(i)
		fieldType := modelType.Field(i)
		fieldKind := fieldType.Type.Kind()

		// Skip fields that  were already defined in parent model.
		if embeddedName != "" && info.HasField(fieldType.Name) {
			continue
		}

		// Ignore private fields.
		firstChar := fieldType.Name[0:1]
		if strings.ToLower(firstChar) == firstChar {
			continue
		}

		if fieldKind == reflect.Struct && fieldType.Anonymous {
			// Embedded struct. Find nested fields.
			if err := info.buildFieldInfo(field, fieldType.Name); err != nil {
				return err
			}
			continue
		}

		fieldInfo, err := ParseFieldTag(fieldType.Tag.Get("db"))
		if err != nil {
			return err
		}

		fieldInfo.Name = fieldType.Name
		fieldInfo.Type = fieldType.Type
		fieldInfo.Embedded = embeddedName

		if fieldInfo.BackendName == "" {
			fieldInfo.BackendName = CamelCaseToUnderscore(fieldType.Name)
		}

		// Default marshal name.
		if fieldInfo.MarshalName == "" {
			fieldInfo.MarshalName = LowerCaseFirst(fieldInfo.Name)
		}

		info.FieldInfo[fieldType.Name] = fieldInfo
	}

	return nil
}

/**
 * Functions for analyzing the relationships between model structs.
 */

// Build the relationship information for the model after all fields have been analyzed.
func BuildAllRelationInfo(models map[string]*ModelInfo) DbError {
	for key := range models {
		if err := buildRelationshipInfo(models, models[key]); err != nil {
			return err
		}
	}

	return nil
}

// Recursive helper for building the relationship information.
// Will properly analyze all embedded structs as well.
// WARNING: will panic on errors.
func buildRelationshipInfo(models map[string]*ModelInfo, model *ModelInfo) DbError {
	for name := range model.FieldInfo {
		fieldInfo := model.FieldInfo[name]

		if fieldInfo.Ignore {
			// Ignored field.
			continue
		}

		fieldType := fieldInfo.Type
		fieldKind := fieldInfo.Type.Kind()

		// Find relationship items for structs and slices.

		var relatedItem interface{}
		relationStructType := ""
		relationIsMany := false

		if fieldKind == reflect.Struct {
			// Field is a direct struct.
			// RelationItem type is the struct.
			relationStructType = fieldType.PkgPath() + "." + fieldType.Name()
			relatedItem = reflect.New(fieldType).Interface()
		} else if fieldKind == reflect.Ptr {
			// Field is a pointer.
			ptrType := fieldType.Elem()

			if ptrType.Kind() == reflect.Struct {
				relationStructType = ptrType.PkgPath() + "." + ptrType.Name()
			}

			relatedItem = reflect.New(ptrType).Interface()
		} else if fieldKind == reflect.Slice {
			// Field is slice.
			// Check if slice items are models or pointers to models.
			sliceType := fieldType.Elem()
			sliceKind := sliceType.Kind()

			if sliceKind == reflect.Struct {
				// Slice contains structs.
				relationStructType = sliceType.PkgPath() + "." + sliceType.Name()
				relatedItem = reflect.New(sliceType).Interface()
				relationIsMany = true
			} else if sliceKind == reflect.Ptr {
				// Slice contains pointers.
				ptrType := sliceType.Elem()

				relationStructType = ptrType.PkgPath() + "." + ptrType.Name()
				relatedItem = reflect.New(ptrType).Interface()
				relationIsMany = true
			}
		}

		if relatedItem == nil {
			// Only process fields with a relation.
			continue
		}

		// Set struct type even if it is not a processed relation.
		// Some backends need this information.
		fieldInfo.StructType = relationStructType

		relatedCollection := MustGetModelCollection(relatedItem)
		if relatedCollection == "" {
			panic("Empty collection")
		}

		relatedInfo, ok := models[relatedCollection]
		if !ok {
			// Related struct type was not registered, so ignore  it.
			continue
		}

		// Update field info.
		fieldInfo.RelationItem = relatedItem
		fieldInfo.RelationCollection = relatedCollection
		fieldInfo.RelationIsMany = relationIsMany

		modelName := model.Name
		relatedName := relatedInfo.Name

		relatedFields := relatedInfo.FieldInfo

		if !(fieldInfo.BelongsTo || fieldInfo.HasOne || fieldInfo.M2M) {
			// No explicit relationship defined. Try to determine it.

			// Can be either HasOne or BelongsTo, since m2m needs to be explicitly specified.

			// Check for HasOne first.
			if !fieldInfo.RelationIsMany {
				// Try to fiend ID field.
				relField := relatedName + "ID"
				_, ok := model.FieldInfo[relField]
				if !ok {
					relField = name + "ID"
					_, ok = model.FieldInfo[relField]
				}
				if ok {
					// Related field exists.
					fieldInfo.HasOne = true
					fieldInfo.HasOneField = relField
					fieldInfo.HasOneForeignField = relatedInfo.PkField
				}
			}

			if !fieldInfo.HasOne {
				// Not has one, check for belongsTo.
				relField := modelName + "ID"
				if _, ok := relatedFields[relField]; ok {
					// realted field found. Is belongsTo.
					fieldInfo.BelongsTo = true
					fieldInfo.BelongsToForeignField = relField
				}
			}
		}

		if fieldInfo.HasOne {
			if fieldInfo.HasOneField == "" {
				return Error{
					Code: "has_one_field_not_determined",
					Message: fmt.Sprintf("has-one specified on model %v, but field %v not found. Specify ID field.",
						modelName, relatedName+"ID"),
				}
			}
			if _, ok := model.FieldInfo[fieldInfo.HasOneField]; !ok {
				return Error{
					Code: "has_one_field_missing",
					Message: fmt.Sprintf("Specified has-one field %v not found on model %v",
						fieldInfo.HasOneField, modelName),
				}
			}

			// Ignore zero values to avoid inserts with 0.
			model.FieldInfo[fieldInfo.HasOneField].IgnoreIfZero = true

			if _, ok := relatedFields[fieldInfo.HasOneForeignField]; !ok {
				return Error{
					Code: "has_one_foreign_field_missing",
					Message: fmt.Sprintf("has-one specified on model %v with foreign key %v which does not exist on target %v",
						modelName, fieldInfo.HasOneForeignField, relatedName),
				}
			}
		} else if fieldInfo.BelongsTo {
			if fieldInfo.BelongsToForeignField == "" {
				return Error{
					Code: "belongs_to_foreign_field_not_determined",
					Message: fmt.Sprintf("belongs-to specified on model %v, but field %v not found. Specify ID field.",
						modelName, modelName+"ID"),
				}
			}
			if _, ok := relatedFields[fieldInfo.BelongsToForeignField]; !ok {
				return Error{
					Code: "belongs_to_foreign_field_missing",
					Message: fmt.Sprintf("Specified belongs-to field %v not found on model %v",
						fieldInfo.BelongsToForeignField, relatedName),
				}
			}

			if fieldInfo.BelongsToField == "" {
				fieldInfo.BelongsToField = model.PkField
			}

			if _, ok := model.FieldInfo[fieldInfo.BelongsToField]; !ok {
				return Error{
					Code:    "belongs_to_field_missing",
					Message: fmt.Sprintf("Model %v has no field %v", modelName, fieldInfo.BelongsToField),
				}
			}

			model.FieldInfo[fieldInfo.BelongsToField].IgnoreIfZero = true
		} else if fieldInfo.M2M {
			if fieldInfo.M2MCollection == "" {
				fieldInfo.M2MCollection = model.BackendName + "_" + relatedInfo.BackendName
			}
		}

		if !(fieldInfo.HasOne || fieldInfo.BelongsTo || fieldInfo.M2M) {
			return Error{
				Code: "relationship_not_determined",
				Message: fmt.Sprintf("Model %v has relationship to %v in field %v, but could not determine the neccessary relation fields.",
					modelName, relatedName, name),
			}
		}
	}

	return nil
}

/**
 * Model marshaling.
 */

/**
 * Query parser functions.
 */

func ParseJsonQuery(collection string, js []byte) (Query, DbError) {
	var data map[string]interface{}
	if err := json.Unmarshal(js, &data); err != nil {
		return nil, Error{
			Code:    "invalid_json",
			Message: "Query json could not be unmarshaled. Check for invalid json.",
		}
	}

	return ParseQuery(collection, data)
}

// Build a database query based a map[string]interface{} data structure
// resembling a Mongo query.
//
// It returns a Query equal to the Mongo query, with unsupported features omitted.
// An error is returned if the building of the query fails.
func ParseQuery(collection string, data map[string]interface{}) (Query, DbError) {
	q := Q(collection)

	// First, Handle joins so query and field specification parsing can use
	// join info.
	if rawJoins, ok := data["joins"]; ok {
		rawJoinSlice, ok := rawJoins.([]interface{})
		if !ok {
			return nil, Error{
				Code:    "invalid_joins",
				Message: "Joins must be an array of strings",
			}
		}

		// Convert []interface{} joins to []string.

		joins := make([]string, 0)
		for _, rawJoin := range rawJoinSlice {
			join, ok := rawJoin.(string)
			if !ok {
				return nil, Error{
					Code:    "invalid_joins",
					Message: "Joins must be an array of strings",
				}
			}
			joins = append(joins, join)
		}

		// To handle nested joins, parseQueryJoins has to be called repeatedly
		// until no more joins are returned.
		for depth := 1; true; depth++ {
			var err DbError
			joins, err = parseQueryJoins(q, joins, depth)
			if err != nil {
				return nil, err
			}

			if len(joins) == 0 {
				break
			}
		}
	}

	if rawQuery, ok := data["filters"]; ok {
		query, ok := rawQuery.(map[string]interface{})
		if !ok {
			return nil, Error{
				Code:    "invalid_filters",
				Message: "The filters key must contain a dict",
			}
		}

		if err := parseQueryFilters(q, query); err != nil {
			return nil, err
		}
	}

	// Handle fields.
	if rawFields, ok := data["fields"]; ok {
		fields, ok := rawFields.([]interface{})
		if !ok {
			return nil, Error{
				Code:    "invalid_fields",
				Message: "Fields specification must be an array",
			}
		}

		for _, rawField := range fields {
			field, ok := rawField.(string)
			if !ok {
				return nil, Error{
					Code:    "invalid_fields",
					Message: "Fields specification must be an array of strings",
				}
			}

			parts := strings.Split(field, ".")
			if len(parts) > 1 {
				// Possibly a field on a joined model. Check if a parent join can be found.
				joinQ := q.GetJoin(strings.Join(parts[:len(parts)-1], "."))
				if joinQ != nil {
					// Join query found, add field to the join query.
					joinQ.AddFields(parts[len(parts)-1])
				} else {
					// No join query found, maybe the backend supports nested fields.
					joinQ.AddFields(field)
				}
			} else {
				// Not nested, just add the field.
				q.AddFields(field)
			}
		}
	}

	// Handle limit.
	if rawLimit, ok := data["limit"]; ok {
		if limit, err := NumericToInt64(rawLimit); err != nil {
			return nil, Error{
				Code:    "limit_non_numeric",
				Message: "Limit must be a number",
			}
		} else {
			q.Limit(int(limit))
		}
	}

	// Handle offset.
	if rawOffset, ok := data["offset"]; ok {
		if offset, err := NumericToInt64(rawOffset); err != nil {
			return nil, Error{
				Code:    "offset_non_numeric",
				Message: "Offset must be a number",
			}
		} else {
			q.Offset(int(offset))
		}
	}

	return q, nil
}

func parseQueryJoins(q Query, joins []string, depth int) ([]string, DbError) {
	remaining := make([]string, 0)

	for _, name := range joins {
		parts := strings.Split(name, ".")
		joinDepth := len(parts)
		if joinDepth == depth {
			// The depth of the join equals to the one that should be processed, so do!
			if len(parts) > 1 {
				// Nested join! So try to retrieve the parent join query.
				joinQ := q.GetJoin(strings.Join(parts[:joinDepth-1], "."))
				if joinQ == nil {
					// Parent join not found, obviosly an error.
					return nil, Error{
						Code:    "invalid_nested_join",
						Message: fmt.Sprintf("Tried to join %v, but the parent join was not found", name),
					}
				}
				// Join the current join on the parent join.
				joinQ.Join(parts[len(parts)-1])
			} else {
				// Not nested, just join on the main query.
				q.Join(name)
			}
		} else {
			// Join has other depth than the one that is processed, so append to
			// remaining.
			remaining = append(remaining, name)
		}
	}

	return remaining, nil
}

func parseQueryFilters(q Query, filters map[string]interface{}) DbError {
	filter, err := parseQueryFilter("", filters)
	if err != nil {
		return err
	}

	// If filter is an and, add the and clauses separately to the query.
	// Done for prettier query without top level AND.
	if andFilter, ok := filter.(*AndCondition); ok {
		for _, filter := range andFilter.Filters {
			q.FilterQ(filter)
		}
	} else {
		q.FilterQ(filter)
	}

	return nil
}

// Parses a mongo query filter to a Filter.
// All mongo operators expect $nor are supported.
// Refer to http://docs.mongodb.org/manual/reference/operator/query.
func parseQueryFilter(name string, data interface{}) (Filter, DbError) {
	// Handle
	switch name {
	case "$eq":
		return Eq("placeholder", data), nil
	case "$ne":
		return Neq("placeholder", data), nil
	case "$in":
		return In("placeholder", data), nil
	case "$like":
		return Like("placeholder", data), nil
	case "$gt":
		return Gt("placeholder", data), nil
	case "$gte":
		return Gte("placeholder", data), nil
	case "$lt":
		return Lt("placeholder", data), nil
	case "$lte":
		return Lte("placeholder", data), nil
	case "$nin":
		return Not(In("placeholder", data)), nil
	}

	if name == "$nor" {
		return nil, Error{
			Code:    "unsupported_nor_query",
			Message: "$nor queryies are not supported",
		}
	}

	// Handle OR.
	if name == "$or" {
		orClauses, ok := data.([]interface{})
		if !ok {
			return nil, Error{Code: "invalid_or_data"}
		}

		or := Or()
		for _, rawClause := range orClauses {
			clause, ok := rawClause.(map[string]interface{})
			if !ok {
				return nil, Error{Code: "invalid_or_data"}
			}

			filter, err := parseQueryFilter("", clause)
			if err != nil {
				return nil, err
			}
			or.Add(filter)
		}

		return or, nil
	}

	if nestedData, ok := data.(map[string]interface{}); ok {
		// Nested dict with multipe AND clauses.

		// Build an AND filter.
		and := And()
		for key := range nestedData {
			filter, err := parseQueryFilter(key, nestedData[key])
			if err != nil {
				return nil, err
			}

			if key == "$or" || key == "$and" || key == "$not" {
				// Do nothing
			} else if name == "" {
				filter.SetField(key)
			} else {
				filter.SetField(name)
			}

			and.Add(filter)
		}

		if len(and.Filters) == 1 {
			return and.Filters[0], nil
		} else {
			return and, nil
		}
	}

	// If execution reaches this point, the filter must be a simple equals filter
	// with a value.
	return Eq(name, data), nil
}
