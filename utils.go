package dukedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/theduke/go-apperror"
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
func FilterToSqlCondition(filter string) (string, apperror.Error) {
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
		return "", &apperror.Err{
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

func Convert(value interface{}, rawType interface{}) (interface{}, error) {
	var typ reflect.Type
	if t, ok := rawType.(reflect.Type); ok {
		typ = t
	} else {
		typ = reflect.TypeOf(rawType)
	}

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

func CompareValues(condition string, a, b interface{}) (bool, apperror.Error) {
	typ := reflect.TypeOf(a).Kind()

	switch typ {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return CompareNumericValues(condition, a, b)
	case reflect.String:
		return CompareStringValues(condition, a, b)
	default:
		return false, &apperror.Err{
			Code:    "unsupported_comparison_type",
			Message: fmt.Sprintf("Type %v can not be compared", typ),
		}
	}
}

func CompareStringValues(condition string, a, b interface{}) (bool, apperror.Error) {
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
		return false, &apperror.Err{
			Code:    "unknown_filter",
			Message: fmt.Sprintf("Unknown filter type '%v'", condition),
		}
	}
}

func CompareNumericValues(condition string, a, b interface{}) (bool, apperror.Error) {
	typ := reflect.TypeOf(a).Kind()
	switch typ {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return CompareIntValues(condition, a, b)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return CompareUintValues(condition, a, b)
	case reflect.Float32, reflect.Float64:
		return CompareFloatValues(condition, a, b)
	default:
		return false, &apperror.Err{
			Code: "unsupported_type_for_numeric_comparison",
			Message: fmt.Sprintf(
				"For a numeric comparision with %v, a numeric type is expected. Got: %v",
				condition, typ),
		}
	}
}

func NumericToInt64(x interface{}) (int64, apperror.Error) {
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
			return int64(0), &apperror.Err{Code: "non_numeric_string"}
		}
		val = x
	default:
		return int64(0), &apperror.Err{Code: "non_numeric_type"}
	}

	return val, nil
}

func NumericToUint64(x interface{}) (uint64, apperror.Error) {
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
			return uint64(0), &apperror.Err{Code: "non_numeric_string"}
		}
		val = uint64(x)
	default:
		panic("nonnumeric")
		return uint64(0), &apperror.Err{Code: "non_numeric_type"}
	}

	return val, nil
}

func NumericToFloat64(x interface{}) (float64, apperror.Error) {
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
			return val, &apperror.Err{Code: "non_numeric_string"}
		}
		val = x
	default:
		return float64(0), &apperror.Err{Code: "non_numeric_type"}
	}

	return val, nil
}

func CompareIntValues(condition string, a, b interface{}) (bool, apperror.Error) {
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
		return false, &apperror.Err{
			Code:    "unknown_filter",
			Message: "Unknown filter type: " + condition,
		}
	}
}

func CompareUintValues(condition string, a, b interface{}) (bool, apperror.Error) {
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
		return false, &apperror.Err{
			Code:    "unknown_filter",
			Message: "Unknown filter type: " + condition,
		}
	}
}

func CompareFloatValues(condition string, a, b interface{}) (bool, apperror.Error) {
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
		return false, &apperror.Err{
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
func GetStructFieldValue(s interface{}, fieldName string) (interface{}, apperror.Error) {
	// Check if struct is valid.
	if s == nil {
		return nil, &apperror.Err{Code: "pointer_or_struct_expected"}
	}

	// Check if it is a pointer, and if so, dereference it.
	v := reflect.ValueOf(s)
	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Type().Kind() != reflect.Struct {
		return nil, &apperror.Err{Code: "struct_expected"}
	}

	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return nil, &apperror.Err{
			Code:    "field_not_found",
			Message: fmt.Sprintf("struct %v does not have field '%v'", v.Type(), fieldName),
		}
	}

	return field.Interface(), nil
}

func GetStructField(s interface{}, fieldName string) (reflect.Value, apperror.Error) {
	// Check if struct is valid.
	if s == nil {
		return reflect.Value{}, &apperror.Err{Code: "pointer_or_struct_expected"}
	}

	// Check if it is a pointer, and if so, dereference it.
	v := reflect.ValueOf(s)
	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Type().Kind() != reflect.Struct {
		return reflect.Value{}, &apperror.Err{Code: "struct_expected"}
	}

	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return reflect.Value{}, &apperror.Err{
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
func SetStructFieldValueFromString(obj interface{}, fieldName string, val string) apperror.Error {
	objVal := reflect.ValueOf(obj)
	if objVal.Type().Kind() != reflect.Ptr {
		return &apperror.Err{Code: "pointer_expected"}
	}

	objVal = objVal.Elem()
	if objVal.Type().Kind() != reflect.Struct {
		return &apperror.Err{Code: "pointer_to_struct_expected"}
	}

	field := objVal.FieldByName(fieldName)
	if !field.IsValid() {
		return &apperror.Err{
			Code:    "unknown_field",
			Message: fmt.Sprintf("Field %v does not exist on %v", fieldName, objVal),
		}
	}

	//fieldType, _ := objType.FieldByName(fieldName)
	convertedVal, err := ConvertStringToType(val, field.Type().Kind())
	if err != nil {
		return &apperror.Err{Code: err.Error()}
	}

	field.Set(reflect.ValueOf(convertedVal))

	return nil
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

func GetModelID(info *ModelInfo, m interface{}) (interface{}, apperror.Error) {
	val, err := GetStructFieldValue(m, info.PkField)
	if err != nil {
		return nil, err
	}

	return val, nil
}

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

func ModelToMap(info *ModelInfo, model interface{}, forBackend, marshal bool) (map[string]interface{}, apperror.Error) {
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
				return nil, &apperror.Err{
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

func ModelToJson(info *ModelInfo, model Model) ([]byte, apperror.Error) {
	if info == nil {
		var err apperror.Error
		info, err = CreateModelInfo(model)
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
		return nil, &apperror.Err{
			Code:    "json_marshal_error",
			Message: err2.Error(),
		}
	}

	return js, nil
}

func BuildModelFromMap(info *ModelInfo, data map[string]interface{}) (interface{}, apperror.Error) {
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

func UpdateModelFromData(info *ModelInfo, obj interface{}, data map[string]interface{}) apperror.Error {
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
		fieldInfo := info.FieldByBackendName(key)
		if fieldInfo == nil {
			// Does not match a backend name.
			// Try to find field by marshal name to support unmarshalled data.
			fieldInfo = info.FieldByMarshalName(key)

			// If key does not match a marshal name either, just assume it to be a plain struct field name.
			if fieldInfo == nil {
				fieldInfo = info.FieldInfo[key]
			}
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

func BuildModelSliceFromMap(info *ModelInfo, items []map[string]interface{}) (interface{}, apperror.Error) {
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

func CallModelHook(b Backend, m interface{}, hook string) apperror.Error {
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
		return &apperror.Err{
			Code:    "invalid_hook",
			Message: fmt.Sprintf("Unknown hook %v", hook),
		}
	}
}
