package dukedb

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

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
	if val == nil {
		return true
	}

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

// Convert converts an arbitrary interface value to a different type.
// The rawType argument may either be a reflect.Type or an acutal instance of the type.
func Convert(value interface{}, rawType interface{}) (interface{}, error) {
	var typ reflect.Type
	if t, ok := rawType.(reflect.Type); ok {
		typ = t
	} else {
		typ = reflect.TypeOf(rawType)
	}

	kind := typ.Kind()

	valType := reflect.TypeOf(value)
	valKind := valType.Kind()

	if valKind == kind {
		// Same kind, nothing to convert.
		return value, nil
	}

	// Parse dates into time.Time.
	isTime := kind == reflect.Struct && typ.PkgPath() == "time" && typ.Name() == "Time"

	if isTime && valKind == reflect.String {
		date, err := time.Parse(time.RFC3339, value.(string))
		if err != nil {
			return nil, apperror.Wrap(err, "time_parse_error", "Invalid time format", true)
		}
		return date, nil
	}

	// Special handling for string to bool.
	if kind == reflect.String && valKind == reflect.Bool {
		str := strings.TrimSpace(value.(string))
		switch str {
		case "y", "yes", "1":
			return true, nil
		case "n", "no", "0":
			return false, nil
		}
	}

	// Special handling for string target.
	if kind == reflect.String {
		if bytes, ok := value.([]byte); ok {
			return string(bytes), nil
		}

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
	var myType reflect.Type

	if t, ok := typ.(reflect.Type); ok {
		myType = t
	} else {
		myType = reflect.TypeOf(typ)
	}

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

func InterfaceToTypedSlice(itemType reflect.Type, slice []interface{}) interface{} {
	newSlice := reflect.ValueOf(NewSlice(itemType))

	for _, item := range slice {
		newSlice = reflect.Append(newSlice, reflect.ValueOf(item))
	}

	return newSlice.Interface()
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
		// Note that numeric fields are not included, since 0 is their zero value.
		// Also, primary keys are ignored.
		if field.IgnoreIfZero && !field.PrimaryKey && !IsNumericKind(field.Type.Kind()) && IsZero(val) {
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

// ModelFieldDiff compares two models and returns a list of fields that are different.
func ModelFieldDiff(info *ModelInfo, m1, m2 interface{}) []string {
	m1Data, _ := ModelToMap(info, m1, false, false)
	m2Data, _ := ModelToMap(info, m2, false, false)

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

		// Need special handling for point type.
		if strings.HasSuffix(fieldInfo.StructType, "go-dukedb.Point") {
			p := new(Point)
			_, err := fmt.Sscanf(data[key].(string), "(%f,%f)", &p.Lat, &p.Lon)
			if err != nil {
				return &apperror.Err{
					Code:    "point_conversion_error",
					Message: fmt.Sprintf("Could not parse point specification: %v", data[key]),
				}
			}
			if fieldInfo.Type.Kind() == reflect.Ptr {
				data[key] = p
			} else {
				data[key] = *p
			}
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

	// Try to convert using Convert() method.
	convertedVal, err := Convert(rawValue, info.Type)
	if err == nil {
		field.Set(reflect.ValueOf(convertedVal))
		return
	}

	// Nothing worked.
	panic(fmt.Sprintf("Could not convert type %v (%v) to type %v: %v", valKind, rawValue, fieldKind, err))
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
 * Query related.
 */

// NormalizeQuery perfoms multiple actions on queries to normalize them.
// It accepts FieldInfo.BackendName, FieldInfo.Name and FieldInfo.MarshalName
// for as field names in filters, field limitations and joins.
// It also replaces an "id" filter with the primary key field,
// if the model does not have an ID struct field.
func NormalizeQuery(info *ModelInfo, query Query) apperror.Error {
	// First, normalize field limitations.
	fields := query.GetFields()
	for index, fieldName := range fields {
		backendName := info.FindBackendFieldName(fieldName)

		if backendName == "" {
			return &apperror.Err{
				Code:    "invalid_field",
				Message: fmt.Sprintf("Collection %v does not have a field %v", info.Collection, fieldName),
				Public:  true,
			}
		}

		fields[index] = backendName
	}

	// Next, normalize joins.
	joins := query.GetJoins()
	for _, join := range joins {
		relationName := join.GetRelationName()

		if relationName != "" {

			fieldName := info.FindStructFieldName(relationName)
			if fieldName != "" {
				join.SetRelationName(fieldName)
			}
		}
	}

	// Now, check filters.
	filters := query.GetFilters()
	for _, filter := range filters {
		if err := NormalizeFilter(info, filter); err != nil {
			return err
		}
	}

	// Finally, check orders.
	orders := query.GetOrders()
	for _, order := range orders {
		// Replace id with actual primary key.
		if order.Field == "id" && !info.HasField("id") {
			order.Field = info.GetField(info.PkField).BackendName
			continue
		}

		if field := info.FindBackendFieldName(order.Field); field != "" {
			order.Field = field
		}
	}

	return nil
}

func NormalizeFilter(info *ModelInfo, filter Filter) apperror.Error {
	// Handle multi filters, by recursively checking them.
	if multi, ok := filter.(MultiFilter); ok {
		for _, f := range multi.GetFilters() {
			if err := NormalizeFilter(info, f); err != nil {
				return err
			}
		}
	}

	// Handle field conditions.
	if cond, ok := filter.(*FieldCondition); ok {
		field := cond.Field

		// If the filter is for "id", but the model does not have an id field,
		// replace it with the PkField.
		if field == "id" && !info.HasField("id") {
			cond.SetField(info.GetField(info.PkField).BackendName)
			return nil
		}

		backendName := info.FindBackendFieldName(field)
		if backendName != "" {
			cond.SetField(backendName)
		}
	}

	// Unknown filter type, so ignore.

	// No errors detected, return nil.
	return nil
}

/**
 * Model hooks.
 */

func ValidateModel(info *ModelInfo, m interface{}) apperror.Error {
	val := reflect.ValueOf(m).Elem()

	for fieldName, fieldInfo := range info.FieldInfo {

		// Fill in default values.
		if fieldInfo.Default != "" {
			fieldVal := val.FieldByName(fieldName)
			if IsZero(fieldVal.Interface()) {
				convertedVal, err := Convert(fieldInfo.Default, fieldInfo.Type)
				if err != nil {
					msg := fmt.Sprintf("Could not convert the default value '%v' for field %v to type %v",
						fieldInfo.Default, fieldName, fieldInfo.Type.Kind())
					return apperror.Wrap(err, "default_value_conversion_error", msg)
				}

				fieldVal.Set(reflect.ValueOf(convertedVal))
			}
		}

		// If NotNull is set to true, and the field is not a primary key, validate that it is
		// not zero.
		// Note: numeric fields will not be checked, since their zero value is "0", which might
		// be a valid field value.
		if fieldInfo.NotNull && !fieldInfo.PrimaryKey && !IsNumericKind(fieldInfo.Type.Kind()) {
			fieldVal := val.FieldByName(fieldName)

			if IsZero(fieldVal.Interface()) {
				return &apperror.Err{
					Code:    "empty_required_field",
					Message: fmt.Sprintf("The required field %v is empty", fieldName),
					Public:  true,
				}
			}
		} else if fieldInfo.Min > 0 || fieldInfo.Max > 0 {
			// Either min or max is set, so check length.
			fieldVal := val.FieldByName(fieldName)

			var length float64

			if fieldInfo.Type.Kind() == reflect.String {
				length = float64(fieldVal.Len())
			} else if IsNumericKind(fieldInfo.Type.Kind()) {
				length = fieldVal.Convert(reflect.TypeOf(float64(0))).Interface().(float64)
			} else {
				// Not string or numeric, so can't check.
				continue
			}

			if fieldInfo.Min > 0 && length < fieldInfo.Min {
				return &apperror.Err{
					Code:    "shorter_than_min_length",
					Message: fmt.Sprintf("The field %v is shorter than the minimum length %v", fieldName, fieldInfo.Min),
				}
			} else if fieldInfo.Max > 0 && length > fieldInfo.Max {
				return &apperror.Err{
					Code:    "longer_than_max_length",
					Message: fmt.Sprintf("The field %v is longer than the maximum length %v", fieldName, fieldInfo.Max),
				}
			}
		}
	}

	// If the model implements ModelValidateHook, call it.
	if validator, ok := m.(ModelValidateHook); ok {
		if err := validator.Validate(); err != nil {
			// Check if error is an apperror, and return it if so.
			if apperr, ok := err.(apperror.Error); ok {
				return apperr
			} else {
				// Not an apperror, so create a new one.
				return apperror.New(err.Error())
			}
		}
	}

	return nil
}

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
