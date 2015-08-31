package dukedb

import(
	"reflect"
	"errors"
	"strings"
	"fmt"
	"strconv"
	"sort"
	"encoding/json"
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
			Code: "unknown_filter",
			Message: "Unknown filter '" + filter + "'",
		}
	}

	return typ, nil
}


/**
 * Generic interface variable handling/comparison functions.
 */

// Convert a string value to the specified type if possible.
// Returns an error for unsupported types.
func ConvertToType(value string, typ reflect.Kind) (interface{}, error) {
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
			Code: "unsupported_comparison_type",
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
			Code: "unknown_filter", 
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
		default:
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
			Code: "unknown_filter",
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
			Code: "unknown_filter",
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
			Code: "unknown_filter",
			Message: "Unknown filter type: " + condition,
		}
	}
}

/**
 * Helpers for creating and converting structs and slices.
 */

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

	for i := 0; i < reflSlice.Len(); i ++ {
		item := reflSlice.Index(i).Interface()
		modelItem, ok := item.(Model)

		// Check that slice items actually implement model interface.
		// Only needed once.
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
	items []interface{}
	field string
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
		items: items,
		field: field,
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
			Code: "field_not_found", 
			Message: fmt.Sprintf("struct does not have field '%v'", fieldName),
		}
	}	

	return field.Interface(), nil
}

// Given a pointer to a struct, set the given field to the given value.
// If the target value is not a string, it will be automatically converted
// to the proper type.
// Returns an error if no pointer to a struct is given, if the field does not 
// exist, or if the string value can not be converted to the actual type.
func SetStructFieldValueFromString(obj interface{}, fieldName string,  val string) DbError {
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
			Code: "unknown_field",
			Message: fmt.Sprintf("Field %v does not exist on %v", fieldName, objVal),
		}
	}

	//fieldType, _ := objType.FieldByName(fieldName)
	convertedVal, err := ConvertToType(val, field.Type().Kind())
	if err != nil {
		return Error{Code: err.Error()}
	}

	field.Set(reflect.ValueOf(convertedVal))

	return nil
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

// Given a struct, set the specified field that contains either a single Model
// or a model slice to the given models.
// If the target field type is struct or pointer to struct, it will be set to 
// the first model in []models.
// If it is a slice, it will be set to the models with the correct type.
func SetStructModelField(obj interface{}, fieldName string, models []Model) error {
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
			if sliceType.Kind() == reflect.Struct {
				slice = reflect.Append(slice, reflect.ValueOf(model))	
			} else {
				slice = reflect.Append(slice, reflect.ValueOf(model))	
			}
		}

		field.Set(slice)
	} else {
		return errors.New("unsupported_field_type")
	}

	return nil
}

/**
 * ModelInfo struct and methods
 */

// Contains information about a single field of a Model.
type FieldInfo struct {
	PrimaryKey bool
	Ignore bool
	Name string
	Type reflect.Kind


	/**
	 * Relationship related fields
	 */
	 
	M2M bool
	M2MCollection string

	HasOne bool
	HasOneField string
	HasOneForeignField string

	BelongsTo bool
	BelongsToField string
	BelongsToForeignField string

	RelationItem Model
	RelationIsMany bool
}

// Contains information about a Model, including field info.
type ModelInfo struct {
	// Name of the struct field.
	PkField string

	Item Model
	ItemName string
	ItemCollection string

	FieldInfo map[string]*FieldInfo
}

// Builds the ModelInfo for a model and returns it.
// Returns an error for all failures.
func NewModelInfo(model Model) (*ModelInfo, DbError) {
	info := ModelInfo{
		Item: model,
		ItemName: reflect.ValueOf(model).Elem().Type().Name(),
		ItemCollection: model.Collection(),
		FieldInfo: make(map[string]*FieldInfo),
	}

	err := info.buildFieldInfo(reflect.ValueOf(model).Elem())
	if err != nil {
		return nil, Error{
			Code: "build_field_info_failed",
			Message: fmt.Sprintf("Could not build field info for %v: %v", info.ItemName, err.GetMessage()),
			Data: err,
		}
	}

	// Ensure primary key exists.
	for name := range info.FieldInfo {
		if info.FieldInfo[name].PrimaryKey {
			info.PkField = name
		}
	}
	if info.PkField == "" {
		// No explicit primary key found, check for ID field.
		if _, ok := info.FieldInfo["ID"]; ok {
			info.PkField = "ID"
		}
	}
	if info.PkField == "" {
		return nil, Error{
			Code: "primary_key_not_found",
			Message: fmt.Sprintf("Primary key could not be determined for model %v", info.ItemName),
		}
	}

	

	return &info, nil
}


func (m ModelInfo) GetPkName() string {
	return m.FieldInfo[m.PkField].Name
}

// Given a database field name, return the struct field name.
func (m ModelInfo) MapFieldName(name string) string {
	for key := range m.FieldInfo {
		if m.FieldInfo[key].Name == name {
			return key
		}
	}

	return ""
}

// Parse the information contained in a 'db:"xxx"' field tag.
func ParseFieldTag(tag string) (*FieldInfo, DbError)  {
	info := FieldInfo{}
	parts := strings.Split(tag, ";")
	for _, part := range parts {
		itemParts := strings.Split(part, ":")

		specifier := part
		var value string
		if len(itemParts) > 1 {
			specifier = itemParts[0]	
			value = itemParts[1]
		}

		switch specifier {
		case "primary_key":
			info.PrimaryKey = true
		case "-":
			info.Ignore = true
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
						Code: "invalid_has_one",
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
						Code: "invalid_belongs_to",
						Message: "Explicit belongs-to needs to be in format 'belongs-to:localField:foreignKey'",
					}
				}
				info.BelongsToField = itemParts[1]
				info.BelongsToForeignField = itemParts[2]
			}
		case "name":
			if value == "" {
				return nil, Error{
					Code: "invalid_name",
					Message: "name specifier must be in format name:the_name",
				}
			}

			info.Name = value
		}
	}

	return &info, nil
}

// Build the field information for the model.
func (info *ModelInfo) buildFieldInfo(modelVal reflect.Value) DbError {
	modelType := modelVal.Type()

	for i := 0; i < modelVal.NumField(); i++ {
		field := modelVal.Field(i)
		fieldType := modelType.Field(i)
		fieldKind := fieldType.Type.Kind()

		if fieldKind == reflect.Struct && fieldType.Anonymous {
			// Embedded struct. Find nested fields.
			if err := info.buildFieldInfo(field); err != nil {
				return err
			}
			continue
		}

		fieldInfo, err := ParseFieldTag(fieldType.Tag.Get("db"))	
		if err != nil {
			return err
		}

		if fieldInfo.Name == "" {
			fieldInfo.Name = CamelCaseToUnderscore(fieldType.Name)
		}

		// Find relationship type for structs and slices.

		if fieldKind == reflect.Struct {
			// Field is a direct struct.
			// RelationItem type is the struct.
			if relItem, ok := reflect.New(fieldType.Type).Interface().(Model); ok {
				fieldInfo.RelationItem = relItem
			}
		} else if fieldKind == reflect.Ptr {
			// Field is a pointer.
			// Check if it points to a Model.
			ptrType := fieldType.Type.Elem()
			if relItem, ok := reflect.New(ptrType).Interface().(Model); ok {
				// Points to a model.
				fieldInfo.RelationItem = relItem
			}
		} else if fieldKind == reflect.Slice {
			// Field is slice.
			// Check if slice items are models or pointers to models.
			sliceType := fieldType.Type.Elem()
			sliceKind := sliceType.Kind()

			if sliceKind == reflect.Struct {
				// Slice contains structs.
				// Same as above code for plain structs.
				if relItem, ok := reflect.New(sliceType).Interface().(Model); ok {
					fieldInfo.RelationItem = relItem
					fieldInfo.RelationIsMany = true
				}
			} else if sliceKind == reflect.Ptr {
				// Slice contains pointers. 
				// Check if it points to a model. Same as above for pointers.
				ptrType := sliceType.Elem()
				if relItem, ok := reflect.New(ptrType).Interface().(Model); ok {
					// Points to a model.
					fieldInfo.RelationItem = relItem
					fieldInfo.RelationIsMany = true
				}
			}
		}

		fieldInfo.Type = fieldType.Type.Kind()
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
		if err := buildRealtionShipInfo(models, models[key]); err != nil {
			return err
		}
	}

	return nil
}

// Recursive helper for building the relationship information. 
// Will properly analyze all embedded structs as well.
// WARNING: will panic on errors. 
func buildRealtionShipInfo(models map[string]*ModelInfo, model *ModelInfo) DbError {
	for name := range model.FieldInfo {
		fieldInfo := model.FieldInfo[name]

		if fieldInfo.RelationItem == nil {
			// Only process fields with a relation.
			continue
		}
		if fieldInfo.Ignore {
			// Ignored field.
			continue
		}

		modelName := reflect.ValueOf(model.Item).Elem().Type().Name()
		relatedItem := fieldInfo.RelationItem
		relatedName := reflect.ValueOf(relatedItem).Elem().Type().Name()
		relatedCollection := relatedItem.Collection()

		// Check that related model is contained in models info.
		if _, ok := models[relatedCollection]; !ok {
			return Error{
				Code: "missing_model_info",
				Message: fmt.Sprintf(
					"Model %v contains relationship %v, but relationship target %v was not registred with backend",
					modelName, name, relatedName),
			}
		}

		relatedInfo := models[relatedItem.Collection()]
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
						modelName, relatedName + "ID"),
				}
			}
			if _, ok := model.FieldInfo[fieldInfo.HasOneField]; !ok {
				return Error{
					Code: "has_one_field_missing",
					Message: fmt.Sprintf("Specified has-one field %v not found on model %v",
						fieldInfo.HasOneField, modelName),
				}
			}

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
						modelName, modelName + "ID"),
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
					Code: "belongs_to_field_missing",
					Message: fmt.Sprintf("Model %v has no field %v", modelName, fieldInfo.BelongsToField),
				}
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
 * Query parser functions.
 */

func ParseJsonQuery(collection string, js []byte) (*Query, DbError) {
	var data map[string]interface{}
	if err := json.Unmarshal(js, &data); err != nil {
		return nil, Error{
			Code: "invalid_json",
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
func ParseQuery(collection string, data map[string]interface{}) (*Query, DbError) {
	q := Q(collection)

	// First, Handle joins so query and field specification parsing can use 
	// join info.
	if rawJoins, ok := data["joins"]; ok {
		rawJoinSlice, ok := rawJoins.([]interface{})
		if !ok {
			return nil, Error{
				Code: "invalid_joins",
				Message: "Joins must be an array of strings",
			}
		}

		// Convert []interface{} joins to []string.

		joins := make([]string, 0)
		for _, rawJoin := range rawJoinSlice {
			join, ok := rawJoin.(string)
			if !ok {
				return nil, Error{
					Code: "invalid_joins",
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
				Code: "invalid_filters",
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
				Code: "invalid_fields",
				Message: "Fields specification must be an array",
			}
		}

		for _, rawField := range fields {
			field, ok := rawField.(string)
			if !ok {
				return nil, Error{
					Code: "invalid_fields",
					Message: "Fields specification must be an array of strings",
				}
			}

			parts := strings.Split(field, ".")
			if len(parts) > 1 {
				// Possibly a field on a joined model. Check if a parent join can be found.
				joinQ := q.GetJoin(strings.Join(parts[:len(parts) - 1], "."))
				if joinQ != nil {
					// Join query found, add field to the join query.
					joinQ.AddFields(parts[len(parts) - 1])
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
				Code: "limit_non_numeric",
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
				Code: "offset_non_numeric",
				Message: "Offset must be a number",
			}
		} else {
			q.Offset(int(offset))
		}
	}


	return q, nil
}

func parseQueryJoins(q *Query, joins []string, depth int) ([]string, DbError) {
	remaining := make([]string, 0)

	for _, name := range joins {
		parts := strings.Split(name, ".")
		joinDepth := len(parts)
		if joinDepth == depth {
			// The depth of the join equals to the one that should be processed, so do!
			if len(parts) > 1 {
				// Nested join! So try to retrieve the parent join query.
				joinQ := q.GetJoin(strings.Join(parts[:joinDepth - 1], "."))
				if joinQ == nil {
					// Parent join not found, obviosly an error.
					return nil, Error{
						Code: "invalid_nested_join",
						Message: fmt.Sprintf("Tried to join %v, but the parent join was not found", name),
					}
				}
				// Join the current join on the parent join.
				joinQ.Join(parts[len(parts) - 1])
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

func parseQueryFilters(q *Query, filters map[string]interface{}) DbError {
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
			Code: "unsupported_nor_query",
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
