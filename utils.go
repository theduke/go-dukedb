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



func ParseFieldTag(tag string) *FieldInfo  {
	info := FieldInfo{}

	parts := strings.Split(tag, ";")
	for _, part := range parts {
		itemParts := strings.Split(part, ":")

		specifier := part
		var value string
		if len(itemParts) == 2 {
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
				subVal := strings.Split(value, ":")
				if len(subVal) < 2 {
					panic(fmt.Sprintf("Explicit belongs-to needs to be in format 'belongs-to:localField:foreignKey'"))
				}
				info.HasOneField = subVal[0]
				info.HasOneForeignField = subVal[1]
			}
		case "belongs-to":
			info.BelongsTo = true
			if value != "" {
				subVal := strings.Split(value, ":")
				if len(subVal) < 2 {
					panic(fmt.Sprintf("Explicit belongs-to needs to be in format 'belongs-to:localField:foreignKey'"))
				}
				info.BelongsToField = subVal[0]
				info.BelongsToForeignField = subVal[1]
			}
		}

		if value != "" {
			switch specifier {
			case "name":
				info.Name = value
			}
		}
	}

	return &info
}

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
	RealtionIsMany bool
}

type ModelInfo struct {
	// Name of the struct field.
	PkField string

	Item Model
	ItemName string
	ItemCollection string

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
		if m.FieldInfo[key].Name == name {
			return key
		}
	}

	return ""
}

func NewModelInfo(model Model) (*ModelInfo, error) {
	info := ModelInfo{
		Item: model,
		ItemName: reflect.ValueOf(model).Elem().Type().Name(),
		ItemCollection: model.GetCollection(),
		FieldInfo: make(map[string]*FieldInfo),
	}

	info.buildFieldInfo(reflect.ValueOf(model).Elem())

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
		panic(fmt.Sprintf("Primary key could not be determined for model %v", reflect.ValueOf(model)))
	}

	

	return &info, nil
}

func (info *ModelInfo) buildFieldInfo(modelVal reflect.Value) {
	modelType := modelVal.Type()

	for i := 0; i < modelVal.NumField(); i++ {
		field := modelVal.Field(i)
		fieldType := modelType.Field(i)
		fieldKind := fieldType.Type.Kind()

		if fieldKind == reflect.Struct && fieldType.Anonymous {
			// Embedded struct. Find nested fields.
			info.buildFieldInfo(field)
			continue
		}

		fieldInfo := ParseFieldTag(fieldType.Tag.Get("db"))	
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
					fieldInfo.RealtionIsMany = true
				}
			} else if sliceKind == reflect.Ptr {
				// Slice contains pointers. 
				// Check if it points to a model. Same as above for pointers.
				ptrType := sliceType.Elem()
				if relItem, ok := reflect.New(ptrType).Interface().(Model); ok {
					// Points to a model.
					fieldInfo.RelationItem = relItem
					fieldInfo.RealtionIsMany = true
				}
			}
		}

		fieldInfo.Type = fieldType.Type.Kind()
		info.FieldInfo[fieldType.Name] = fieldInfo
	}
}

func BuildAllRelationInfo(models map[string]*ModelInfo) {
	for key := range models {
		buildRealtionShipInfo(models, models[key])
	}
}

func buildRealtionShipInfo(models map[string]*ModelInfo, model *ModelInfo) {
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
		relatedCollection := relatedItem.GetCollection()

		// Check that related model is contained in models info.
		if _, ok := models[relatedCollection]; !ok {
			panic(fmt.Sprintf(
				"Model %v contains relationship %v, but relationship target %v was not registred with backend",
				modelName, name, relatedName))
		}

		relatedInfo := models[relatedItem.GetCollection()]
		relatedFields := relatedInfo.FieldInfo

		if !(fieldInfo.BelongsTo || fieldInfo.HasOne || fieldInfo.M2M) {
			// No explicit relationship defined. Try to determine it.	

			// Can be either HasOne or BelongsTo, since m2m needs to be explicitly specified.

			// Check for HasOne first.
			if !fieldInfo.RealtionIsMany {
				// Try to fiend ID field.
				relField := relatedName + "ID"
				if _, ok := model.FieldInfo[relField]; ok {
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
				panic(fmt.Sprintf("has-one specified on model %v, but field %v not found. Specify ID field.",
					modelName, relatedName + "ID"))
			}
			if _, ok := model.FieldInfo[fieldInfo.HasOneField]; !ok {
				panic(fmt.Sprintf("Specified has-one field %v not found on model %v",
					fieldInfo.HasOneField, modelName))
			}

			if _, ok := relatedFields[fieldInfo.HasOneForeignField]; !ok {
				panic(fmt.Sprintf(
					"has-one specified on model %v with foreign key %v which does not exist on target %v",
					modelName, fieldInfo.HasOneForeignField, relatedName))
			}
		} else if fieldInfo.BelongsTo {
			if fieldInfo.BelongsToForeignField == "" {
				panic(fmt.Sprintf("belongs-to specified on model %v, but field %v not found. Specify ID field.",
					modelName, modelName + "ID"))
			}
			if _, ok := relatedFields[fieldInfo.BelongsToForeignField]; !ok {
				panic(fmt.Sprintf("Specified belongs-to field %v not found on model %v",
					fieldInfo.BelongsToForeignField, relatedName))
			}

			if fieldInfo.BelongsToField == "" {
				fieldInfo.BelongsToField = model.PkField
			}	

			if _, ok := model.FieldInfo[fieldInfo.BelongsToField]; !ok {
				panic(fmt.Sprintf("Model %v has no field %v", modelName, fieldInfo.BelongsToField))
			}
		}

		if !(fieldInfo.HasOne || fieldInfo.BelongsTo || fieldInfo.M2M) {
			panic(fmt.Sprintf("Model %v has relationship to %v in field %v, but could not determine the neccessary relation fields.",
				modelName, relatedName, name))
		}
	}
}

