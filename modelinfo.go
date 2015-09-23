package dukedb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

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
func CreateModelInfo(model interface{}) (*ModelInfo, DbError) {
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

	//backendModel, isBackendModel := model.(Model)

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
		if name != "" {
			info.BackendName = name
		}
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
