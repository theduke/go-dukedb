package dukedb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/theduke/go-apperror"
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

	// Whether to embed the struct.
	// Embedding is dependant on the backend.
	// In relational databases, the data will be stored in a text field as json.
	// In document/graph databases it will be stored as a nested document.
	Embed bool

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
	BackendType       string

	/**
	 * Relationship related fields
	 */

	// Instance of the related struct.
	RelationItem interface{}

	// Collection name of the related struct.
	RelationCollection string

	// Wheter the relationship is many.
	RelationIsMany bool

	// Wheter to auto-persist this relationship. Defaults to true.
	RelationAutoPersist bool

	M2M           bool
	M2MCollection string

	HasOne             bool
	HasOneField        string
	HasOneForeignField string

	BelongsTo             bool
	BelongsToField        string
	BelongsToForeignField string
}

func NewFieldInfo() *FieldInfo {
	return &FieldInfo{
		RelationAutoPersist: true,
	}
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
func CreateModelInfo(model interface{}) (*ModelInfo, apperror.Error) {
	typ := reflect.TypeOf(model)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil, apperror.New("invalid_model_argument",
			fmt.Sprintf("Must use pointer to struct or struct, got %v", typ))
	}

	//backendModel, isBackendModel := model.(Model)

	collection, err := GetModelCollection(model)
	if err != nil {
		return nil, err
	}

	info := &ModelInfo{
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
		return nil, apperror.Wrap(err, "build_field_info_error",
			fmt.Sprintf("Could not build field info for %v", info.Name))
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

			// Only set unique to true if no unique-with was specified.
			if fieldInfo.UniqueWith == nil {
				fieldInfo.Unique = true
			}

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
		return nil, apperror.New("primary_key_not_found",
			fmt.Sprintf("Primary key could not be determined for model %v", info.Name))
	}

	return info, nil
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

// Tries to determine the backend name by checking struct field names, MarshalName and
// BackendName. Returns the backend field name, or an empty string if not found.
func (m ModelInfo) FindBackendFieldName(name string) string {
	if m.HasField(name) {
		return m.GetField(name).BackendName
	}

	for _, fieldInfo := range m.FieldInfo {
		if fieldInfo.BackendName == name {
			return name
		}
		if fieldInfo.MarshalName == name {
			return fieldInfo.BackendName
		}
	}

	return ""
}

// Tries to determine the struct field name by checking struct field names, MarshalName and
// BackendName. Returns the struct field name, or an empty string if not found.
func (m ModelInfo) FindStructFieldName(name string) string {
	if m.HasField(name) {
		return name
	}

	for _, fieldInfo := range m.FieldInfo {
		if fieldInfo.BackendName == name {
			return fieldInfo.Name
		}
		if fieldInfo.MarshalName == name {
			return fieldInfo.Name
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

func (m ModelInfo) FieldByMarshalName(name string) *FieldInfo {
	for key := range m.FieldInfo {
		if m.FieldInfo[key].MarshalName == name {
			return m.FieldInfo[key]
		}
	}

	return nil
}

// Parse the information contained in a 'db:"xxx"' field tag.
func ParseFieldTag(tag string) (*FieldInfo, apperror.Error) {
	info := NewFieldInfo()

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
				return nil, apperror.New("invalid_name", "name specifier must be in format name:the_name")
			}

			info.BackendName = value

		case "type":
			info.BackendType = value

		case "marshal-name":
			if value == "" {
				return nil, apperror.New("invalid_name", "name specifier must be in format marshal-name:the_name")
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

		case "index":
			if value == "" {
				// Set a default name for the index.
				// The buildFieldInfo function will create a proper name later.
				value = "index"
			}
			info.Index = value

		case "default":
			if value == "" {
				return nil, apperror.New("invalid_default", "default specifier must be in format default:value")
			}
			info.Default = value

		case "min":
			x, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, apperror.New("invalid_min", "min:xx must be a valid number")
			}
			info.Min = x

		case "max":
			x, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return nil, apperror.New("invalid_max", "max:xx must be a valid number")
			}
			if x == -1 {
				info.Max = 1000000000000
			} else {
				info.Max = x
			}

		case "unique-with":
			parts := strings.Split(value, ",")
			if parts[0] == "" {
				return nil, apperror.New("invalid_unique_with", "unique-with must be a comma-separated list of fields")
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
					return nil, apperror.New("invalid_has_one",
						"Explicit has-one needs to be in format 'has-one:localField:foreignKey'")
				}
				info.HasOneField = itemParts[1]
				info.HasOneForeignField = itemParts[2]
			}

		case "belongs-to":
			info.BelongsTo = true
			if value != "" {
				if len(itemParts) < 3 {
					return nil, apperror.New("invalid_belongs_to",
						"Explicit belongs-to needs to be in format 'belongs-to:localField:foreignKey'")
				}
				info.BelongsToField = itemParts[1]
				info.BelongsToForeignField = itemParts[2]
			}

		case "no-auto-persist":
			info.RelationAutoPersist = false
		}
	}

	return info, nil
}

// Build the field information for the model.
func (info *ModelInfo) buildFieldInfo(modelVal reflect.Value, embeddedName string) apperror.Error {
	modelType := modelVal.Type()

	// First build the info for embedded structs, since the random ordering of struct fields
	// by reflect might mean that an overwritting field is not picked up on, and the nested
	// field is put in FieldInfo instead.

	for i := 0; i < modelVal.NumField(); i++ {
		field := modelVal.Field(i)
		fieldType := modelType.Field(i)
		fieldKind := fieldType.Type.Kind()

		if fieldKind == reflect.Struct && fieldType.Anonymous {
			// Embedded struct. Find nested fields.
			if err := info.buildFieldInfo(field, fieldType.Name); err != nil {
				return err
			}
		}
	}

	for i := 0; i < modelVal.NumField(); i++ {
		fieldType := modelType.Field(i)
		fieldKind := fieldType.Type.Kind()

		// Ignore private fields.
		firstChar := fieldType.Name[0:1]
		if strings.ToLower(firstChar) == firstChar {
			continue
		}

		// Ignore embedded structs, which were handled above.
		if fieldKind == reflect.Struct && fieldType.Anonymous {
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

		// If index is set to the default value "index", fill in a proper name.
		if fieldInfo.Index == "index" {
			fieldInfo.Index = info.BackendName + "_" + fieldInfo.BackendName
		}

		info.FieldInfo[fieldType.Name] = fieldInfo
	}

	return nil
}

/**
 * Functions for analyzing the relationships between model structs.
 */

// Build the relationship information for the model after all fields have been analyzed.
func BuildAllRelationInfo(models map[string]*ModelInfo) apperror.Error {
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
func buildRelationshipInfo(models map[string]*ModelInfo, model *ModelInfo) apperror.Error {
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
				return apperror.New("has_one_field_not_determined",
					fmt.Sprintf("has-one specified on model %v, but field %v not found. Specify ID field.",
						modelName, relatedName+"ID"))
			}
			if _, ok := model.FieldInfo[fieldInfo.HasOneField]; !ok {
				msg := fmt.Sprintf("Specified has-one field %v not found on model %v", fieldInfo.HasOneField, modelName)
				return apperror.New("has_one_field_missing", msg)
			}

			// Ignore zero values to avoid inserts with 0.
			model.FieldInfo[fieldInfo.HasOneField].IgnoreIfZero = true

			if _, ok := relatedFields[fieldInfo.HasOneForeignField]; !ok {
				msg := fmt.Sprintf("has-one specified on model %v with foreign key %v which does not exist on target %v", modelName, fieldInfo.HasOneForeignField, relatedName)
				return apperror.New("has_one_foreign_field_missing", msg)
			}
		} else if fieldInfo.BelongsTo {
			if fieldInfo.BelongsToForeignField == "" {
				msg := fmt.Sprintf("belongs-to specified on model %v, but field %v not found. Specify ID field.", modelName, modelName+"ID")
				return apperror.New("belongs_to_foreign_field_not_determined", msg)
			}
			if _, ok := relatedFields[fieldInfo.BelongsToForeignField]; !ok {
				msg := fmt.Sprintf("Specified belongs-to field %v not found on model %v", fieldInfo.BelongsToForeignField, relatedName)
				return apperror.New("belongs_to_foreign_field_missing", msg)
			}

			if fieldInfo.BelongsToField == "" {
				fieldInfo.BelongsToField = model.PkField
			}

			if _, ok := model.FieldInfo[fieldInfo.BelongsToField]; !ok {
				msg := fmt.Sprintf("Model %v has no field %v", modelName, fieldInfo.BelongsToField)
				return apperror.New("belongs_to_field_missing", msg)
			}

			model.FieldInfo[fieldInfo.BelongsToField].IgnoreIfZero = true
		} else if fieldInfo.M2M {
			if fieldInfo.M2MCollection == "" {
				fieldInfo.M2MCollection = model.BackendName + "_" + relatedInfo.BackendName
			}
		}

		if !(fieldInfo.HasOne || fieldInfo.BelongsTo || fieldInfo.M2M) {
			msg := fmt.Sprintf("Model %v has relationship to %v in field %v, but could not determine the neccessary relation fields.", modelName, relatedName, name)
			return apperror.New("relationship_not_determined", msg)
		}
	}

	return nil
}
