package dukedb

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/theduke/go-apperror"
)

/**
 * ModelInfo.
 */

// ModelInfo describes a model managed by DukeDB.
type ModelInfo interface {
	// Item returns an instance of the model struct.
	Item() interface{}

	// StructName returns the unqualified struct name.
	StructName() string

	// FullStructName returns the fully qualified name of the struct.
	// For example: mypackage.MyModel.
	FullStructName() string

	// Collection returns the collection name.
	Collection() string

	// BackendName returns the collection name used by the backend.
	BackendName() string

	// MarshalName returns the model name to be used when marshalling.
	MarshalName() string

	/**
	 * Fields.
	 */

	Attributes() map[string]Attribute
	SetAttributes(attrs map[string]Attribute)

	Attribute(name string) Attribute
	HasAttribute(name string) bool
	PkAttribute() Attribute
	// FindAttribute tries to find a field by checking its Name, BackendName and MarshalName.
	FindAttribute(name string) Attribute

	/**
	 * Relations.
	 */

	Relations() map[string]Relation
	SetRelations(rels map[string]Relation)

	Relation(name string) Relation
	HasRelation(name string) bool
	FindRelation(name string) Relation
}

/**
 * modelInfo.
 */

type modelInfo struct {
	item           interface{}
	structName     string
	fullStructName string
	collection     string
	backendName    string
	marshalName    string

	// transientFields store fields which are not determined to be either
	// a relationship or an attribute.
	// See buildFields() for an explanation.
	transientFields map[string]*field

	attributes map[string]Attribute
	relations  map[string]Relation
}

/**
 * Item.
 */

func (m *modelInfo) Item() interface{} {
	return m.item
}

func (m *modelInfo) SetItem(val interface{}) {
	m.item = val
}

/**
 * StructName.
 */

func (m *modelInfo) StructName() string {
	return m.structName
}

func (m *modelInfo) SetStructName(val string) {
	m.structName = val
}

/**
 * FullStructName.
 */

func (m *modelInfo) FullStructName() string {
	return m.fullStructName
}

func (m *modelInfo) SetFullStructName(val string) {
	m.fullStructName = val
}

/**
 * Collection.
 */

func (m *modelInfo) Collection() string {
	return m.collection
}

func (m *modelInfo) SetCollection(val string) {
	m.collection = val
}

/**
 * BackendName.
 */

func (m *modelInfo) BackendName() string {
	return m.backendName
}

func (m *modelInfo) SetBackendName(val string) {
	m.backendName = val
}

/**
 * MarshalName.
 */

func (m *modelInfo) MarshalName() string {
	return m.marshalName
}

func (m *modelInfo) SetMarshalName(val string) {
	m.marshalName = val
}

/**
 * Attributes.
 */

func (m *modelInfo) Attributes() map[string]Attribute {
	return m.attributes
}

func (m *modelInfo) SetAttributes(attrs map[string]Attribute) {
	m.attributes = attrs
}

func (m *modelInfo) HasAttribute(name string) bool {
	_, ok := m.attributes[name]
	return ok
}

func (m *modelInfo) Attribute(name string) Attribute {
	return m.attributes[name]
}

func (m *modelInfo) PkAttribute() Attribute {
	for _, attr := range m.attributes {
		if attr.IsPrimaryKey() {
			return attr
		}
	}

	return nil
}

// FindField tries to find a field by checking its Name, BackendName and MarshalName.
func (m *modelInfo) FindAttribute(name string) Attribute {
	for _, attr := range m.attributes {
		if attr.Name() == name || attr.BackendName() == name || attr.MarshalName() == name {
			return attr
		}
	}

	return nil
}

/**
 * Relations.
 */
func (m *modelInfo) Relations() map[string]Relation {
	return m.relations
}

func (m *modelInfo) SetRelations(rels map[string]Relation) {
	m.relations = rels
}

func (m *modelInfo) HasRelation(name string) bool {
	_, ok := m.relations[name]
	return ok
}

func (m *modelInfo) Relation(name string) Relation {
	return m.relations[name]
}

func (m *modelInfo) FindRelation(name string) Relation {
	for _, relation := range m.relations {
		if relation.Name() == name || relation.BackendName() == name || relation.MarshalName() == name {
			return relation
		}
	}
	return nil
}

/**
 * ModelInfo.
 */

// Builds the ModelInfo for a model and returns it.
func BuildModelInfo(model interface{}) (ModelInfo, apperror.Error) {
	typ := reflect.TypeOf(model)
	modelVal := reflect.ValueOf(model)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
		modelVal = modelVal.Elem()
	}

	if typ.Kind() != reflect.Struct {
		return nil, apperror.New("invalid_model_argument",
			fmt.Sprintf("Must use pointer to struct or struct, got %v", typ))
	}

	collection, err := GetModelCollection(model)
	if err != nil {
		return nil, err
	}

	info := &modelInfo{
		item:           reflect.New(typ).Interface(),
		fullStructName: typ.PkgPath() + "." + typ.Name(),
		structName:     typ.Name(),
		collection:     collection,

		transientFields: make(map[string]*field),
		attributes:      make(map[string]Attribute),
		relations:       make(map[string]Relation),
	}

	// Determine BackendName.
	info.backendName = info.collection
	// If model implements .BackendName() call it to determine backend name.
	if nameHook, ok := model.(ModelBackendNameHook); ok {
		name := nameHook.BackendName()
		if name == "" {
			panic(fmt.Sprintf("%v.BackendName() returned an empty string.", info.FullStructName()))
		}
		info.backendName = name
	}

	// Dertermine MarshalName.
	info.marshalName = info.collection
	if nameHook, ok := model.(ModelMarshalNameHook); ok {
		name := nameHook.MarshalName()
		if name == "" {
			panic(fmt.Sprintf("%v.MarshalName() returned an empty string.", info.FullStructName()))
		}
		info.marshalName = name
	}

	err = info.buildFields(modelVal, "")
	if err != nil {
		return nil, apperror.Wrap(err, "build_field_info_error",
			fmt.Sprintf("Could not build field info for %v", info.StructName()))
	}

	// Ensure primary key exists.
	if info.PkAttribute() == nil {
		// No explicit primary key found, check for ID field.
		if attr := info.Attribute("ID"); attr != nil {
			attr.SetIsPrimaryKey(true)
		} else if attr := info.Attribute("Id"); attr != nil {
			attr.SetIsPrimaryKey(true)
		}
	}

	for _, attr := range info.attributes {
		if attr.IsPrimaryKey() {
			attr.SetIsRequired(true)
			attr.SetIgnoreIfZero(true)

			// Only set unique to true if no unique-with was specified.
			if attr.IsUniqueWith() == nil {
				attr.SetIsUnique(true)
			}

			// On numeric fields, activate autoincrement.
			// TODO: allow a way to disable autoincrement with a tag.
			if IsNumericKind(attr.Type().Kind()) {
				attr.SetAutoIncrement(true)
			}
		}
	}

	return info, nil
}

// Build the field information for the model.
func (info *modelInfo) buildFields(modelVal reflect.Value, embeddedName string) apperror.Error {
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
			if err := info.buildFields(field, fieldType.Name); err != nil {
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

		// Try to determine if the field contains structs in any way.
		// It might be:
		//  * a struct
		//  * a pointer to a struct
		//  * a slice of structs
		//  * a slice of pointers to structs
		var structType reflect.Type

		if fieldKind == reflect.Struct {
			// Field is a direct struct.
			structType = fieldType.Type
		} else if fieldKind == reflect.Ptr {
			// Field is a pointer.
			ptrType := fieldType.Type.Elem()

			// Only process pointers to structs.
			if ptrType.Kind() == reflect.Struct {
				structType = ptrType
			}
		} else if fieldKind == reflect.Slice {
			// Field is slice.
			// Check if slice items are structs or pointers to structs.
			sliceType := fieldType.Type.Elem()
			sliceKind := sliceType.Kind()

			if sliceKind == reflect.Struct {
				// Slice contains structs.
				structType = sliceType
			} else if sliceKind == reflect.Ptr {
				// Slice contains pointers.
				ptrType := sliceType.Elem()

				// Only process structs.
				if ptrType.Kind() == reflect.Struct {
					structType = ptrType
				}
			}
		}

		// Build the base field.
		field := &field{
			typ:                 fieldType.Type,
			name:                fieldType.Name,
			structType:          structType,
			embeddingStructName: embeddedName,
			backendName:         CamelCaseToUnderscore(fieldType.Name),
			marshalName:         LowerCaseFirst(fieldType.Name),
		}
		if structType != nil {
			field.structName = structType.PkgPath() + "." + structType.Name()
		}

		if err := field.parseTag(fieldType.Tag.Get("db")); err != nil {
			return apperror.Wrap(err, "invalid_field_tag", fmt.Sprintf("The field %v has an invalid db tag", field.name))
		}

		// If tag specifies ignore, we can skip this field now.
		if field.tag.ignore {
			continue
		}

		if structType == nil {
			// No struct type found, so this field cannot possibly be a
			// relation and must be an attribute.
			// Construct attribute now.

			attr := buildAttribute(*field)
			// Add the attribute to attributes map.
			info.attributes[attr.Name()] = attr
		} else {
			// The field points to a struct, so we can not be sure if it is a
			// relationship or an attribute.
			// This can only be determined in AnalyzeRelations() once all models
			// have been registered.
			// To get around this, we store the field as a transientField.
			// The transientFields will be split into attributes and relations
			// in AnalyzeRelations().

			info.transientFields[field.name] = field
		}
	}

	return nil
}

/**
 * ModelInfos.
 */

// ModelInfos is a container collecting ModelInfo for a backend with some
// convenience methods.
type ModelInfos interface {
	Get(collection string) ModelInfo
	Add(info ModelInfo)
	Has(collection string) bool

	// Find looks for a model by checking Collection,
	// BackendName and MarshalName.
	Find(name string) ModelInfo

	AnalyzeRelations() apperror.Error
}

type modelInfos map[string]ModelInfo

// Ensure modelInfos implements ModelInfos.
var _ ModelInfos = (*modelInfos)(nil)

func (i modelInfos) Get(collection string) ModelInfo {
	return i[collection]
}

func (i modelInfos) Add(info ModelInfo) {
	i[info.Collection()] = info
}

func (i modelInfos) Has(collection string) bool {
	_, ok := i[collection]
	return ok
}

func (i modelInfos) Find(name string) ModelInfo {
	for _, info := range i {
		if info.Collection() == name || info.BackendName() == name || info.MarshalName() == name {
			return info
		}
	}
	return nil
}

/**
 * Functions for analyzing the relationships between model structs.
 */

func (m modelInfos) AnalyzeRelations() apperror.Error {
	for _, info := range m {
		if err := m.analyzeModelRelations(info.(*modelInfo)); err != nil {
			return err
		}
	}
	return nil
}

// Recursive helper for building the relationship information.
// Will properly analyze all embedded structs as well.
// All transientFields will be checked, and split intro attributes or
// relations.
func (m modelInfos) analyzeModelRelations(model *modelInfo) apperror.Error {
	for fieldName, field := range model.transientFields {
		relatedItem := reflect.New(field.structType)

		// Try to determine the collection of the related struct.
		relatedCollection, err := GetModelCollection(relatedItem)
		if err != nil {
			panic(fmt.Sprintf("Could not determine collection name for struct %v", field.structName))
		}

		relatedInfo := m.Get(relatedCollection)
		if relatedInfo == nil {
			// Related struct type was not registered.
			// This is not a relation, but an attribute.
			// We need to build the attribute now and add it to the attributes
			// map.

			attr := buildAttribute(*field)
			model.attributes[attr.Name()] = attr
			continue
		}

		// Field is a relation, so build the relation struct.
		relation := buildRelation(*field)
		relation.SetModel(model)
		relation.SetRelatedModel(relatedInfo)

		modelName := model.StructName()
		relatedName := relatedInfo.StructName()

		// If an explicit relation type was specified, verify the fields.
		if relation.RelationType() != "" {
			// Relation was set explicitly. Verify fields.

			// Check m2m.
			if relation.RelationType() == RELATION_TYPE_M2M {
				if relation.LocalField() == "" || relation.ForeignField() == "" {
					// Set fields to respective PKs.
					relation.SetLocalField(model.PkAttribute().Name())
					relation.SetForeignField(relatedInfo.PkAttribute().Name())
				}

				// Valid m2m, all done.
				continue
			}

			// Checks for has-many, has-one and belongs-to can be done
			// generically, since we only need to verify that the fields exist.
			if !model.HasAttribute(relation.LocalField()) {
				return apperror.New(
					"invalid_relation_local_field",
					fmt.Sprintf("Invalid %v relation spec: field %v.%v does not exist", relation.RelationType(), modelName, relation.LocalField()))
			}

			if !relatedInfo.HasAttribute(relation.ForeignField()) {
				return apperror.New(
					"invalid_relation_foreign_field",
					fmt.Sprintf("Invalid %v relation spec: field %v.%v does not exist", relation.RelationType(), relatedName, relation.ForeignField()))
			}

			// All is verified, nothing more to do.
			continue
		}

		// No relationship is set, try to determine it.

		if relation.IsMany() {
			// Since relation is many, it must be has-many, since m2m needs
			// to be set explicitly.
			relation.SetRelationType(RELATION_TYPE_HAS_MANY)
			// TODO: determine fields.
			continue
		}

		// Relationship must be either has-one or belongs-to.
		// Try to determine.

		// Check has-one first.

		// Try to fiend ID field.
		// we check: fieldNameID, fieldNameId, relationNameID and relationNameId
		relField := fieldName + "ID"
		if !model.HasAttribute(relField) {
			relField = fieldName + "Id"
			if !model.HasAttribute(relField) {
				relField = relatedName + "ID"
				if !model.HasAttribute(relField) {
					relField = relatedName + "Id"
					if !model.HasAttribute(relField) {
						// No appropriate field for has-one found!.
						relField = ""
					}
				}
			}
		}

		if relField != "" {
			// Found a appropriate field for has-one.
			relation.SetRelationType(RELATION_TYPE_HAS_ONE)
			relation.SetLocalField(relField)
			relation.SetForeignField(relatedInfo.PkAttribute().Name())
			// Valid has-one, all done.
			continue
		}

		// Not has-one, so must be belongs to.
		// Try to find foreign key field.
		// We check modelNameID, modelNameId
		relField = modelName + "ID"
		if !relatedInfo.HasAttribute(relField) {
			relField = modelName + "Id"
			if !relatedInfo.HasAttribute(relField) {
				relField = ""
			}
		}

		if relField != "" {
			// Found an appropriate field for belongs-to!
			relation.SetRelationType(RELATION_TYPE_BELONGS_TO)
			relation.SetForeignField(relField)
			relation.SetLocalField(model.PkAttribute().Name())

			// Valid belongs-to. All done.
			continue
		}

		// If code reaches this point, relationship type could nto be determined.
		msg := fmt.Sprintf("Model %v has relationship to %v in field %v, but could not determine the relationship type. Specify explicitly with has-one/has-many/belongs-to/m2m", modelName, relatedName, fieldName)
		return apperror.New("relationship_not_determined", msg)
	}

	return nil
}
