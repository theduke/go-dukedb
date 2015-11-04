package dukedb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/theduke/go-reflector"

	"github.com/theduke/go-apperror"
	. "github.com/theduke/go-dukedb/expressions"
	"github.com/theduke/go-utils"
)

/**
 * ModelInfo.
 */

/**
 * modelInfo.
 */

type ModelInfo struct {
	item      interface{}
	reflector *reflector.StructReflector

	structName     string
	fullStructName string
	collection     string
	backendName    string
	marshalName    string

	// transientFields store fields which are not determined to be either
	// a relationship or an attribute.
	// See buildFields() for an explanation.
	transientFields map[string]*Field

	attributes map[string]*Attribute
	relations  map[string]*Relation
}

/**
 * StructReflector.
 */

func (m *ModelInfo) Reflector() *reflector.StructReflector {
	return m.reflector
}

/**
 * Item.
 */

func (m *ModelInfo) Item() interface{} {
	return m.item
}

func (m *ModelInfo) SetItem(val interface{}) {
	m.item = val
}

/**
 * StructName.
 */

func (m *ModelInfo) StructName() string {
	return m.structName
}

func (m *ModelInfo) SetStructName(val string) {
	m.structName = val
}

/**
 * FullStructName.
 */

func (m *ModelInfo) FullStructName() string {
	return m.fullStructName
}

func (m *ModelInfo) SetFullStructName(val string) {
	m.fullStructName = val
}

/**
 * Collection.
 */

func (m *ModelInfo) Collection() string {
	return m.collection
}

func (m *ModelInfo) SetCollection(val string) {
	m.collection = val
}

/**
 * BackendName.
 */

func (m *ModelInfo) BackendName() string {
	return m.backendName
}

func (m *ModelInfo) SetBackendName(val string) {
	m.backendName = val
}

/**
 * MarshalName.
 */

func (m *ModelInfo) MarshalName() string {
	return m.marshalName
}

func (m *ModelInfo) SetMarshalName(val string) {
	m.marshalName = val
}

func (m *ModelInfo) New() interface{} {
	return m.reflector.New().Interface()
}

func (m *ModelInfo) NewReflector() *reflector.StructReflector {
	return m.reflector.New()
}

func (m *ModelInfo) NewSlice() *reflector.SliceReflector {
	return m.reflector.Value().NewSlice()
}

/**
 * Attributes.
 */

func (m *ModelInfo) Attributes() map[string]*Attribute {
	return m.attributes
}

func (m *ModelInfo) SetAttributes(attrs map[string]*Attribute) {
	m.attributes = attrs
}

func (m *ModelInfo) HasAttribute(name string) bool {
	_, ok := m.attributes[name]
	return ok
}

func (m *ModelInfo) Attribute(name string) *Attribute {
	return m.attributes[name]
}

func (m *ModelInfo) PkAttribute() *Attribute {
	for _, attr := range m.attributes {
		if attr.IsPrimaryKey() {
			return attr
		}
	}

	return nil
}

// FindField tries to find a field by checking its Name, BackendName and MarshalName.
func (m *ModelInfo) FindAttribute(name string) *Attribute {
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
func (m *ModelInfo) Relations() map[string]*Relation {
	return m.relations
}

func (m *ModelInfo) SetRelations(rels map[string]*Relation) {
	m.relations = rels
}

func (m *ModelInfo) HasRelation(name string) bool {
	_, ok := m.relations[name]
	return ok
}

func (m *ModelInfo) Relation(name string) *Relation {
	return m.relations[name]
}

func (m *ModelInfo) FindRelation(name string) *Relation {
	for _, relation := range m.Relations() {
		if relation.Name() == name || relation.BackendName() == name || relation.MarshalName() == name {
			return relation
		}
	}
	return nil
}

// Builds the ModelInfo for a model and returns it.
func BuildModelInfo(model interface{}) (*ModelInfo, apperror.Error) {
	structReflector, err := reflector.Reflect(model).Struct()
	if err != nil {
		return nil, apperror.New("invalid_model_argument",
			fmt.Sprintf("Must use pointer to struct or struct, got %v", reflect.TypeOf(model)))
	}

	collection, err2 := GetModelCollection(model)
	if err != nil {
		return nil, err2
	}

	info := &ModelInfo{
		reflector:      structReflector,
		item:           structReflector.New().Value().Interface(),
		fullStructName: structReflector.FullName(),
		structName:     structReflector.Name(),
		collection:     collection,

		transientFields: make(map[string]*Field),
		attributes:      make(map[string]*Attribute),
		relations:       make(map[string]*Relation),
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

	err = info.buildFields(structReflector, "")
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
			if reflector.IsNumericKind(attr.Type().Kind()) {
				attr.SetAutoIncrement(true)
			}
		}
	}

	return info, nil
}

// Build the field information for the model.
func (info *ModelInfo) buildFields(modelVal *reflector.StructReflector, embeddedName string) apperror.Error {
	// First build the info for embedded structs, since the random ordering of struct fields
	// by reflect might mean that an overwritting field is not picked up on, and the nested
	// field is put in FieldInfo instead.

	for _, refl := range modelVal.EmbeddedFields() {
		if err := info.buildFields(refl, refl.Name()); err != nil {
			return err
		}
	}

	for name, fieldInfo := range modelVal.FieldInfo() {
		// Ignore embedded fields, since they were handled above.
		if fieldInfo.Anonymous {
			continue
		}

		// Ignore private fields.
		firstChar := fieldInfo.Name[0:1]
		if strings.ToLower(firstChar) == firstChar {
			continue
		}

		// Try to determine if the field contains structs in any way.
		// It might be:
		//  * a struct
		//  * a pointer to a struct
		//  * a slice of structs
		//  * a slice of pointers to structs
		var structType reflect.Type

		fieldR := modelVal.Field(name)
		if fieldR.IsStruct() {
			structType = fieldR.Type()
		} else if fieldR.IsStructPtr() {
			structType = fieldR.Type().Elem()
		} else if fieldR.IsSlice() {
			sliceItemType := fieldR.Type().Elem()
			if sliceItemType.Kind() == reflect.Struct {
				structType = sliceItemType
			} else if sliceItemType.Kind() == reflect.Ptr && sliceItemType.Elem().Kind() == reflect.Struct {
				structType = sliceItemType.Elem()
			}
		}

		// Build the base field.
		field := &Field{
			typ:                 fieldInfo.Type,
			name:                fieldInfo.Name,
			structType:          structType,
			embeddingStructName: embeddedName,
			backendName:         utils.CamelCaseToUnderscore(fieldInfo.Name),
			marshalName:         utils.LowerCaseFirst(fieldInfo.Name),
		}

		if name == "Tags" {
			fmt.Sprintf("tags: %+v\n", field)
		}
		if structType != nil {
			field.structName = structType.PkgPath() + "." + structType.Name()
		}

		if err := field.parseTag(fieldInfo.Tag.Get("db")); err != nil {
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

			attr := buildAttribute(field)
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

func (info *ModelInfo) DetermineModelId(model interface{}) (interface{}, apperror.Error) {
	if hook, ok := model.(ModelIDGetterHook); ok {
		return hook.GetID(), nil
	}

	r, err := reflector.Reflect(model).Struct()
	if err != nil {
		return nil, apperror.Wrap(err, "invalid_model")
	}
	field := r.Field(info.PkAttribute().Name())

	if field.IsZero() {
		return nil, nil
	}
	return field.Interface(), nil
}

func (info *ModelInfo) MustDetermineModelId(model interface{}) interface{} {
	id, err := info.DetermineModelId(model)
	if err != nil {
		panic(err)
	}
	return id
}

// Determine the  ID for a model and convert it to string.
func (info *ModelInfo) DetermineModelStrId(model interface{}) (string, apperror.Error) {
	if hook, ok := model.(ModelStrIDGetterHook); ok {
		return hook.GetStrID(), nil
	}

	id, err := info.DetermineModelId(model)
	if err != nil {
		return "", err
	}

	if reflector.Reflect(id).IsZero() {
		return "", nil
	}

	return fmt.Sprint(id), nil
}

// Determine the  ID for a model and convert it to string. Panics on error.
func (info *ModelInfo) MustDetermineModelStrId(model interface{}) string {
	id, err := info.DetermineModelStrId(model)
	if err != nil {
		panic(fmt.Sprintf("Could not determine id for model: %v", err))
	}

	return id
}

func (info *ModelInfo) ModelHasId(model interface{}) (bool, apperror.Error) {
	id, err := info.DetermineModelStrId(model)
	if err != nil {
		return false, err
	}
	return id != "", nil
}

func (info *ModelInfo) SetModelId(model, id interface{}) apperror.Error {
	// If ID is string, check if model implements SetStrID.
	if strId, ok := id.(string); ok {
		if hook, ok := model.(ModelStrIDSetterHook); ok {
			// String id and hook implemented, so use it.
			err := hook.SetStrID(strId)
			if err != nil {
				return apperror.Wrap(err, "model_set_id_error")
			}
		}
	}

	// Check if model implements SetID.
	// If so, use it.
	if hook, ok := model.(ModelIDSetterHook); ok {
		err := hook.SetID(id)
		if err != nil {
			return apperror.Wrap(err, "model_set_id_error")
		}
	}

	r, err := reflector.Reflect(model).Struct()
	if err != nil {
		return apperror.Wrap(err, "invalid_model")
	}
	if err := r.SetFieldValue(info.PkAttribute().Name(), id, true); err != nil {
		return apperror.Wrap(err, err.Error(),
			fmt.Sprintf("Could not set %v.%v to value %v: %v", info.Collection(), info.PkAttribute().Name(), id))
	}
	return nil
}

func (info *ModelInfo) ModelToMap(model interface{}, forBackend, marshal bool, includeRelations bool) (map[string]interface{}, apperror.Error) {
	data := make(map[string]interface{})

	r, err := reflector.Reflect(model).Struct()
	if err != nil {
		return nil, apperror.New("invalid_model")
	}

	// Handle regular fields.
	for fieldName, fieldInfo := range info.Attributes() {
		field := r.Field(fieldName)
		if field.IsPtr() && !field.IsZero() {
			field = field.Elem()
		}

		// Ignore zero values if specified.
		if fieldInfo.IgnoreIfZero() && field.IsZero() {
			continue
		}

		val := field.Interface()

		if forBackend && fieldInfo.BackendMarshal() {
			js, err := json.Marshal(val)
			if err != nil {
				return nil, &apperror.Err{
					Code:    "marshal_error",
					Message: fmt.Sprintf("Could not marshal %v.%v to json: %v", info.StructName(), fieldName, err),
				}
			}
			val = js
		}

		name := fieldName
		if forBackend {
			name = fieldInfo.BackendName()
		} else if marshal {
			name = fieldInfo.MarshalName()
		}

		data[name] = val
	}

	if !includeRelations {
		return data, nil
	}

	// TODO: write code for including relation data!
	/*
		for fieldName, relation := range info.Relations() {

		}
	*/

	return data, nil
}

func (info *ModelInfo) UpdateModelFromData(model interface{}, data map[string]interface{}) apperror.Error {
	var r *reflector.StructReflector
	if x, ok := model.(*reflector.StructReflector); ok {
		r = x
	} else {
		x, err := reflector.Reflect(model).Struct()
		if err != nil {
			return apperror.Wrap(err, "invalid_model")
		}
		r = x
	}

	nestedData := make(map[string]map[string]interface{})

	for key, val := range data {
		if val == nil {
			continue
		}

		attr := info.FindAttribute(key)
		if attr != nil {
			// Key is a regular attribute.

			// Check if it is a marshalled attribute.
			if attr.BackendMarshal() {
				// Marshalled attribute, so try to unmarshal it.
				var js []byte
				if slice, ok := val.([]uint8); ok {
					js = []byte(slice)
				} else if str, ok := val.(string); ok {
					js = []byte(str)
				}
				if js != nil {
					if err := json.Unmarshal(js, r.Field(attr.Name()).Addr().Interface()); err != nil {
						return apperror.Wrap(err, "json_unmarshal_error")
					}
					continue
				}
			}

			if err := r.SetFieldValue(attr.Name(), val, true); err != nil {
				msg := fmt.Sprintf("Data for field %v (%v) could not be converted to %v", attr.Name(), val, attr.Type())
				return apperror.Wrap(err, "unconvertable_field_value", msg)
			}
		} else {
			// Check if key might be relation data.
			left, right := utils.StrSplitLeft(key, ".")
			relation := info.FindRelation(left)

			if relation != nil {
				if !relation.IsMany() {
					if mapData, ok := val.(map[string]interface{}); ok {
						// Value is map, so just store it in nested data.
						nestedData[relation.Name()] = mapData
					} else {
						if _, ok := nestedData[relation.Name()]; !ok {
							nestedData[relation.Name()] = make(map[string]interface{})
						}
						nestedData[relation.Name()][right] = val
					}
				}
			}
		}
	}

	// Process nested data.
	for relationName, data := range nestedData {
		relation := info.Relation(relationName)
		nestedR := r.Field(relationName).MustStruct()

		if err := relation.RelatedModel().UpdateModelFromData(nestedR, data); err != nil {
			return err
		}
	}

	return nil
}

func (info *ModelInfo) BuildCreateStmt(withReferences bool) *CreateCollectionStmt {
	fieldsMap := make(map[string]*FieldExpr, 0)
	constraints := make([]Expression, 0)

	for name, attr := range info.Attributes() {
		fieldsMap[name] = attr.BuildFieldExpression()

		// Add unique fields constraint to collection if specified.
		if len(attr.isUniqueWith) > 0 {
			fields := []Expression{NewIdExpr(attr.BackendName())}
			for _, name := range attr.isUniqueWith {
				fields = append(fields, NewIdExpr(name))
			}
			constr := NewUniqueFieldsConstraint(fields...)
			constraints = append(constraints, constr)
		}
	}

	if withReferences {
		// Add reference constraints.
		/*
			for name, relation := range info.Relations() {

			}
		*/
	}

	fields := make([]*FieldExpr, 0)
	for _, f := range fieldsMap {
		fields = append(fields, f)
	}

	stmt := NewCreateColStmt(info.BackendName(), true, fields, constraints)
	return stmt
}

func (info *ModelInfo) ModelToFieldExpressions(model interface{}) ([]*FieldValueExpr, apperror.Error) {
	exprs := make([]*FieldValueExpr, 0)

	data, err := info.ModelToMap(model, true, false, false)
	if err != nil {
		return nil, err
	}
	for name, val := range data {
		exprs = append(exprs, NewFieldVal(name, val))
	}

	return exprs, nil
}

func (info *ModelInfo) ModelFilter(model interface{}) Expression {
	id := reflector.Reflect(model).MustStruct().Field(info.PkAttribute().Name())
	if id.IsZero() {
		return nil
	}

	f := NewFieldValFilter(info.BackendName(), info.PkAttribute().BackendName(), OPERATOR_EQ, id.Interface())
	return f
}

func (info *ModelInfo) ModelSelect(model interface{}) *SelectStmt {
	stmt := NewSelectStmt(info.BackendName())
	stmt.FilterAnd(info.ModelFilter(model))
	return stmt
}

func (info *ModelInfo) ModelDeleteStmt(model interface{}) *DeleteStmt {
	stmt := NewDeleteStmt(info.BackendName(), info.ModelSelect(model))
	return stmt
}

// ModelFromMap creates a new model, fills it with data from the map, and returns a pointer to the new model struct.
// Data may contain the struct field names, the backend names or the marshal names as keys.
func (info *ModelInfo) ModelFromMap(data map[string]interface{}) (interface{}, apperror.Error) {
	r := info.NewReflector()

	if err := info.UpdateModelFromData(r, data); err != nil {
		return nil, err
	}

	return r.AddrInterface(), nil
}

func (info *ModelInfo) ValidateModel(m interface{}) apperror.Error {
	r, err := reflector.Reflect(m).Struct()
	if err != nil {
		return apperror.Wrap(err, "invalid_model")
	}

	for fieldName, fieldInfo := range info.Attributes() {
		field := r.Field(fieldName)

		// Fill in default values.
		if defaultVal := fieldInfo.DefaultValue(); defaultVal != nil && field.IsZero() {
			if err := field.SetValue(defaultVal); err != nil {
				msg := fmt.Sprintf("Invalid default value (%v) for %v.%v", defaultVal, info.collection, fieldName)
				return apperror.Wrap(err, "invalid_default_value", msg, true)
			}
		}

		// If field is required, and the field is not a primary key, validate that it is
		// not zero.
		// Note: numeric fields will not be checked, since their zero value is "0", which might
		// be a valid field value.
		if !field.IsNumeric() && fieldInfo.IsRequired() && !fieldInfo.AutoIncrement() {
			if field.IsZero() {
				return &apperror.Err{
					Code:    "empty_required_field",
					Message: fmt.Sprintf("The required field %v is empty", fieldName),
					Public:  true,
				}
			}
		}
		if fieldInfo.Min() > 0 || fieldInfo.Max() > 0 {
			// Either min or max is set, so check length.
			var length float64

			if field.IsIterable() {
				length = float64(field.Len())
			} else if field.IsNumeric() {
				x, _ := field.ConvertTo(float64(0))
				length = x.(float64)
			} else {
				msg := fmt.Sprintf("Field %v.%v has min or max set, but is neither numeric nor a string", info.collection, fieldName)
				return apperror.New("invalid_min_or_max_condition", msg)
			}

			if fieldInfo.Min() > 0 && length < fieldInfo.Min() {
				return &apperror.Err{
					Code:    "shorter_than_min_length",
					Message: fmt.Sprintf("The field %v is shorter than the minimum length %v", fieldName, fieldInfo.Min()),
				}
			}
			if fieldInfo.Max() > 0 && length > fieldInfo.Max() {
				return &apperror.Err{
					Code:    "longer_than_max_length",
					Message: fmt.Sprintf("The field %v is longer than the maximum length %v", fieldName, fieldInfo.Max()),
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

/**
 * ModelInfos.
 */

type ModelInfos map[string]*ModelInfo

func (i ModelInfos) Get(collection string) *ModelInfo {
	return i[collection]
}

func (i ModelInfos) Add(info *ModelInfo) {
	i[info.Collection()] = info
}

func (i ModelInfos) Has(collection string) bool {
	_, ok := i[collection]
	return ok
}

func (i ModelInfos) Find(name string) *ModelInfo {
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

func (m ModelInfos) AnalyzeRelations() apperror.Error {
	for _, info := range m {
		if err := m.analyzeModelRelations(info); err != nil {
			return err
		}
	}
	return nil
}

// Recursive helper for building the relationship information.
// Will properly analyze all embedded structs as well.
// All transientFields will be checked, and split intro attributes or
// relations.
func (m ModelInfos) analyzeModelRelations(model *ModelInfo) apperror.Error {
	for fieldName, field := range model.transientFields {

		relatedItem := reflect.New(field.structType)

		// Try to determine the collection of the related struct.
		relatedCollection, err := GetModelCollection(relatedItem.Interface())
		if err != nil {
			panic(fmt.Sprintf("Could not determine collection name for struct %v", field.structName))
		}

		relatedInfo := m.Get(relatedCollection)
		if relatedInfo == nil {
			// Related struct type was not registered.
			// This is not a relation, but an attribute.
			// We need to build the attribute now and add it to the attributes
			// map.
			//

			attr := buildAttribute(field)
			model.attributes[attr.Name()] = attr
			continue
		}

		// Field is a relation, so build the relation struct.
		relation := buildRelation(field)
		relation.SetModel(model)
		relation.SetRelatedModel(relatedInfo)

		modelName := model.StructName()
		relatedName := relatedInfo.StructName()

		// If an explicit relation type was specified, verify the fields.
		if relation.RelationType() != "" {
			// Relation was set explicitly. Verify fields.

			// Check m2m.
			if relation.RelationType() == RELATION_TYPE_M2M {

				if relation.LocalField() == "" {
					relation.SetLocalField(model.PkAttribute().Name())
				} else if !model.HasAttribute(relation.LocalField()) {
					msg := fmt.Sprintf("Specified inexistant %v.%v as m2m field", modelName, relation.LocalField())
					return apperror.New("invalid_m2m_field", msg)
				}
				if relation.ForeignField() == "" {
					// Set fields to respective PKs.
					relation.SetForeignField(relatedInfo.PkAttribute().Name())
				} else if !relatedInfo.HasAttribute(relation.ForeignField()) {
					msg := fmt.Sprintf("Specified inexistant %v.%v as m2m field", relatedName, relation.ForeignField())
					return apperror.New("invalid_m2m_field", msg)
				}

				// Valid m2m.
				model.relations[fieldName] = relation
				if err := m.buildM2MRelation(relation); err != nil {
					return err
				}
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
			model.relations[fieldName] = relation
			continue
		}

		// No relationship is set, try to determine it.

		if relation.IsMany() {
			// Since relation is many, it must be has-many, since m2m needs
			// to be set explicitly.
			relation.SetRelationType(RELATION_TYPE_HAS_MANY)

			if relation.LocalField() == "" {
				relation.SetLocalField(model.PkAttribute().Name())
			}

			relField := ""
			if relation.ForeignField() == "" {
				// Try to find foreign key field.
				// We check modelNameID, modelNameId.
				relField = modelName + "ID"
				if !relatedInfo.HasAttribute(relField) {
					relField = modelName + "Id"
					if !relatedInfo.HasAttribute(relField) {
						relField = ""
					}
				}

				if relField == "" {
					msg := fmt.Sprintf("Model %v has has-many relationship to %v in field %v, but could not determine the relationship type. Specify explicitly with has-many:LocalField:ForeignField", modelName, relatedName, fieldName)
					return apperror.New("relationship_not_determined", msg)
				}
			} else if !relatedInfo.HasAttribute(relation.ForeignField()) {
				msg := fmt.Sprintf("%v.%v was specified for has-many relationship, but does not exist", relatedInfo.StructName(), relation.ForeignField())
				return apperror.New("invalid_relationship", msg)
			}

			relation.SetForeignField(relField)
			model.relations[fieldName] = relation
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
			model.relations[fieldName] = relation
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
			model.relations[fieldName] = relation
			continue
		}

		// If code reaches this point, relationship type could nto be determined.
		msg := fmt.Sprintf("Model %v has relationship to %v in field %v, but could not determine the relationship type. Specify explicitly with has-one/has-many/belongs-to/m2m", modelName, relatedName, fieldName)
		return apperror.New("relationship_not_determined", msg)
	}

	// All done.
	// Unset transientFields.
	model.transientFields = nil

	return nil
}

func (m ModelInfos) buildM2MRelation(relation *Relation) apperror.Error {
	colName := relation.BackendName()
	if colName == "" || colName == relation.RelatedModel().BackendName() {
		colName = relation.Model().BackendName() + "_" + relation.RelatedModel().BackendName()
	}
	relation.SetBackendName(colName)

	if m.Has(colName) {
		msg := fmt.Sprintf("Could not build m2m relationship: the collection %v already exists", colName)
		return apperror.New("m2m_collection_exists", msg)
	}

	localField := relation.Model().Attribute(relation.LocalField())
	localFieldName := relation.Model().BackendName() + "." + localField.BackendName()
	localAttr := &Attribute{
		Field: Field{
			typ:         localField.Type(),
			name:        localFieldName,
			backendName: localFieldName,
		},
		isRequired: true,
	}

	fk := relation.RelatedModel().Attribute(relation.ForeignField())
	fkName := relation.RelatedModel().BackendName() + "." + fk.BackendName()
	fkAttr := &Attribute{
		Field: Field{
			typ:         fk.Type(),
			name:        fkName,
			backendName: fkName,
		},
		isRequired:   true,
		isUniqueWith: []string{localFieldName},
	}

	col := &ModelInfo{
		collection:  colName,
		backendName: colName,
		attributes: map[string]*Attribute{
			localFieldName: localAttr,
			fkName:         fkAttr,
		},
	}

	m[colName] = col

	return nil
}
