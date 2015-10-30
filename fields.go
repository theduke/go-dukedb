package dukedb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/theduke/go-apperror"
)

/**
 * Fields.
 */

/**
 * Field.
 */

type Field interface {
	// Name returns the struct field name.
	Name() string

	// Type returns the type of the struct field.
	Type() reflect.Type

	// StructType returns the full path of the struct that this model field contains.
	// Empty for non-struct fields.
	// Example: "time.Time".
	StructType() reflect.Type
	SetStructType(typ reflect.Type)

	StructName() string
	SetStructName(typ string)

	// EmbeddedIn returns the qualified path(eg "time.Time") of the embedded
	// struct that this field is contained in.
	// If the field is on the main struct, returns an emtpy string.
	EmbeddingStructName() string
	SetEmbeddingStructName(name string)

	// BackendName returns the name of the field that should be used by the
	// backend.
	BackendName() string
	SetBackendName(name string)

	// MarshalName returns the name that should be used when marshalling this
	// field.
	MarshalName() string
	SetMarshalName(name string)
}

/**
 * fieldTag.
 */

// fieldTag holds all the information that can be containt in struct field tags
// insid the db:"" tag.
type fieldTag struct {
	ignore      bool
	name        string
	typ         string
	marshalName string

	primaryKey    bool
	ignoreIfZero  bool
	autoIncrement bool
	unique        bool
	uniqueWith    []string
	required      bool
	index         bool
	indexName     string
	defaultVal    string
	min           float64
	max           float64

	marshal bool
	embed   bool

	m2m          bool
	m2mName      string
	hasOne       bool
	hasMany      bool
	belongsTo    bool
	localField   string
	foreignField string

	autoPersist bool
	autoCreate  bool
	autoUpdate  bool
	autoDelete  bool
}

/**
 * field.
 */

type field struct {
	// tag contains the parsed tag data.
	tag *fieldTag

	typ                 reflect.Type
	name                string
	structType          reflect.Type
	structName          string
	embeddingStructName string
	backendName         string
	marshalName         string
}

// Ensure field implements Field.
var _ Field = (*field)(nil)

// Parse the information contained in a 'db:"xxx"' field tag.
func (f *field) parseTag(tagContent string) apperror.Error {
	tag := &fieldTag{}
	f.tag = tag

	parts := strings.Split(strings.TrimSpace(tagContent), ";")
	for _, part := range parts {
		if part == "" {
			continue
		}

		part = strings.TrimSpace(part)
		itemParts := strings.Split(part, ":")

		specifier := part
		var value string
		if len(itemParts) > 1 {
			specifier = itemParts[0]
			value = itemParts[1]
		}

		switch specifier {
		case "-":
			tag.ignore = true
			return nil

		case "name":
			if value == "" {
				return apperror.New("invalid_name", "name specifier must be in format name:the_name")
			}

			tag.name = value

		case "type":
			tag.typ = value

		case "marshal-name":
			if value == "" {
				return apperror.New("invalid_name", "name specifier must be in format marshal-name:the_name")
			}
			tag.marshalName = value

		case "primary-key":
			tag.primaryKey = true

		case "ignore-zero":
			tag.ignoreIfZero = true

		case "auto-increment":
			tag.autoIncrement = true

		case "unique":
			tag.unique = true

		case "unique-with":
			parts := strings.Split(value, ",")
			if parts[0] == "" {
				return apperror.New("invalid_unique_with", "unique-with must be a comma-separated list of fields")
			}
			tag.uniqueWith = parts

		case "required":
			tag.required = true

		case "index":
			tag.index = true
			if value == "" {
				return apperror.New("invalid_index_tag", "index must be in format: index:index_name")
			}
			tag.indexName = value

		case "default":
			if value == "" {
				return apperror.New("invalid_default", "default specifier must be in format default:value")
			}
			tag.defaultVal = value

		case "min":
			x, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return apperror.New("invalid_min", "min:xx must be a valid number")
			}
			tag.min = x

		case "max":
			x, err := strconv.ParseFloat(value, 64)
			if err != nil {
				return apperror.New("invalid_max", "max:xx must be a valid number")
			}
			tag.max = x

		case "marshal":
			tag.marshal = true

		case "embed":
			tag.embed = true

		case "m2m":
			tag.m2m = true
			parts := strings.Split(value, ":")
			if len(parts) == 1 {
				tag.m2mName = value
			} else if len(parts) == 3 {
				tag.m2mName = parts[0]
				tag.localField = parts[1]
				tag.foreignField = parts[2]
			} else {
				return apperror.New("invalid_m2m_tag", "Invalid m2m tag, must be either 'm2m' or 'm2m:m2mCollectionName' or 'm2m:colName:localField:foreignField'")
			}

		case "has-one":
			tag.hasOne = true
			if len(itemParts) < 3 {
				return apperror.New("invalid_has_one",
					"Explicit has-one needs to be in format 'has-one:localField:foreignField'")
			}
			tag.localField = itemParts[1]
			tag.foreignField = itemParts[2]

		case "has-many":
			tag.hasMany = true
			if len(itemParts) < 3 {
				return apperror.New("invalid_has_many",
					"Explicit has-many needs to be in format 'has-many:localField:foreignField'")
			}
			tag.localField = itemParts[1]
			tag.foreignField = itemParts[2]

		case "belongs-to":
			tag.belongsTo = true
			if len(itemParts) < 3 {
				return apperror.New("invalid_belongs_to",
					"Explicit belongs-to needs to be in format 'belongs-to:localField:foreignField'")
			}
			tag.localField = itemParts[1]
			tag.foreignField = itemParts[2]

		case "auto-persist":
			tag.autoPersist = true

		case "auto-create":
			tag.autoCreate = true

		case "auto-update":
			tag.autoUpdate = true

		case "auto-delete":
			tag.autoDelete = true

		default:
			return apperror.New("invalid_tag", "Invalid field tag: %v", specifier)
		}
	}

	return nil
}

/**
 * Name.
 */

func (f *field) Name() string {
	return f.name
}

func (f *field) SetName(val string) {
	f.name = val
}

/**
 * Type.
 */

func (f *field) Type() reflect.Type {
	return f.typ
}

func (f *field) SetType(val reflect.Type) {
	f.typ = val
}

/**
 * StructType.
 */

func (f *field) StructType() reflect.Type {
	return f.structType
}

func (f *field) SetStructType(x reflect.Type) {
	f.structType = x
}

/**
 * StructName.
 */

func (f *field) StructName() string {
	return f.structName
}

func (f *field) SetStructName(val string) {
	f.structName = val
}

/**
 * EmbeddingStructName.
 */

func (f *field) EmbeddingStructName() string {
	return f.embeddingStructName
}

func (f *field) SetEmbeddingStructName(val string) {
	f.embeddingStructName = val
}

/**
 * BackendName.
 */

func (f *field) BackendName() string {
	return f.backendName
}

func (f *field) SetBackendName(val string) {
	f.backendName = val
}

/**
 * MarshalName.
 */

func (f *field) MarshalName() string {
	return f.marshalName
}

func (f *field) SetMarshalName(val string) {
	f.marshalName = val
}

/**
 * Attribute.
 */

type Attribute interface {
	Field

	// BackendType returns the backend field type to be used.
	BackendType() string
	SetBackendType(typ string)

	// BackendMarshal returns true if the field should be stored in the backend
	// in a marshalled form (usually JSON).
	BackendMarshal() bool
	SetBackendMarshal(flag bool)

	// BackendEmbed returns true if the field should be embedded in the
	// backend.
	//
	// Only document based databases like MongoDB support embedding.
	BackendEmbed() bool
	SetBackendEmbed(flag bool)

	IsPrimaryKey() bool
	SetIsPrimaryKey(flag bool)

	AutoIncrement() bool
	SetAutoIncrement(flag bool)

	IsUnique() bool
	SetIsUnique(flag bool)

	IsUniqueWith() (fieldNames []string)
	SetIsUniqueWith(fieldNames []string)

	IsRequired() bool
	SetIsRequired(flag bool)

	IgnoreIfZero() bool
	SetIgnoreIfZero(flag bool)

	IsIndex() bool
	SetIsIndex(flag bool)

	IndexName() string
	SetIndexName(name string)

	Min() float64
	SetMin(min float64)

	Max() float64
	SetMax(max float64)

	DefaultValue() interface{}
	SetDefaultValue(val interface{})
}

/**
 * attribute.
 */

type attribute struct {
	// Embed field.
	field

	backendType    string
	backendMarshal bool
	backendEmbed   bool
	isPrimaryKey   bool
	autoIncrement  bool
	isUnique       bool
	isUniqueWith   []string
	isRequired     bool
	ignoreIfZero   bool
	isIndex        bool
	indexName      string
	min            float64
	max            float64
	defaultValue   interface{}
}

// Ensure attribute implements Attribute.
var _ Attribute = (*attribute)(nil)

// buildAttribute builds up an attribute based on a field.
func buildAttribute(field field) Attribute {
	attr := &attribute{}
	attr.field = field

	// Check if any relationship data has been specified on the tag.
	// If so, an error must be returned.
	tag := attr.tag
	if tag.hasOne || tag.belongsTo || tag.hasMany || tag.m2m {
		panic(fmt.Sprintf("Tag for field %v specifies a relationship, but field type cannot possibly be a relationship (struct required).", field.name))
	}

	// Read information from tag into attribute.
	attr.readTag()

	return attr
}

func (a *attribute) readTag() {
	tag := a.tag
	if tag == nil {
		panic("Can't call readTag() when tag is not set.")
	}

	a.isPrimaryKey = tag.primaryKey
	a.ignoreIfZero = tag.ignoreIfZero
	a.autoIncrement = tag.autoIncrement
	a.isUnique = tag.unique
	a.isUniqueWith = tag.uniqueWith
	a.isRequired = tag.required
	a.isIndex = tag.index
	a.indexName = tag.indexName
	a.defaultValue = tag.defaultVal
	a.min = tag.min
	a.max = tag.max

	a.backendMarshal = tag.marshal
	a.backendEmbed = tag.embed
}

/**
 * BackendType.
 */

func (a *attribute) BackendType() string {
	return a.backendType
}

func (a *attribute) SetBackendType(val string) {
	a.backendType = val
}

/**
 * BackendMarshal.
 */

func (a *attribute) BackendMarshal() bool {
	return a.backendMarshal
}

func (a *attribute) SetBackendMarshal(val bool) {
	a.backendMarshal = val
}

/**
 * BackendEmbed.
 */

func (a *attribute) BackendEmbed() bool {
	return a.backendEmbed
}

func (a *attribute) SetBackendEmbed(val bool) {
	a.backendEmbed = val
}

/**
 * IsPrimaryKey.
 */

func (a *attribute) IsPrimaryKey() bool {
	return a.isPrimaryKey
}

func (a *attribute) SetIsPrimaryKey(val bool) {
	a.isPrimaryKey = val
}

/**
 * AutoIncrement.
 */

func (a *attribute) AutoIncrement() bool {
	return a.autoIncrement
}

func (a *attribute) SetAutoIncrement(val bool) {
	a.autoIncrement = val
}

/**
 * IsUnique.
 */

func (a *attribute) IsUnique() bool {
	return a.isUnique
}

func (a *attribute) SetIsUnique(val bool) {
	a.isUnique = val
}

/**
 * IsUniqueWith.
 */

func (a *attribute) IsUniqueWith() []string {
	return a.isUniqueWith
}

func (a *attribute) SetIsUniqueWith(val []string) {
	a.isUniqueWith = val
}

/**
 * IsRequired.
 */

func (a *attribute) IsRequired() bool {
	return a.isRequired
}

func (a *attribute) SetIsRequired(val bool) {
	a.isRequired = val
}

/**
 * IgnoreIfZero.
 */

func (a *attribute) IgnoreIfZero() bool {
	return a.ignoreIfZero
}

func (a *attribute) SetIgnoreIfZero(val bool) {
	a.ignoreIfZero = val
}

/**
 * IsIndex.
 */

func (a *attribute) IsIndex() bool {
	return a.isIndex
}

func (a *attribute) SetIsIndex(x bool) {
	a.isIndex = x
}

/**
 * IndexName.
 */

func (a *attribute) IndexName() string {
	return a.indexName
}

func (a *attribute) SetIndexName(val string) {
	a.indexName = val
}

/**
 * Min.
 */

func (a *attribute) Min() float64 {
	return a.min
}

func (a *attribute) SetMin(val float64) {
	a.min = val
}

/**
 * Max.
 */

func (a *attribute) Max() float64 {
	return a.max
}

func (a *attribute) SetMax(val float64) {
	a.max = val
}

/**
 * DefaultValue.
 */

func (a *attribute) DefaultValue() interface{} {
	return a.defaultValue
}

func (a *attribute) SetDefaultValue(val interface{}) {
	a.defaultValue = val
}

/**
 * Relation.
 */

const (
	RELATION_TYPE_HAS_ONE    = "has_one"
	RELATION_TYPE_HAS_MANY   = "has_many"
	RELATION_TYPE_BELONGS_TO = "belongs_to"
	RELATION_TYPE_M2M        = "m2m"
)

var RELATION_TYPE_MAP map[string]bool = map[string]bool{
	"has_one":    true,
	"has_many":   true,
	"belongs_to": true,
	"m2m":        true,
}

type Relation interface {
	Field

	// ModelInfo returns the model info for the model that contains the relation.
	Model() ModelInfo
	SetModel(m ModelInfo)

	RelatedModel() ModelInfo
	SetRelatedModel(m ModelInfo)

	// Type returns the type of the relation as one of the
	// RELATION_TYPE* constants.
	RelationType() string
	SetRelationType(typ string)

	// RelationIsMany returns true if the relation type is has_many or m2m.
	IsMany() bool

	InversingField() string
	SetInversingField(name string)

	// AutoCreate returns true if the relationship should be automatically
	// created when the parent model is created.
	AutoCreate() bool
	SetAutoCreate(flag bool)

	// AutoUpdate returns true if the relation should be updated
	// automatically when the parent model is updated.
	AutoUpdate() bool
	SetAutoUpdate(flag bool)

	// AutoDelete returns true if the relation should be deleted
	// automatically when the parent model is deleted.
	AutoDelete() bool
	SetAutoDelete(flag bool)

	// LocalField returns the name of the structs key field to use for
	// relations.
	//
	// This usually is the primary key.
	LocalField() string
	SetLocalField(field string)

	// ForeignField returns the field name of the related struct to be used
	// for the relation.
	ForeignField() string
	SetForeignField(field string)
}

/**
 * relation.
 */

type relation struct {
	// Embed field.
	field

	model          ModelInfo
	relatedModel   ModelInfo
	relationType   string
	autoCreate     bool
	autoUpdate     bool
	autoDelete     bool
	localField     string
	foreignField   string
	inversingField string
}

// Ensure relation implements Relation.
var _ Relation = (*relation)(nil)

// buildRelation builds up a relation based on a field.
func buildRelation(field field) Relation {
	relation := &relation{}
	relation.field = field

	// Read information from tag into relation.
	relation.readTag()

	return relation
}

func (r *relation) readTag() {
	tag := r.tag
	if tag == nil {
		panic("Can't call relation.readTag() if tag is not set")
	}

	if tag.m2m {
		r.relationType = RELATION_TYPE_M2M
		if tag.m2mName != "" {
			r.backendName = tag.m2mName
		}
	} else if tag.hasMany {
		r.relationType = RELATION_TYPE_HAS_MANY
	} else if tag.hasOne {
		r.relationType = RELATION_TYPE_HAS_ONE
	} else if tag.belongsTo {
		r.relationType = RELATION_TYPE_BELONGS_TO
	}

	r.localField = tag.localField
	r.foreignField = tag.foreignField

	r.autoCreate = tag.autoCreate
	r.autoUpdate = tag.autoUpdate
	r.autoDelete = tag.autoDelete
	if tag.autoPersist {
		r.autoCreate = true
		r.autoUpdate = true
		r.autoDelete = true
	}
}

/**
 * Model.
 */

func (r *relation) Model() ModelInfo {
	return r.model
}

func (r *relation) SetModel(val ModelInfo) {
	r.model = val
}

/**
 * RelatedModel.
 */

func (r *relation) RelatedModel() ModelInfo {
	return r.relatedModel
}

func (r *relation) SetRelatedModel(val ModelInfo) {
	r.relatedModel = val
}

/**
 * Type.
 */

func (r *relation) RelationType() string {
	return r.relationType
}

func (r *relation) SetRelationType(val string) {
	r.relationType = val
}

func (f *relation) IsMany() bool {
	if f.relationType != "" {
		return f.relationType == RELATION_TYPE_HAS_MANY || f.relationType == RELATION_TYPE_M2M
	} else {
		// Type not determined yet.
		// Assume that a many relationship requires a slice.
		return f.structType.Kind() == reflect.Slice
	}
}

/**
 * InversingField.
 */

func (r *relation) InversingField() string {
	return r.inversingField
}

func (r *relation) SetInversingField(val string) {
	r.inversingField = val
}

/**
 * AutoCreate.
 */

func (r *relation) AutoCreate() bool {
	return r.autoCreate
}

func (r *relation) SetAutoCreate(val bool) {
	r.autoCreate = val
}

/**
 * AutoUpdate.
 */

func (r *relation) AutoUpdate() bool {
	return r.autoUpdate
}

func (r *relation) SetAutoUpdate(val bool) {
	r.autoUpdate = val
}

/**
 * AutoDelete.
 */

func (r *relation) AutoDelete() bool {
	return r.autoDelete
}

func (r *relation) SetAutoDelete(val bool) {
	r.autoDelete = val
}

/**
 * LocalField.
 */

func (r *relation) LocalField() string {
	return r.localField
}

func (r *relation) SetLocalField(val string) {
	r.localField = val
}

/**
 * ForeignField.
 */

func (r *relation) ForeignField() string {
	return r.foreignField
}

func (r *relation) SetForeignField(val string) {
	r.foreignField = val
}
