package dukedb

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/theduke/go-apperror"

	. "github.com/theduke/go-dukedb/expressions"
)

/**
 * Fields.
 */

/**
 * Field.
 */

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

type Field struct {
	// tag contains the parsed tag data.
	tag *fieldTag

	typ                 reflect.Type
	name                string
	structType          reflect.Type
	structName          string
	embeddingStructName string
	backendName         string
	marshalName         string

	isRequired bool
}

// Parse the information contained in a 'db:"xxx"' field tag.
func (f *Field) parseTag(tagContent string) apperror.Error {
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

func (f *Field) Name() string {
	return f.name
}

func (f *Field) SetName(val string) {
	f.name = val
}

/**
 * Type.
 */

func (f *Field) Type() reflect.Type {
	return f.typ
}

func (f *Field) SetType(val reflect.Type) {
	f.typ = val
}

/**
 * StructType.
 */

func (f *Field) StructType() reflect.Type {
	return f.structType
}

func (f *Field) SetStructType(x reflect.Type) {
	f.structType = x
}

/**
 * StructName.
 */

func (f *Field) StructName() string {
	return f.structName
}

func (f *Field) SetStructName(val string) {
	f.structName = val
}

/**
 * EmbeddingStructName.
 */

func (f *Field) EmbeddingStructName() string {
	return f.embeddingStructName
}

func (f *Field) SetEmbeddingStructName(val string) {
	f.embeddingStructName = val
}

/**
 * BackendName.
 */

func (f *Field) BackendName() string {
	return f.backendName
}

func (f *Field) SetBackendName(val string) {
	f.backendName = val
}

/**
 * MarshalName.
 */

func (f *Field) MarshalName() string {
	return f.marshalName
}

func (f *Field) SetMarshalName(val string) {
	f.marshalName = val
}

/**
 * IsRequired.
 */

func (f *Field) IsRequired() bool {
	return f.isRequired
}

func (f *Field) SetIsRequired(val bool) {
	f.isRequired = val
}

/**
 * Attribute.
 */

type Attribute struct {
	// Embed field.
	Field

	backendType    string
	backendMarshal bool
	backendEmbed   bool
	isPrimaryKey   bool
	autoIncrement  bool
	isUnique       bool
	isUniqueWith   []string
	ignoreIfZero   bool
	isIndex        bool
	indexName      string
	min            float64
	max            float64
	defaultValue   interface{}
}

// buildAttribute builds up an attribute based on a field.
func BuildAttribute(field *Field) *Attribute {
	attr := &Attribute{}
	attr.Field = *field

	// Check if any relationship data has been specified on the tag.
	// If so, an error must be returned.
	tag := attr.tag
	if tag.hasOne || tag.belongsTo || tag.hasMany || tag.m2m {
		panic(fmt.Sprintf("Tag for field %v specifies a relationship, but could not determine the related model.\n"+
			"Did you forget to backend.RegisterModel() your related model?", field.name))
	}

	// Read information from tag into attribute.
	attr.readTag()

	return attr
}

func (a *Attribute) readTag() {
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
	if tag.defaultVal != "" {
		a.defaultValue = tag.defaultVal
	}
	a.min = tag.min
	a.max = tag.max

	a.backendMarshal = tag.marshal
	a.backendEmbed = tag.embed

	if a.backendMarshal || a.backendEmbed {
		a.ignoreIfZero = true
	}
}

/**
 * BackendType.
 */

func (a *Attribute) BackendType() string {
	return a.backendType
}

func (a *Attribute) SetBackendType(val string) {
	a.backendType = val
}

/**
 * BackendMarshal.
 */

func (a *Attribute) BackendMarshal() bool {
	return a.backendMarshal
}

func (a *Attribute) SetBackendMarshal(val bool) {
	a.backendMarshal = val
}

/**
 * BackendEmbed.
 */

func (a *Attribute) BackendEmbed() bool {
	return a.backendEmbed
}

func (a *Attribute) SetBackendEmbed(val bool) {
	a.backendEmbed = val
}

/**
 * IsPrimaryKey.
 */

func (a *Attribute) IsPrimaryKey() bool {
	return a.isPrimaryKey
}

func (a *Attribute) SetIsPrimaryKey(val bool) {
	a.isPrimaryKey = val
}

/**
 * AutoIncrement.
 */

func (a *Attribute) AutoIncrement() bool {
	return a.autoIncrement
}

func (a *Attribute) SetAutoIncrement(val bool) {
	a.autoIncrement = val
}

/**
 * IsUnique.
 */

func (a *Attribute) IsUnique() bool {
	return a.isUnique
}

func (a *Attribute) SetIsUnique(val bool) {
	a.isUnique = val
}

/**
 * IsUniqueWith.
 */

func (a *Attribute) IsUniqueWith() []string {
	return a.isUniqueWith
}

func (a *Attribute) SetIsUniqueWith(val []string) {
	a.isUniqueWith = val
}

/**
 * IgnoreIfZero.
 */

func (a *Attribute) IgnoreIfZero() bool {
	return a.ignoreIfZero
}

func (a *Attribute) SetIgnoreIfZero(val bool) {
	a.ignoreIfZero = val
}

/**
 * IsIndex.
 */

func (a *Attribute) IsIndex() bool {
	return a.isIndex
}

func (a *Attribute) SetIsIndex(x bool) {
	a.isIndex = x
}

/**
 * IndexName.
 */

func (a *Attribute) IndexName() string {
	return a.indexName
}

func (a *Attribute) SetIndexName(val string) {
	a.indexName = val
}

/**
 * Min.
 */

func (a *Attribute) Min() float64 {
	return a.min
}

func (a *Attribute) SetMin(val float64) {
	a.min = val
}

/**
 * Max.
 */

func (a *Attribute) Max() float64 {
	return a.max
}

func (a *Attribute) SetMax(val float64) {
	a.max = val
}

/**
 * DefaultValue.
 */

func (a *Attribute) DefaultValue() interface{} {
	return a.defaultValue
}

func (a *Attribute) SetDefaultValue(val interface{}) {
	a.defaultValue = val
}

func (a *Attribute) BuildFieldExpression() *FieldExpr {
	constraints := make([]Expression, 0)

	if a.isPrimaryKey {
		constraints = append(constraints, NewConstraintExpr(CONSTRAINT_PRIMARY_KEY))
	}
	if a.autoIncrement {
		constraints = append(constraints, NewConstraintExpr(CONSTRAINT_AUTO_INCREMENT))
	}
	if a.isUnique && len(a.isUniqueWith) == 0 {
		constraints = append(constraints, NewConstraintExpr(CONSTRAINT_UNIQUE))
	}
	if a.isRequired {
		constraints = append(constraints, NewConstraintExpr(CONSTRAINT_NOT_NULL))
	}

	typ := NewFieldTypeExpr(a.backendType, a.typ)
	e := NewFieldExpr(a.BackendName(), typ, constraints...)

	return e
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

type Relation struct {
	// Embed field.
	Field

	model          *ModelInfo
	relatedModel   *ModelInfo
	relationType   string
	autoCreate     bool
	autoUpdate     bool
	autoDelete     bool
	localField     string
	foreignField   string
	inversingField string
}

// buildRelation builds up a relation based on a field.
func BuildRelation(field *Field) *Relation {
	relation := &Relation{}
	relation.Field = *field

	// Read information from tag into relation.
	relation.readTag()

	return relation
}

func (r *Relation) readTag() {
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

func (r *Relation) Model() *ModelInfo {
	return r.model
}

func (r *Relation) SetModel(val *ModelInfo) {
	r.model = val
}

/**
 * RelatedModel.
 */

func (r *Relation) RelatedModel() *ModelInfo {
	return r.relatedModel
}

func (r *Relation) SetRelatedModel(val *ModelInfo) {
	r.relatedModel = val
}

/**
 * Type.
 */

func (r *Relation) RelationType() string {
	return r.relationType
}

func (r *Relation) SetRelationType(val string) {
	r.relationType = val
}

func (f *Relation) IsMany() bool {
	if f.relationType != "" {
		return f.relationType == RELATION_TYPE_HAS_MANY || f.relationType == RELATION_TYPE_M2M
	} else {
		// Type not determined yet.
		// Assume that a many relationship requires a slice.
		return f.Type().Kind() == reflect.Slice
	}
}

/**
 * InversingField.
 */

func (r *Relation) InversingField() string {
	return r.inversingField
}

func (r *Relation) SetInversingField(val string) {
	r.inversingField = val
}

/**
 * AutoCreate.
 */

func (r *Relation) AutoCreate() bool {
	return r.autoCreate
}

func (r *Relation) SetAutoCreate(val bool) {
	r.autoCreate = val
}

/**
 * AutoUpdate.
 */

func (r *Relation) AutoUpdate() bool {
	return r.autoUpdate
}

func (r *Relation) SetAutoUpdate(val bool) {
	r.autoUpdate = val
}

/**
 * AutoDelete.
 */

func (r *Relation) AutoDelete() bool {
	return r.autoDelete
}

func (r *Relation) SetAutoDelete(val bool) {
	r.autoDelete = val
}

/**
 * LocalField.
 */

func (r *Relation) LocalField() string {
	return r.localField
}

func (r *Relation) SetLocalField(val string) {
	r.localField = val
}

/**
 * ForeignField.
 */

func (r *Relation) ForeignField() string {
	return r.foreignField
}

func (r *Relation) SetForeignField(val string) {
	r.foreignField = val
}
