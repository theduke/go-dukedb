package dukedb

import (
	"reflect"
)

/**
 * FieldInfos.
 */

/**
 * FieldInfo.
 */

type FieldInfo interface {
	// Name returns the struct field name.
	Name() string
	SetName(name string)

	// Type returns the type of the struct field.
	Type() reflect.Type

	// StructType returns the full path of the struct that this model field contains.
	// Empty for non-struct fields.
	// Example: "time.Time".
	StructType() string

	// EmbeddedIn returns the qualified path(eg "time.Time") of the embedded
	// struct that this field is contained in.
	// If the field is on the main struct, returns an emtpy string.
	EmbeddedIn() string

	/**
	 * Field settings.
	 */

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
	IndexName() string
	SetIndexName(name string)

	Min() float64
	SetMin(min float64)

	Max() float64
	SetMax(max float64)

	DefaultValue() interface{}
	SetDefaultValue(val interface{})

	/**
	 * Relationship methods.
	 */

	IsRelation() bool
	Relation() ModelInfo

	// RelationType returns the type of the relation as one of the
	// RELATION_TYPE* constants.
	RelationType() string

	// RelationIsMany returns true if the relation type is has_many or m2m.
	RelationIsMany() bool

	// AutoCreate returns true if the relationship should be automatically
	// created when the parent model is created.
	AutoCreate() bool

	// AutoUpdate returns true if the relation should be updated
	// automatically when the parent model is updated.
	AutoUpdate() bool

	// AutoDelete returns true if the relation should be deleted
	// automatically when the parent model is deleted.
	AutoDelete() bool

	// M2MCollectionName returns the name that should be used for the m2m
	// collection.
	M2MCollectionName() string

	// RelationLocalField returns the name of the structs key field to use
	// for relations.
	// This usually is the primary key.
	RelationLocalField() string

	// RelationForeignKeyField returns the field name of the related struct
	// to be used for the relation.
	RelationForeignKeyField()

	// BackendName returns the name of the field that should be used by the
	// backend.
	BackendName() string
	SetBackendName(name string)

	// BackendType returns the backend field type to be used.
	BackendType() string
	SetBackendType(typ string)

	// BackendMarshal returns true if the field should be stored in the backend
	// in a marshalled form (for example as JSON).
	BackendMarshal() bool
	SetBackendMarshal(flag bool)

	// BackendEmbed returns true if the field should be embedded in the
	// backend.
	//
	// Only document based databases like MongoDB support embedding.
	BackendEmbed() bool
	SetBackendEmbed(flag bool)

	// MarshalName returns the name that should be used when marshalling this
	// field.
	MarshalName() string
	SetMarshalName(name string)
}

/**
 * fieldInfo.
 */

type fieldInfo struct {
	name                 string
	typ                  reflect.Type
	structType           string
	embeddedIn           string
	isPrimaryKey         bool
	autoIncrement        bool
	isUnique             bool
	isRequired           bool
	ignoreIfZero         bool
	indexName            string
	min                  float64
	max                  float64
	defaultValue         interface{}
	relation             ModelInfo
	relationType         string
	autoCreate           bool
	autoUpdate           bool
	autoDelete           bool
	m2mCollectionName    string
	relationLocalField   string
	relationForeignField string
	backendName          string
	backendType          string
	backendMarshal       bool
	backendEmbed         bool
	marshalName          string
}

// Ensure fieldInfo implements FieldInfo.
var _ FieldInfo = (*fieldInfo)(nil)

func NewFieldInfo() FieldInfo {
	return &fieldInfo{}
}

/**
 * Name.
 */

func (f *fieldInfo) Name() string {
	return f.name
}

func (f *fieldInfo) SetName(val string) {
	f.name = val
}

/**
 * Typ.
 */

func (f *fieldInfo) Typ() reflect.Type {
	return f.typ
}

func (f *fieldInfo) SetTyp(val reflect.Type) {
	f.typ = val
}

/**
 * StructType.
 */

func (f *fieldInfo) StructType() string {
	return f.structType
}

func (f *fieldInfo) SetStructType(val string) {
	f.structType = val
}

/**
 * EmbeddedIn.
 */

func (f *fieldInfo) EmbeddedIn() string {
	return f.embeddedIn
}

func (f *fieldInfo) SetEmbeddedIn(val string) {
	f.embeddedIn = val
}

/**
 * IsPrimaryKey.
 */

func (f *fieldInfo) IsPrimaryKey() bool {
	return f.isPrimaryKey
}

func (f *fieldInfo) SetIsPrimaryKey(val bool) {
	f.isPrimaryKey = val
}

/**
 * AutoIncrement.
 */

func (f *fieldInfo) AutoIncrement() bool {
	return f.autoIncrement
}

func (f *fieldInfo) SetAutoIncrement(val bool) {
	f.autoIncrement = val
}

/**
 * IsUnique.
 */

func (f *fieldInfo) IsUnique() bool {
	return f.isUnique
}

func (f *fieldInfo) SetIsUnique(val bool) {
	f.isUnique = val
}

/**
 * IsRequired.
 */

func (f *fieldInfo) IsRequired() bool {
	return f.isRequired
}

func (f *fieldInfo) SetIsRequired(val bool) {
	f.isRequired = val
}

/**
 * IgnoreIfZero.
 */

func (f *fieldInfo) IgnoreIfZero() bool {
	return f.ignoreIfZero
}

func (f *fieldInfo) SetIgnoreIfZero(val bool) {
	f.ignoreIfZero = val
}

/**
 * IndexName.
 */

func (f *fieldInfo) IndexName() string {
	return f.indexName
}

func (f *fieldInfo) SetIndexName(val string) {
	f.indexName = val
}

/**
 * Min.
 */

func (f *fieldInfo) Min() float64 {
	return f.min
}

func (f *fieldInfo) SetMin(val float64) {
	f.min = val
}

/**
 * Max.
 */

func (f *fieldInfo) Max() float64 {
	return f.max
}

func (f *fieldInfo) SetMax(val float64) {
	f.max = val
}

/**
 * DefaultValue.
 */

func (f *fieldInfo) DefaultValue() interface{} {
	return f.defaultValue
}

func (f *fieldInfo) SetDefaultValue(val interface{}) {
	f.defaultValue = val
}

/**
 * Relation.
 */

func (f *fieldInfo) Relation() ModelInfo {
	return f.relation
}

func (f *fieldInfo) SetRelation(val ModelInfo) {
	f.relation = val
}

/**
 * RelationType.
 */

func (f *fieldInfo) RelationType() string {
	return f.relationType
}

func (f *fieldInfo) SetRelationType(val string) {
	f.relationType = val
}

/**
 * AutoCreate.
 */

func (f *fieldInfo) AutoCreate() bool {
	return f.autoCreate
}

func (f *fieldInfo) SetAutoCreate(val bool) {
	f.autoCreate = val
}

/**
 * AutoUpdate.
 */

func (f *fieldInfo) AutoUpdate() bool {
	return f.autoUpdate
}

func (f *fieldInfo) SetAutoUpdate(val bool) {
	f.autoUpdate = val
}

/**
 * AutoDelete.
 */

func (f *fieldInfo) AutoDelete() bool {
	return f.autoDelete
}

func (f *fieldInfo) SetAutoDelete(val bool) {
	f.autoDelete = val
}

/**
 * M2mCollectionName.
 */

func (f *fieldInfo) M2mCollectionName() string {
	return f.m2mCollectionName
}

func (f *fieldInfo) SetM2mCollectionName(val string) {
	f.m2mCollectionName = val
}

/**
 * RelationLocalField.
 */

func (f *fieldInfo) RelationLocalField() string {
	return f.relationLocalField
}

func (f *fieldInfo) SetRelationLocalField(val string) {
	f.relationLocalField = val
}

/**
 * RelationForeignField.
 */

func (f *fieldInfo) RelationForeignField() string {
	return f.relationForeignField
}

func (f *fieldInfo) SetRelationForeignField(val string) {
	f.relationForeignField = val
}

/**
 * BackendName.
 */

func (f *fieldInfo) BackendName() string {
	return f.backendName
}

func (f *fieldInfo) SetBackendName(val string) {
	f.backendName = val
}

/**
 * BackendType.
 */

func (f *fieldInfo) BackendType() string {
	return f.backendType
}

func (f *fieldInfo) SetBackendType(val string) {
	f.backendType = val
}

/**
 * BackendMarshal.
 */

func (f *fieldInfo) BackendMarshal() bool {
	return f.backendMarshal
}

func (f *fieldInfo) SetBackendMarshal(val bool) {
	f.backendMarshal = val
}

/**
 * BackendEmbed.
 */

func (f *fieldInfo) BackendEmbed() bool {
	return f.backendEmbed
}

func (f *fieldInfo) SetBackendEmbed(val bool) {
	f.backendEmbed = val
}

/**
 * MarshalName.
 */

func (f *fieldInfo) MarshalName() string {
	return f.marshalName
}

func (f *fieldInfo) SetMarshalName(val string) {
	f.marshalName = val
}
