package dukedb

import (
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/theduke/go-apperror"
	"github.com/theduke/go-reflector"

	. "github.com/theduke/go-dukedb/expressions"
)

type BaseM2MCollection struct {
	modelInfo *ModelInfo
	Backend   Backend
	Name      string
	Items     []interface{}
}

func (c BaseM2MCollection) Count() int {
	return len(c.Items)
}

func (c BaseM2MCollection) Contains(model interface{}) bool {
	return c.GetByID(c.modelInfo.MustDetermineModelId(model)) != nil
}

func (c BaseM2MCollection) ContainsID(id interface{}) bool {
	return c.GetByID(id) != nil
}

func (c BaseM2MCollection) All() []interface{} {
	return c.Items
}

func (c BaseM2MCollection) GetByID(id interface{}) interface{} {
	for _, item := range c.Items {
		if c.modelInfo.MustDetermineModelId(item) == id {
			return item
		}
	}

	return nil
}

type BaseBackend struct {
	name             string
	debug            bool
	logger           *logrus.Logger
	modelInfo        ModelInfos
	profilingEnabled bool

	HasNativeJoins bool

	// Parent backend reference.
	backend Backend

	hooks map[string][]HookHandler
}

func NewBaseBackend(backend Backend) BaseBackend {
	return BaseBackend{
		backend:   backend,
		modelInfo: make(ModelInfos),
	}
}

func (b BaseBackend) unknownColErr(collection string, model ...interface{}) apperror.Error {
	msg := "Collection " + collection + " was not registered with backend " + b.name
	if len(model) > 0 {
		msg += " (struct: " + reflect.TypeOf(model[0]).String() + ")"
	}

	return &apperror.Err{
		Public:  true,
		Code:    "unknown_model",
		Message: msg,
	}
}

func (b *BaseBackend) Name() string {
	return b.name
}

func (b *BaseBackend) SetName(name string) {
	b.name = name
}

func (b *BaseBackend) Debug() bool {
	return b.debug
}

func (b *BaseBackend) SetDebug(x bool) {
	b.debug = x
}

func (b *BaseBackend) Logger() *logrus.Logger {
	return b.logger
}

func (b *BaseBackend) SetLogger(x *logrus.Logger) {
	b.logger = x
}

func (b *BaseBackend) BuildLogger() {
	l := &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}
	//l = l.WithField("scope", b.name)
	b.logger = l
}

func (b *BaseBackend) ProfilingEnabled() bool {
	return b.profilingEnabled
}

func (b *BaseBackend) EnableProfiling() {
	b.profilingEnabled = true
}

func (b *BaseBackend) DisableProfiling() {
	b.profilingEnabled = false
}

func (b *BaseBackend) Clone() *BaseBackend {
	return &BaseBackend{
		name:           b.name,
		debug:          b.debug,
		logger:         b.logger,
		modelInfo:      b.modelInfo,
		HasNativeJoins: b.HasNativeJoins,
		backend:        b.backend,
		hooks:          b.hooks,
	}
}

/**
 * Hooks.
 */

func (b *BaseBackend) RegisterHook(hook string, handler HookHandler) {
	switch hook {
	case "before_create", "after_create", "before_update", "after_update", "before_delete", "after_delete":
		// No op.
	default:
		panic("Unknown hook type: " + hook)
	}

	if b.hooks == nil {
		b.hooks = make(map[string][]HookHandler)
	}

	if _, ok := b.hooks[hook]; !ok {
		b.hooks[hook] = make([]HookHandler, 0)
	}

	b.hooks[hook] = append(b.hooks[hook], handler)
}

func (b *BaseBackend) GetHooks(hook string) []HookHandler {
	if b.hooks == nil {
		return nil
	}

	return b.hooks[hook]
}

/**
 * Model info.
 */

func (b *BaseBackend) ModelInfos() ModelInfos {
	return b.modelInfo
}

func (b *BaseBackend) ModelInfo(collection string) *ModelInfo {
	return b.modelInfo.Get(collection)
}

func (b *BaseBackend) InfoForModel(model interface{}) (*ModelInfo, apperror.Error) {
	collection, err := GetModelCollection(model)
	if err != nil {
		return nil, err
	}

	info := b.ModelInfo(collection)
	if info == nil {
		return nil, b.unknownColErr(collection, model)
	}

	return info, nil
}

func (b *BaseBackend) HasCollection(collection string) bool {
	return b.modelInfo.Has(collection)
}

func (b *BaseBackend) RegisterModel(model interface{}) *ModelInfo {
	info, err := BuildModelInfo(model)
	if err != nil {
		panic(fmt.Sprintf("Could not register model '%v': %v\n", reflect.TypeOf(model).Name(), err))
	}

	b.modelInfo.Add(info)
	return info
}

func (b *BaseBackend) Build() {
	if err := b.modelInfo.AnalyzeRelations(); err != nil {
		panic(fmt.Sprintf("Analyzing relationships failed: %v", err))
	}
}

/**
 * Creation helpers.
 */

func (b *BaseBackend) NewModel(collection string) (interface{}, apperror.Error) {
	info := b.ModelInfo(collection)
	if info == nil {
		b.unknownColErr(collection).Panic()
	}

	return info.New(), nil
}

func (b *BaseBackend) NewModelSlice(collection string) (interface{}, apperror.Error) {
	info := b.ModelInfo(collection)
	if info == nil {
		b.unknownColErr(collection).Panic()
	}

	return info.NewSlice().Interface(), nil
}

func (b *BaseBackend) ModelToMap(model interface{}, marshal bool, includeRelations bool) (map[string]interface{}, apperror.Error) {
	info, err := b.InfoForModel(model)
	if err != nil {
		return nil, err
	}
	data, err := info.ModelToMap(model, false, marshal, includeRelations)
	if err != nil {
		return nil, err
	}
	return data, nil
}

/**
 *
 */

func (b *BaseBackend) CreateCollection(collections ...string) apperror.Error {
	for _, collection := range collections {
		info := b.ModelInfo(collection)
		if info == nil {
			return b.unknownColErr(collection)
		}

		stmt := info.BuildCreateStmt(false)
		if err := b.backend.Exec(stmt); err != nil {
			return err
		}
	}

	return nil
}

func (b *BaseBackend) RenameCollection(collection, newName string) apperror.Error {
	info := b.ModelInfo(collection)
	if info != nil {
		collection = info.BackendName()
	}

	stmt := NewRenameColStmt(collection, newName)
	err := b.backend.Exec(stmt)

	if info != nil && err == nil {
		info.SetBackendName(newName)
	}

	return err
}

func (b *BaseBackend) DropCollection(collection string, ifExists, cascade bool) apperror.Error {
	info := b.ModelInfo(collection)
	if info != nil {
		collection = info.BackendName()
	}

	stmt := NewDropColStmt(collection, ifExists, cascade)
	return b.backend.Exec(stmt)
}

func (b *BaseBackend) DropAllCollections() apperror.Error {
	for col, _ := range b.ModelInfos() {
		if err := b.DropCollection(col, true, true); err != nil {
			return err
		}
	}
	return nil
}

func (b *BaseBackend) CreateField(collection, fieldName string) apperror.Error {
	info := b.ModelInfo(collection)
	if info == nil {
		return b.unknownColErr(collection)
	}
	attr := info.Attribute(fieldName)
	if attr == nil {
		return apperror.New("unknown_field", fmt.Sprintf("Collection %v does not have a field %v", collection, fieldName))
	}

	fieldExpr := attr.BuildFieldExpression()
	stmt := NewCreateFieldStmt(info.BackendName(), fieldExpr)

	return b.backend.Exec(stmt)
}

func (b *BaseBackend) RenameField(collection, field, newName string) apperror.Error {
	info := b.ModelInfo(collection)
	var attr *Attribute
	if info != nil {
		collection = info.BackendName()
		attr = info.Attribute(field)
		if attr != nil {
			field = attr.BackendName()
		}
	}

	stmt := NewRenameFieldStmt(collection, field, newName)

	err := b.backend.Exec(stmt)
	if err == nil && attr != nil {
		attr.SetBackendName(newName)
	}

	return err
}

func (b *BaseBackend) DropField(collection, field string) apperror.Error {
	info := b.ModelInfo(collection)
	var attr *Attribute
	if info != nil {
		collection = info.BackendName()
		attr = info.Attribute(field)
		if attr != nil {
			field = attr.BackendName()
		}
	}

	stmt := NewDropFieldStmt(collection, field, true, false)

	return b.backend.Exec(stmt)
}

func (b *BaseBackend) CreateIndex(collection, indexName string, fields ...string) apperror.Error {
	info := b.ModelInfo(collection)
	if info != nil {
		collection = info.BackendName()
	}

	fieldExprs := make([]Expression, 0)
	for _, field := range fields {
		if info != nil {
			attr := info.FindAttribute(field)
			if attr != nil {
				field = attr.BackendName()
			}
		}
		fieldExprs = append(fieldExprs, NewIdExpr(field))
	}

	stmt := NewCreateIndexStmt(indexName, NewIdExpr(collection), fieldExprs, false, "")

	return b.backend.Exec(stmt)
}

func (b *BaseBackend) DropIndex(indexName string) apperror.Error {
	stmt := NewDropIndexStmt(indexName, true, false)
	return b.backend.Exec(stmt)
}

/**
 * Query methods.
 */

func (b *BaseBackend) NewQuery(collection string) (*Query, apperror.Error) {
	info := b.ModelInfo(collection)
	if info == nil {
		return nil, b.unknownColErr(collection)
	}
	return newQuery(b.backend, collection), nil
}

func (b *BaseBackend) NewModelQuery(model interface{}) (*Query, apperror.Error) {
	info, err := b.InfoForModel(model)
	if err != nil {
		return nil, err
	}
	filter := info.ModelFilter(model)
	if filter == nil {
		return nil, apperror.New("invalid_model_no_id", "You can't create a query for a model which does not have it's primary key set")
	}

	q := newQuery(b.backend, info.Collection())
	q.SetModels([]interface{}{model})
	q.FilterExpr(filter)
	return q, nil
}

func (b *BaseBackend) Q(arg interface{}, args ...interface{}) *Query {
	if collection, ok := arg.(string); ok {
		q, err := b.backend.NewQuery(collection)
		if err != nil {
			panic(err)
		}

		if len(args) > 0 {
			r := reflector.Reflect(args[0])
			if r.IsZero() {
				panic("Can't create model query with zero id")
			}
			id, err := r.ConvertToType(q.modelInfo.PkAttribute().Type())
			if err != nil {
				panic("Could not convert id: " + err.Error())
			}
			q.Filter(q.modelInfo.PkAttribute().BackendName(), id)
		}

		return q
	}

	q, err := b.backend.NewModelQuery(arg)
	if err != nil {
		panic(err)
	}
	return q
}

func (b *BaseBackend) Query(q *Query, targetSlice ...interface{}) ([]interface{}, apperror.Error) {
	var started time.Time
	if b.profilingEnabled && q.GetName() != "" {
		started = time.Now()
	}

	stmt := q.GetStatement()
	result, err := b.backend.ExecQuery(stmt, false)
	if err != nil {
		return nil, err
	}

	models := make([]interface{}, 0)

	for _, data := range result {
		model, err := q.modelInfo.ModelFromMap(data)
		if err != nil {
			return nil, err
		}
		models = append(models, model)
	}

	if len(targetSlice) > 0 {
		// TODO: assign result to target slice.
		SetSlicePointer(targetSlice[0], models)
	}

	if !started.IsZero() {
		timeTaken := time.Now().Sub(started)
		ms := timeTaken.Nanoseconds() / 1000
		b.Logger().WithFields(logrus.Fields{
			"backend":    b.name,
			"action":     "query",
			"query_name": q.GetName(),
			"ms":         ms,
		}).Debugf("Executed query %v on collection %v in %v ms", q.GetName(), q.GetCollection(), ms)
	}

	return models, nil
}

func (b *BaseBackend) QueryCursor(q *Query) (Cursor, apperror.Error) {
	panic("QueryCursor() not implemented!")
}

func (b *BaseBackend) QueryOne(q *Query, targetModels ...interface{}) (interface{}, apperror.Error) {
	res, err := b.backend.Query(q.Limit(1))
	if err != nil {
		return nil, err
	}

	if len(res) == 0 {
		return nil, nil
	}

	m := res[0]

	if len(targetModels) > 0 {
		SetPointer(targetModels[0], m)
	}

	return m, nil
}

func (b *BaseBackend) Last(q *Query, targetModels ...interface{}) (interface{}, apperror.Error) {
	// Revers all sorts.
	sorts := q.GetStatement().Sorts()
	orderLen := len(sorts)
	if orderLen > 0 {
		for i := 0; i < orderLen; i++ {
			sorts[i].SetAscending(!sorts[i].Ascending())
		}
	} else {
		info := b.backend.ModelInfo(q.GetCollection())
		q = q.Sort(info.PkAttribute().BackendName(), false)
	}

	model, err := b.backend.QueryOne(q)
	if err != nil {
		return nil, err
	}

	if len(targetModels) > 0 {
		SetPointer(targetModels[0], model)
	}

	return model, nil
}

func (b *BaseBackend) FindBy(collection, field string, value interface{}, targetSlice ...interface{}) ([]interface{}, apperror.Error) {
	return b.backend.Q(collection).Filter(field, value).Find(targetSlice...)
}

func (b *BaseBackend) FindOne(collection string, id interface{}, targetModel ...interface{}) (interface{}, apperror.Error) {
	info := b.backend.ModelInfo(collection)
	if info == nil {
		return nil, b.unknownColErr(collection)
	}

	// Try to convert the id to the correct type.
	id, err := reflector.Reflect(id).ConvertTo(info.PkAttribute().Type())
	if err != nil {
		return nil, apperror.Wrap(err, "id_conversion_error")
	}

	return b.backend.Q(collection).Filter(info.PkAttribute().BackendName(), id).First(targetModel...)
}

func (b *BaseBackend) FindOneBy(collection, field string, value interface{}, targetModel ...interface{}) (interface{}, apperror.Error) {
	return b.backend.Q(collection).Filter(field, value).First(targetModel...)
}

func (b *BaseBackend) Count(q *Query) (int, apperror.Error) {
	count := NewFieldSelectorExpr("count", NewFuncExpr("COUNT", NewTextExpr("*")), reflect.TypeOf(1))
	q.SetFieldExpressions([]Expression{count})

	result, err := b.backend.Pluck(q)
	if err != nil {
		return 0, err
	}

	if len(result) < 1 {
		return 0, apperror.New("invalid_empty_result")
	}
	data := result[0]

	countVal, ok := data["count"]
	if !ok {
		return 0, apperror.New("invalid_result_no_count_field")
	}

	r := reflector.Reflect(countVal)
	if !r.IsNumeric() {
		return 0, apperror.New("invalid_result_non_numeric_count")
	}

	x, err2 := r.ConvertTo(int(0))
	if err2 != nil {
		return 0, apperror.Wrap(err2, "count_conversion_error")
	}

	return x.(int), nil
}

func (b *BaseBackend) Pluck(q *Query) ([]map[string]interface{}, apperror.Error) {
	return b.backend.ExecQuery(q.GetStatement(), true)
}

/**
 * Relationship related methods.
 */

func (b *BaseBackend) BuildRelationQuery(q *RelationQuery) (*Query, apperror.Error) {
	baseQ := q.GetBaseQuery()
	baseInfo := b.ModelInfo(baseQ.GetCollection())
	if baseInfo == nil {
		return nil, b.unknownColErr(baseQ.GetCollection())
	}

	baseModels := q.GetModels()

	// If baseModels is empty, check if we need to load them first.
	if len(baseModels) < 1 && !b.HasNativeJoins {
		// No baseModels, and backend does not have native joins, so execute
		// base query first.
		var err apperror.Error
		baseModels, err = baseQ.Find()
		if err != nil {
			return nil, err
		}
		if len(baseModels) == 0 {
			return nil, apperror.New("relation_on_empty_result", "Called .Related() or .Join() on a query without result")
		}
	}

	relationName := q.GetRelationName()
	if relationName == "" {
		return nil, apperror.New("invalid_join_query_no_relation_name", "Invalid join query: no RelationName set")
	}
	relation := baseInfo.Relation(relationName)
	if relation == nil {
		return nil, apperror.New(
			"invalid_join_query_unknown_relation",
			fmt.Sprintf("Join query tried to join on inexistant relation %v.%v", baseInfo.Collection(), relationName))
	}
	relatedInfo := relation.RelatedModel()

	// Build filter arguments.
	filterArgs := make([]interface{}, 0)
	for _, m := range baseModels {
		r, err := reflector.Reflect(m).Struct()
		if err != nil {
			return nil, apperror.Wrap(err, "invalid_base_model")
		}
		val, _ := r.FieldValue(relation.LocalField())

		// TODO: Figure out why this code was added.
		// When might the field value be a slice of arguments??
		// Probably something to do with m2m.
		reflVal := reflect.ValueOf(val)
		if reflVal.Type().Kind() == reflect.Slice {
			for i := 0; i < reflVal.Len(); i++ {
				filterArgs = append(filterArgs, reflVal.Index(i).Interface())
			}
		} else {
			filterArgs = append(filterArgs, val)
		}
	}

	resultQuery := &q.Query

	if relation.RelationType() != RELATION_TYPE_M2M {
		if len(baseModels) > 0 {
			// Basemodels present, so just use the data from them.
			operator := OPERATOR_EQ
			if len(filterArgs) > 1 {
				operator = OPERATOR_IN
			}
			filter := NewFieldValFilter(relatedInfo.BackendName(), relation.ForeignField(), operator, filterArgs)
			resultQuery.FilterExpr(filter)
		} else {
			// No basemodels, so do a native join!

			// Add filter from base query to join, if any.
			baseFilter := baseQ.GetStatement().Filter()
			if baseFilter != nil {
				q.FilterExpr(baseFilter)
			}

			// Join base query collection on this query.
			resultQuery.JoinQ(RelQCustom(resultQuery, baseInfo.BackendName(), relation.ForeignField(), relation.LocalField(), JOIN_INNER))
		}
	} else {
		// M2M query!

		// Relation.BackendName holds the name of the m2m collection.
		localField := relatedInfo.BackendName() + "_" + relation.ForeignField()
		foreignField := baseInfo.BackendName() + "_" + relation.LocalField()
		relQ := RelQCustom(resultQuery, relation.BackendName(), localField, foreignField, JOIN_INNER)

		resultQuery.JoinQ(relQ)

		if len(baseModels) > 0 {
			// Basemodels present, so limit with them.
			operator := OPERATOR_EQ
			if len(filterArgs) > 1 {
				operator = OPERATOR_IN
			}
			filter := NewFieldValFilter(relation.BackendName(), relation.LocalField(), operator, filterArgs)
			resultQuery.FilterExpr(filter)
		}
	}

	if relation.RelationType() == RELATION_TYPE_HAS_ONE {
	}

	return resultQuery, nil
}

/**
 * Convenience functions.
 */

func (b *BaseBackend) persistBelongsToRelation(
	action string,
	beforePersist bool,
	info *ModelInfo,
	r *reflector.StructReflector,
	relation *Relation,
	related *reflector.StructReflector) apperror.Error {

	relatedInfo := relation.RelatedModel()

	// Create or update belongs-to relation after persist.
	if (action == "create" || action == "update") && !beforePersist {
		hasId, err := relatedInfo.ModelHasId(related.AddrInterface())
		if err != nil {
			// Should never happen, just be save.
			return apperror.Wrap(err, "invalid_related_model")
		}

		isNew := !hasId
		if isNew && !relation.AutoCreate() {
			// Auto-create disabled, so ignore.
			return nil
		} else if !isNew && !relation.AutoUpdate() {
			return nil
		}

		// First, set the foreign key.
		if err := related.SetField(relation.ForeignField(), r.Field(relation.LocalField())); err != nil {
			// Should never happen, just be save.
			return apperror.Wrap(err, "foreign_key_update_error")
		}

		if err := b.backend.Save(related.AddrInterface()); err != nil {
			return err
		}
	} else if action == "delete" && beforePersist && relation.AutoDelete() {
		// Main model is being deleted, we are before persist, and auto-delete is on.
		// This means we need to delete the related model.
		if err := b.backend.Delete(related.AddrInterface()); err != nil {
			return err
		}
	}

	return nil
}

//
// action may be either "create", "update" or "delete"
func (b *BaseBackend) PersistRelations(action string, beforePersist bool, info *ModelInfo, model interface{}) apperror.Error {

	r, err := reflector.Reflect(model).Struct()
	if err != nil {
		return apperror.Wrap(err, "invalid_model")
	}

	for _, relation := range info.Relations() {
		relatedInfo := relation.RelatedModel()

		// Handle has-one.
		// Only need to check has-one before persist, since we can do everything there.
		if relation.RelationType() == RELATION_TYPE_HAS_ONE && beforePersist {
			relatedVal := r.Field(relation.Name())
			if relatedVal.IsZero() {
				continue
			}
			related, err := relatedVal.Struct()
			if err != nil {
				continue
			}

			if action == "create" || action == "update" {
				// Action is create or update, so see if we need to save/update the related model.

				hasId, err := relatedInfo.ModelHasId(related.AddrInterface())
				if err != nil {
					// Should never happen, just be save.
					return apperror.Wrap(err, "invalid_related_model")
				}

				isNew := !hasId

				if !isNew {
					// Related model is new.
					if !relation.AutoCreate() {
						// Auto-create disabled, so ignore.
						continue
					}

					// Auto-creating enabled, so save model.
					if err := b.backend.Create(related.AddrInterface()); err != nil {
						return err
					}
				}

				// Update has one field.
				foreignKey := related.Field(relation.ForeignField())
				if err := r.SetField(relation.LocalField(), foreignKey); err != nil {
					// This should never happen. Just be save.
					return apperror.Wrap(err, "foreign_key_update_error")
				}

				if relation.AutoUpdate() {
					// Auto-update enabled, so update the related model.
					if err := b.backend.Update(related.AddrInterface()); err != nil {
						return err
					}
				}
			} else if action == "delete" && relation.AutoDelete() {
				// Auto-delete enabled, so delete the relation.
				if err := b.backend.Delete(related.AddrInterface()); err != nil {
					return err
				}
			}
		}

		if relation.RelationType() == RELATION_TYPE_BELONGS_TO {
			relatedVal := r.Field(relation.Name())
			if relatedVal.IsZero() {
				return nil
			}
			related, err := relatedVal.Struct()
			if err != nil {
				return nil
			}

			if err := b.persistBelongsToRelation(action, beforePersist, info, r, relation, related); err != nil {
				return err
			}
		}

		if relation.RelationType() == RELATION_TYPE_HAS_MANY {
			slice, err := r.Field(relation.Name()).Slice()
			if err != nil {
				// This should never happen, just be save.
				return apperror.Wrap(err, "invalid_slice_field")
			}

			for _, item := range slice.Items() {
				if item.IsZero() {
					continue
				}
				related, err := item.Struct()
				if err != nil {
					// This should never happen, just be save.
					return apperror.Wrap(err, "invalid_has_many_struct")
				}

				if err := b.persistBelongsToRelation(action, beforePersist, info, r, relation, related); err != nil {
					return err
				}
			}
		}

		if relation.RelationType() == RELATION_TYPE_M2M {
			if (action == "create" && relation.AutoCreate()) || (action == "update" && relation.AutoUpdate()) && !beforePersist {
				slice, err := r.Field(relation.Name()).Slice()
				if err != nil {
					// This should never happen, just be save.
					return apperror.Wrap(err, "invalid_slice_field")
				}

				if slice.Len() < 1 {
					// Ignore when empty.
					continue
				}

				newModels := make([]interface{}, 0)

				for _, item := range slice.Items() {
					if item.IsZero() {
						continue
					}

					related, err := item.Struct()
					if err != nil {
						// Should never happen, just be save.
						return apperror.Wrap(err, "invalid_m2m_struct")
					}

					hasId, err2 := relatedInfo.ModelHasId(related.AddrInterface())
					if err2 != nil {
						// Should never happen, just be save.
						return err2
					}

					if !hasId {
						// Related model does is new.
						if !relation.AutoCreate() {
							// No auto-create enabled, so ignore.
							continue
						}

						// Create related model.
						if err := b.backend.Create(related.AddrInterface()); err != nil {
							return err
						}
					}

					newModels = append(newModels, related.AddrInterface())
				}

				m2m, err2 := b.backend.M2M(model, relation.Name())
				if err2 != nil {
					return err2
				}

				if err := m2m.Replace(newModels); err != nil {
					return err
				}
			}

			if action == "delete" && beforePersist {
				m2m, err := b.backend.M2M(model, relation.Name())
				if err != nil {
					return err
				}

				if err := m2m.Clear(); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (b *BaseBackend) Related(model interface{}, name string) (*RelationQuery, apperror.Error) {
	info, err := b.InfoForModel(model)
	if err != nil {
		return nil, err
	}

	if !info.HasRelation(name) {
		return nil, &apperror.Err{
			Code:    "invalid_relation",
			Message: fmt.Sprintf("The collection %v does not have a relation '%v'", info.Collection(), name),
		}
	}

	return b.backend.Q(model).Related(name), nil
}

func (b *BaseBackend) M2M(model interface{}, name string) (M2MCollection, apperror.Error) {
	return nil, nil
}

/**
 * Create, update, delete.
 */

func (b *BaseBackend) Create(model interface{}) apperror.Error {
	info, err := b.backend.InfoForModel(model)
	if err != nil {
		return err
	}

	// Call BeforeCreate hook on model.
	if err := CallModelHook(b.backend, model, "BeforeCreate"); err != nil {
		return err
	}

	// Call backend-wide before_create hooks.
	for _, handler := range b.backend.GetHooks("before_create") {
		handler(b.backend, model)
	}

	// Persist relationships before create.
	if err := b.PersistRelations("create", true, info, model); err != nil {
		return err
	}

	if err := info.ValidateModel(model); err != nil {
		return err
	}

	values, err := info.ModelToFieldExpressions(model)
	if err != nil {
		return err
	}

	// Build a CreateStatement.
	stmt := NewCreateStmt(info.BackendName(), values)

	if err := b.backend.Exec(stmt); err != nil {
		return err
	}

	// Persist relationships again since m2m can only be handled  when an ID is set.
	if err := b.PersistRelations("create", false, info, model); err != nil {
		return err
	}

	CallModelHook(b.backend, model, "AfterCreate")

	// Call backend-wide after_create hooks.
	for _, handler := range b.GetHooks("after_create") {
		handler(b.backend, model)
	}

	return nil
}

func (b *BaseBackend) Update(model interface{}) apperror.Error {
	info, err := b.InfoForModel(model)
	if err != nil {
		return err
	}

	// Verify that ID is not zero.
	id, err := info.DetermineModelId(model)
	if err != nil {
		return err
	}
	if reflector.Reflect(id).IsZero() {
		return apperror.New("cant_update_model_without_id",
			fmt.Sprintf("Trying to update model %v with zero id", info.Collection()))
	}

	if err := CallModelHook(b.backend, model, "BeforeUpdate"); err != nil {
		return err
	}
	if err := info.ValidateModel(model); err != nil {
		return err
	}

	// Call backend-wide before_update hooks.
	for _, handler := range b.GetHooks("before_update") {
		handler(b.backend, model)
	}

	if err := b.PersistRelations("update", true, info, model); err != nil {
		return err
	}

	values, err := info.ModelToFieldExpressions(model)
	if err != nil {
		return err
	}

	// Build a update statement.
	stmt := NewUpdateStmt(info.BackendName(), values, info.ModelSelect(model))

	if err := b.backend.Exec(stmt); err != nil {
		return err
	}

	// Persist relationships again since m2m can only be handled  when an ID is set.
	if err := b.PersistRelations("update", false, info, model); err != nil {
		return err
	}

	CallModelHook(b.backend, model, "AfterUpdate")

	// Call backend-wide after_update hooks.
	for _, handler := range b.GetHooks("after_update") {
		handler(b.backend, model)
	}

	return nil
}

func (b *BaseBackend) Save(model interface{}) apperror.Error {
	info, err := b.InfoForModel(model)
	if err != nil {
		return err
	}

	hasId, err := info.ModelHasId(model)
	if err != nil {
		return err
	}

	if !hasId {
		return b.backend.Create(model)
	} else {
		return b.backend.Update(model)
	}
}

func (b *BaseBackend) UpdateByMap(query *Query, data map[string]interface{}) apperror.Error {
	values := make([]*FieldValueExpr, 0)
	for key, val := range data {
		values = append(values, NewFieldVal(key, val))
	}
	stmt := NewUpdateStmt(query.modelInfo.BackendName(), values, query.GetStatement())

	return b.backend.Exec(stmt)
}

func (b *BaseBackend) Delete(model interface{}) apperror.Error {
	info, err := b.InfoForModel(model)
	if err != nil {
		return err
	}

	// Verify that ID is not zero.
	hasId, err := info.ModelHasId(model)
	if err != nil {
		return err
	} else if !hasId {
		return apperror.New("model_without_id", "Can't delete a model without an id.")
	}

	if err := CallModelHook(b.backend, model, "BeforeDelete"); err != nil {
		return err
	}

	// Call backend-wide before_delete hooks.
	for _, handler := range b.GetHooks("before_delete") {
		handler(b.backend, model)
	}

	if err := b.PersistRelations("delete", true, info, model); err != nil {
		return err
	}

	stmt := info.ModelDeleteStmt(model)
	if err := b.backend.Exec(stmt); err != nil {
		return err
	}

	if err := b.PersistRelations("delete", false, info, model); err != nil {
		return err
	}

	CallModelHook(b.backend, model, "AfterDelete")

	// Call backend-wide after_delete hooks.
	for _, handler := range b.GetHooks("after_delete") {
		handler(b.backend, model)
	}

	return nil
}

func (b *BaseBackend) DeleteQ(query *Query) apperror.Error {
	stmt := NewDeleteStmt(query.modelInfo.BackendName(), query.GetStatement())
	return b.backend.Exec(stmt)
}

/**
 * Join logic.
 */

/*
func BackendDoJoins(b Backend, model string, objs []interface{}, joins []*RelationQuery) apperror.Error {
	if len(objs) == 0 {
		// Nothing to do if no objects.
		return nil
	}

	handledJoins := make(map[string]bool)
	nestedJoins := make([]*RelationQuery, 0)

	modelInfo := b.ModelInfo(joins[0].GetBaseQuery().GetCollection())
	if modelInfo == nil {
		panic("Missing model info!")
	}

	for _, joinQ := range joins {
		// With a specific join type, joins should be handled by the backend itself.
		if joinQ.GetJoinType() != "" {
			continue
		}

		parts := strings.Split(joinQ.GetRelationName(), ".")
		if len(parts) > 1 {
			// Add the nested join to nestedJoins and execute it later, after this loop.
			nestedJoins = append(nestedJoins, joinQ)

			// Build a new join query for the first level join.
			RelQ(joinQ.GetBaseQuery(), parts[0], JOIN_LEFT)
		}

		// Skip already executed joins to avoid duplicate work.
		if _, ok := handledJoins[joinQ.GetRelationName()]; ok {
			continue
		}

		err := doJoin(b, model, objs, joinQ)
		if err != nil {
			return err
		}
	}

	// If no nestedJoins remain, we got nothing to do, so return.
	if len(nestedJoins) < 1 {
		return nil
	}

	// Nested joins remain!
	// First, group the joins by parent.
	joinMap := make(map[string][]*RelationQuery, 0)

	for _, joinQ := range nestedJoins {
		parts := strings.Split(joinQ.GetRelationName(), ".")
		joinMap[parts[0]] = append(joinMap[parts[0]], joinQ)
	}

	// Execute the joins for each Field.
	for parentField := range joinMap {
		fieldInfo := modelInfo.GetField(parentField)
		parentCollection := fieldInfo.RelationCollection

		// First, build a new slice of the objects to join.
		var nestedObjs []interface{}

		for _, obj := range objs {
			nestedObj, err := GetStructFieldValue(obj, fieldInfo.Name)
			if err != nil {
				panic(err)
			}

			// Ignore zero values.
			if nestedObj == nil || IsZero(nestedObj) {
				continue
			}

			if fieldInfo.RelationIsMany {
				// Many relationship means the nestedObj is actually an array, so
				// we need to add all items of that array to nestedObjs.

				nestedSlice := reflect.ValueOf(nestedObj)
				for i := 0; i < nestedSlice.Len(); i++ {
					nestedObjs = append(nestedObjs, nestedSlice.Index(i).Interface())
				}
			} else {
				nestedObjs = append(nestedObjs, nestedObj)
			}
		}

		// If no objs were found, avoid unneccessary work and skip this Field.
		if len(nestedObjs) == 0 {
			continue
		}

		// Need to determine the collection.

		// Build a list of nested joins.
		var nestedJoins []*RelationQuery
		for _, joinQ := range joinMap[parentField] {
			parts := strings.Split(joinQ.GetRelationName(), ".")
			nestedQ := RelQ(Q(parentCollection), strings.Join(parts[1:], "."), JOIN_LEFT)
			nestedJoins = append(nestedJoins, nestedQ)
		}

		// Now, Actually execute the nested joins.
		BackendDoJoins(b, parentCollection, nestedObjs, nestedJoins)
	}

	return nil
}

func doJoin(b Backend, model string, objs []interface{}, joinQ *RelationQuery) apperror.Error {
	resultQuery, err := BuildRelationQuery(b, objs, joinQ)
	if err != nil {
		if apperror.IsCode(err, "relation_query_on_empty_result") {
			// If the base result was empty, we just ignore this join.
			return nil
		}

		return err
	}

	res, err := resultQuery.Find()
	if err != nil {
		return err
	}

	if len(res) > 0 {
		if assigner := resultQuery.GetJoinResultAssigner(); assigner != nil {
			assigner(objs, res, joinQ)
		} else {
			assignJoinModels(objs, res, joinQ)
		}
	}

	return nil
}

func assignJoinModels(objs, joinedModels []interface{}, joinQ *RelationQuery) {
	targetField := joinQ.GetRelationName()
	joinedField := "" // joinQ.GetJoinFieldName()
	joinField := ""   //joinQ.GetForeignFieldName()

	mapper := make(map[interface{}][]interface{})
	for _, model := range joinedModels {
		val, _ := GetStructFieldValue(model, joinField)
		mapper[val] = append(mapper[val], model)
	}

	for _, model := range objs {
		val, err := GetStructFieldValue(model, joinedField)
		if err != nil {
			panic("Join result assignment error: " + err.Error())
		}

		if joins, ok := mapper[val]; ok && len(joins) > 0 {
			SetStructModelField(model, targetField, joins)
		}
	}
}
*/
