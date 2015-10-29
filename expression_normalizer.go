package dukedb

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/theduke/go-apperror"
)

/**
 * Expression normalizer.
 */

type expressionNormalizer struct {
	info       ModelInfos
	expression Expression
}

func NewExpressionNormalizer(modelInfo ModelInfos) *expressionNormalizer {
	return &expressionNormalizer{
		info: modelInfo,
	}
}

func (n *expressionNormalizer) Normalize(expression Expression) apperror.Error {
	if expression == nil {
		return nil
	}

	switch expr := expression.(type) {
	case UpdateStatement:
		if err := n.Normalize(expr.Select()); err != nil {
			return err
		}

		// Same as for CreateStatement.
		backendName := n.info.FindBackendName(expr.Collection())
		if backendName == "" {
			return apperror.New("unknown_collection", fmt.Sprintf("The collection %v does not exist", expr.Collection()))
		}
		expr.SetCollection(backendName)

		for _, fieldVal := range expr.Values() {
			if err := n.Normalize(fieldVal); err != nil {
				return err
			}
		}

	case CreateStatement:
		backendName := n.info.FindBackendName(expr.Collection())
		if backendName == "" {
			return apperror.New("unknown_collection", fmt.Sprintf("The collection %v does not exist", expr.Collection()))
		}
		expr.SetCollection(backendName)

		for _, fieldVal := range expr.Values() {
			if err := n.Normalize(fieldVal); err != nil {
				return err
			}
		}

	case SelectStatement:
		if err := n.NormalizeSelect(expr); err != nil {
			return err
		}

	case JoinStatement:
		if err := n.Normalize(expr.JoinCondition()); err != nil {
			return err
		}
		if err := n.Normalize(expr.SelectStatement()); err != nil {
			return err
		}

	case MultiExpression:
		for _, expr := range expr.Expressions() {
			if err := n.Normalize(expr); err != nil {
				return err
			}
		}

	case NestedExpression:
		if err := n.Normalize(expr.Expression()); err != nil {
			return err
		}

	case FilterExpression:
		if err := n.Normalize(expr.Field()); err != nil {
			return err
		}
		if err := n.Normalize(expr.Clause()); err != nil {
			return err
		}

	case SortExpression:
		// Ignore.
		// Should  be handled by NestedExpression clause above.

	case FieldValueExpression:
		if err := n.Normalize(expr.Field()); err != nil {
			return err
		}
		if err := n.Normalize(expr.Value()); err != nil {
			return err
		}

	case CollectionFieldIdentifierExpression:
		if expr.Collection() == "" {
			return nil
		}

		info := n.info.Find(expr.Collection())
		if info == nil {
			return apperror.New("unknown_collection", fmt.Sprintf("The collection '%v' does not exist", expr.Collection()), true)
		}
		expr.SetCollection(info.Collection)

		// We found a valid collection.
		// Now check the field name.
		fieldName := info.FindBackendName(expr.Field())
		if fieldName == "" {
			return apperror.New("unknown_field", fmt.Sprintf("The collection '%v' has no field '%v'", info.Collection, expr.Field))
		}
		expr.SetField(fieldName)

	case IdentifierExpression:
		// Ignore.

	default:
		panic(fmt.Sprintf("Unhandled expression type: %v\n", reflect.TypeOf(expr)))
	}

	return nil
}

func (n *expressionNormalizer) NormalizeSelect(stmt SelectStatement) apperror.Error {
	// Fix nesting (joins and fields).
	if err := n.fixNestedJoins(stmt); err != nil {
		return err
	}
	n.fixNestedFields(stmt)

	// Normalize fields.
	for _, field := range stmt.Fields() {
		if err := n.Normalize(field); err != nil {
			return err
		}
	}
	// Normalize filter.
	if err := n.Normalize(stmt.Filter()); err != nil {
		return err
	}
	// Normalize sorts.
	for _, sort := range stmt.Sorts() {
		if err := n.Normalize(sort); err != nil {
			return err
		}
	}
	// Normalize joins.
	for _, join := range stmt.Joins() {
		if err := n.Normalize(join); err != nil {
			return err
		}
	}

	return nil
}

func (n *expressionNormalizer) fixNestedJoins(s SelectStatement) apperror.Error {
	if len(s.Joins()) < 1 {
		return nil
	}
	return n.fixNestedJoinsRecursive(s, 2, 1)
}

func (n *expressionNormalizer) fixNestedJoinsRecursive(s SelectStatement, lvl, maxLvl int) apperror.Error {
	// TODO: Optimize this.

	remainingJoins := make([]JoinStatement, 0)

	for _, join := range s.Joins() {
		if join.RelationName() == "" {
			// No RelationName set, so ignore this custom join.
			remainingJoins = append(remainingJoins, join)
			continue
		}

		parts := strings.Split(join.RelationName(), ".")
		joinLvl := len(parts)
		if joinLvl > maxLvl {
			maxLvl = joinLvl
		}

		if joinLvl != lvl {
			// Join is not on the level currently processed, so skip.
			remainingJoins = append(remainingJoins, join)
			continue
		}

		parentJoin := s.GetJoin(strings.Join(parts[0:joinLvl-2], "."))
		if parentJoin == nil {
			return &apperror.Err{
				Public:  true,
				Code:    "invalid_join",
				Message: fmt.Sprintf("Invalid nested join '%v': parent join '%v' not found", join.RelationName, parts[0]),
			}
		}
		join.SetRelationName(parts[joinLvl-1])
		parentJoin.AddJoin(join)
	}

	s.SetJoins(remainingJoins)

	if lvl < maxLvl {
		return n.fixNestedJoinsRecursive(s, lvl+1, maxLvl)
	}
	return nil
}

func (n *expressionNormalizer) fixNestedFields(s SelectStatement) {
	if len(s.Fields()) < 1 {
		return
	}

	remainingFields := make([]Expression, 0)

	for _, fieldExpr := range s.Fields() {
		fieldName := ""

		if field, ok := fieldExpr.(IdentifierExpression); ok {
			fieldName = field.Identifier()
		} else if field, ok := fieldExpr.(CollectionFieldIdentifierExpression); ok {
			if field.Collection() != s.Collection() {
				fieldName = field.Collection() + "." + field.Field()
			}
		}

		if fieldName == "" {
			remainingFields = append(remainingFields, fieldExpr)
			continue
		}

		parts := strings.Split(fieldName, ".")
		if len(parts) < 2 {
			remainingFields = append(remainingFields, fieldExpr)
			continue
		}

		// Nested field, so try to find parent join.
		joinName := strings.Join(parts[0:len(parts)-2], ".")
		join := s.GetJoin(joinName)
		if join == nil {
			// Maybe the backend supports nested fields, or this is a statement with
			// direct database joins, so leave the field untouched.
			remainingFields = append(remainingFields, fieldExpr)
		} else {
			// Found parent join, so add the field to it.
			join.AddField(ColFieldIdExpr(join.Collection(), parts[len(parts)-1]))
		}
	}

	s.SetFields(remainingFields)
}
