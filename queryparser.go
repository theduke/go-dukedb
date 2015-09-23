package dukedb

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/theduke/go-apperror"
)

/**
 * Query parser functions.
 */

func ParseJsonQuery(collection string, js []byte) (Query, apperror.Error) {
	var data map[string]interface{}
	if err := json.Unmarshal(js, &data); err != nil {
		return nil, &apperror.AppError{
			Code:    "invalid_json",
			Message: "Query json could not be unmarshaled. Check for invalid json.",
		}
	}

	return ParseQuery(collection, data)
}

// Build a database query based a map[string]interface{} data structure
// resembling a Mongo query.
//
// It returns a Query equal to the Mongo query, with unsupported features omitted.
// An error is returned if the building of the query fails.
func ParseQuery(collection string, data map[string]interface{}) (Query, apperror.Error) {
	q := Q(collection)

	// First, Handle joins so query and field specification parsing can use
	// join info.
	if rawJoins, ok := data["joins"]; ok {
		rawJoinSlice, ok := rawJoins.([]interface{})
		if !ok {
			return nil, &apperror.AppError{
				Code:    "invalid_joins",
				Message: "Joins must be an array of strings",
			}
		}

		// Convert []interface{} joins to []string.

		joins := make([]string, 0)
		for _, rawJoin := range rawJoinSlice {
			join, ok := rawJoin.(string)
			if !ok {
				return nil, &apperror.AppError{
					Code:    "invalid_joins",
					Message: "Joins must be an array of strings",
				}
			}
			joins = append(joins, join)
		}

		// To handle nested joins, parseQueryJoins has to be called repeatedly
		// until no more joins are returned.
		for depth := 1; true; depth++ {
			var err apperror.Error
			joins, err = parseQueryJoins(q, joins, depth)
			if err != nil {
				return nil, err
			}

			if len(joins) == 0 {
				break
			}
		}
	}

	if rawQuery, ok := data["filters"]; ok {
		query, ok := rawQuery.(map[string]interface{})
		if !ok {
			return nil, &apperror.AppError{
				Code:    "invalid_filters",
				Message: "The filters key must contain a dict",
			}
		}

		if err := parseQueryFilters(q, query); err != nil {
			return nil, err
		}
	}

	// Handle fields.
	if rawFields, ok := data["fields"]; ok {
		fields, ok := rawFields.([]interface{})
		if !ok {
			return nil, &apperror.AppError{
				Code:    "invalid_fields",
				Message: "Fields specification must be an array",
			}
		}

		for _, rawField := range fields {
			field, ok := rawField.(string)
			if !ok {
				return nil, &apperror.AppError{
					Code:    "invalid_fields",
					Message: "Fields specification must be an array of strings",
				}
			}

			parts := strings.Split(field, ".")
			if len(parts) > 1 {
				// Possibly a field on a joined model. Check if a parent join can be found.
				joinQ := q.GetJoin(strings.Join(parts[:len(parts)-1], "."))
				if joinQ != nil {
					// Join query found, add field to the join query.
					joinQ.AddFields(parts[len(parts)-1])
				} else {
					// No join query found, maybe the backend supports nested fields.
					joinQ.AddFields(field)
				}
			} else {
				// Not nested, just add the field.
				q.AddFields(field)
			}
		}
	}

	// Handle limit.
	if rawLimit, ok := data["limit"]; ok {
		if limit, err := NumericToInt64(rawLimit); err != nil {
			return nil, &apperror.AppError{
				Code:    "limit_non_numeric",
				Message: "Limit must be a number",
			}
		} else {
			q.Limit(int(limit))
		}
	}

	// Handle offset.
	if rawOffset, ok := data["offset"]; ok {
		if offset, err := NumericToInt64(rawOffset); err != nil {
			return nil, &apperror.AppError{
				Code:    "offset_non_numeric",
				Message: "Offset must be a number",
			}
		} else {
			q.Offset(int(offset))
		}
	}

	return q, nil
}

func parseQueryJoins(q Query, joins []string, depth int) ([]string, apperror.Error) {
	remaining := make([]string, 0)

	for _, name := range joins {
		parts := strings.Split(name, ".")
		joinDepth := len(parts)
		if joinDepth == depth {
			// The depth of the join equals to the one that should be processed, so do!
			if len(parts) > 1 {
				// Nested join! So try to retrieve the parent join query.
				joinQ := q.GetJoin(strings.Join(parts[:joinDepth-1], "."))
				if joinQ == nil {
					// Parent join not found, obviosly an error.
					return nil, &apperror.AppError{
						Code:    "invalid_nested_join",
						Message: fmt.Sprintf("Tried to join %v, but the parent join was not found", name),
					}
				}
				// Join the current join on the parent join.
				joinQ.Join(parts[len(parts)-1])
			} else {
				// Not nested, just join on the main query.
				q.Join(name)
			}
		} else {
			// Join has other depth than the one that is processed, so append to
			// remaining.
			remaining = append(remaining, name)
		}
	}

	return remaining, nil
}

func parseQueryFilters(q Query, filters map[string]interface{}) apperror.Error {
	filter, err := parseQueryFilter("", filters)
	if err != nil {
		return err
	}

	// If filter is an and, add the and clauses separately to the query.
	// Done for prettier query without top level AND.
	if andFilter, ok := filter.(*AndCondition); ok {
		for _, filter := range andFilter.Filters {
			q.FilterQ(filter)
		}
	} else {
		q.FilterQ(filter)
	}

	return nil
}

// Parses a mongo query filter to a Filter.
// All mongo operators expect $nor are supported.
// Refer to http://docs.mongodb.org/manual/reference/operator/query.
func parseQueryFilter(name string, data interface{}) (Filter, apperror.Error) {
	// Handle
	switch name {
	case "$eq":
		return Eq("placeholder", data), nil
	case "$ne":
		return Neq("placeholder", data), nil
	case "$in":
		return In("placeholder", data), nil
	case "$like":
		return Like("placeholder", data), nil
	case "$gt":
		return Gt("placeholder", data), nil
	case "$gte":
		return Gte("placeholder", data), nil
	case "$lt":
		return Lt("placeholder", data), nil
	case "$lte":
		return Lte("placeholder", data), nil
	case "$nin":
		return Not(In("placeholder", data)), nil
	}

	if name == "$nor" {
		return nil, &apperror.AppError{
			Code:    "unsupported_nor_query",
			Message: "$nor queryies are not supported",
		}
	}

	// Handle OR.
	if name == "$or" {
		orClauses, ok := data.([]interface{})
		if !ok {
			return nil, &apperror.AppError{Code: "invalid_or_data"}
		}

		or := Or()
		for _, rawClause := range orClauses {
			clause, ok := rawClause.(map[string]interface{})
			if !ok {
				return nil, &apperror.AppError{Code: "invalid_or_data"}
			}

			filter, err := parseQueryFilter("", clause)
			if err != nil {
				return nil, err
			}
			or.Add(filter)
		}

		return or, nil
	}

	if nestedData, ok := data.(map[string]interface{}); ok {
		// Nested dict with multipe AND clauses.

		// Build an AND filter.
		and := And()
		for key := range nestedData {
			filter, err := parseQueryFilter(key, nestedData[key])
			if err != nil {
				return nil, err
			}

			if key == "$or" || key == "$and" || key == "$not" {
				// Do nothing
			} else if name == "" {
				filter.SetField(key)
			} else {
				filter.SetField(name)
			}

			and.Add(filter)
		}

		if len(and.Filters) == 1 {
			return and.Filters[0], nil
		} else {
			return and, nil
		}
	}

	// If execution reaches this point, the filter must be a simple equals filter
	// with a value.
	return Eq(name, data), nil
}