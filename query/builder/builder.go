package builder

import (
	"reflect"
	"strings"

	f "github.com/core-go/firestore"
	"github.com/core-go/search"
)

type Builder[F any] struct {
	ModelType reflect.Type
}

func NewBuilder[F any](resultModelType reflect.Type) *Builder[F] {
	return &Builder[F]{ModelType: resultModelType}
}
func UseQuery[T any, F any]() func(F) ([]f.Query, []string) {
	var t T
	resultModelType := reflect.TypeOf(t)
	b := NewBuilder[F](resultModelType)
	return b.BuildQuery
}
func (b *Builder[F]) BuildQuery(filter F) ([]f.Query, []string) {
	return BuildQueryByType(filter, b.ModelType)
}

var operators = map[string]string{
	"=":                  "==",
	"==":                 "==",
	"!=":                 "!=",
	">":                  ">",
	">=":                 ">=",
	"<":                  "<",
	"<=":                 "<=",
	"array-contains":     "array-contains",
	"array-contains-any": "array-contains-any",
	"in":                 "in",
	"not-in":             "not-in",
}

func BuildQueryByType(filter interface{}, resultModelType reflect.Type) ([]f.Query, []string) {
	var query = make([]f.Query, 0)
	fields := make([]string, 0)

	if _, ok := filter.(*search.Filter); ok {
		return query, fields
	}

	value := reflect.Indirect(reflect.ValueOf(filter))
	filterType := value.Type()
	numField := value.NumField()
	for i := 0; i < numField; i++ {
		fsName := getFirestore(filterType, i)
		if fsName == "-" {
			continue
		}
		filterType := value.Type()
		operator := "=="
		if key, ok := filterType.Field(i).Tag.Lookup("operator"); ok && len(key) > 0 {
			oper, ok2 := operators[key]
			if ok2 {
				operator = oper
			}
		}

		field := value.Field(i)
		kind := field.Kind()
		x := field.Interface()
		var psv string
		if kind == reflect.Ptr {
			if field.IsNil() {
				continue
			} else {
				field = field.Elem()
				kind = field.Kind()
				x = field.Interface()
			}
		}
		s0, ok0 := x.(string)
		if ok0 {
			if len(s0) == 0 {
				continue
			}
			psv = s0
		}
		if len(fsName) == 0 {
			fsName = getFirestoreName(resultModelType, filterType.Field(i).Name)
		}
		if v, ok := x.(search.Filter); ok {
			if len(v.Fields) > 0 {
				for _, key := range v.Fields {
					i, _, fsName := getFieldByJson(resultModelType, key)
					if len(fsName) <= 0 {
						fields = fields[len(fields):]
						break
					} else if i == -1 {
						fsName = key
					}
					if len(fsName) > 0 && fsName != "-" {
						fields = append(fields, fsName)
					}
				}
			}
			continue
		} else if len(fsName) == 0 {
			continue
		}
		if len(psv) > 0 {
			query = append(query, f.Query{Path: fsName, Operator: operator, Value: psv})
		} else if rangeTime, ok := x.(search.TimeRange); ok {
			timeQuery := make([]f.Query, 0)
			if rangeTime.Min == nil {
				timeQuery = []f.Query{{Path: fsName, Operator: "<=", Value: rangeTime.Max}}
			} else if rangeTime.Max == nil {
				timeQuery = []f.Query{{Path: fsName, Operator: ">=", Value: rangeTime.Min}}
			} else {
				timeQuery = []f.Query{{Path: fsName, Operator: ">=", Value: rangeTime.Min}, {Path: fsName, Operator: "<=", Value: rangeTime.Max}}
			}
			query = append(query, timeQuery...)
		} else if rangeDate, ok := x.(search.DateRange); ok {
			dateQuery := make([]f.Query, 0)
			if rangeDate.Min == nil && rangeDate.Max == nil {
				continue
			} else if rangeDate.Min == nil {
				dateQuery = []f.Query{{Path: fsName, Operator: "<=", Value: rangeDate.Max}}
			} else if rangeDate.Max == nil {
				dateQuery = []f.Query{{Path: fsName, Operator: ">=", Value: rangeDate.Min}}
			} else {
				dateQuery = []f.Query{{Path: fsName, Operator: ">=", Value: rangeDate.Min}, {Path: fsName, Operator: "<=", Value: rangeDate.Max}}
			}
			query = append(query, dateQuery...)
		} else if numberRange, ok := x.(search.NumberRange); ok {
			numQuery := make([]f.Query, 0)

			if numberRange.Min != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: ">=", Value: *numberRange.Min})
			} else if numberRange.Lower != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: ">", Value: *numberRange.Lower})
			}
			if numberRange.Max != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: "<=", Value: *numberRange.Max})
			} else if numberRange.Upper != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: "<", Value: *numberRange.Upper})
			}

			if len(numQuery) > 0 {
				query = append(query, numQuery...)
			}
		} else if numberRange, ok := x.(search.Int64Range); ok {
			numQuery := make([]f.Query, 0)

			if numberRange.Min != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: ">=", Value: *numberRange.Min})
			} else if numberRange.Lower != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: ">", Value: *numberRange.Lower})
			}
			if numberRange.Max != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: "<=", Value: *numberRange.Max})
			} else if numberRange.Upper != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: "<", Value: *numberRange.Upper})
			}

			if len(numQuery) > 0 {
				query = append(query, numQuery...)
			}
		} else if numberRange, ok := x.(search.IntRange); ok {
			numQuery := make([]f.Query, 0)

			if numberRange.Min != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: ">=", Value: *numberRange.Min})
			} else if numberRange.Lower != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: ">", Value: *numberRange.Lower})
			}
			if numberRange.Max != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: "<=", Value: *numberRange.Max})
			} else if numberRange.Upper != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: "<", Value: *numberRange.Upper})
			}

			if len(numQuery) > 0 {
				query = append(query, numQuery...)
			}
		} else if numberRange, ok := x.(search.Int32Range); ok {
			numQuery := make([]f.Query, 0)

			if numberRange.Min != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: ">=", Value: *numberRange.Min})
			} else if numberRange.Lower != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: ">", Value: *numberRange.Lower})
			}
			if numberRange.Max != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: "<=", Value: *numberRange.Max})
			} else if numberRange.Upper != nil {
				numQuery = append(numQuery, f.Query{Path: fsName, Operator: "<", Value: *numberRange.Upper})
			}

			if len(numQuery) > 0 {
				query = append(query, numQuery...)
			}
		} else if kind == reflect.Slice {
			if reflect.Indirect(reflect.ValueOf(x)).Len() > 0 {
				if operator == "==" {
					operator = "in"
				}
				q := f.Query{Path: fsName, Operator: operator, Value: x}
				query = append(query, q)
			}
		} else {
			q := f.Query{Path: fsName, Operator: operator, Value: x}
			query = append(query, q)
		}
	}
	return query, fields
}

func getFieldByJson(modelType reflect.Type, jsonName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		tag1, ok1 := field.Tag.Lookup("json")
		if ok1 && strings.Split(tag1, ",")[0] == jsonName {
			if tag2, ok2 := field.Tag.Lookup("firestore"); ok2 {
				return i, field.Name, strings.Split(tag2, ",")[0]
			}
			return i, field.Name, ""
		}
	}
	return -1, jsonName, jsonName
}
func getFirestoreName(modelType reflect.Type, fieldName string) string {
	field, _ := modelType.FieldByName(fieldName)
	bsonTag := field.Tag.Get("firestore")
	tags := strings.Split(bsonTag, ",")
	if len(tags) > 0 {
		return tags[0]
	}
	return fieldName
}
func getFirestore(filterType reflect.Type, i int) string {
	field := filterType.Field(i)
	if tag, ok := field.Tag.Lookup("firestore"); ok {
		return strings.Split(tag, ",")[0]
	}
	return ""
}
