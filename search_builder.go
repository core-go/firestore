package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"log"
	"reflect"
	"strings"
)

type SearchBuilder struct {
	Collection       *firestore.CollectionRef
	ModelType        reflect.Type
	BuildQuery       func(searchModel interface{}) ([]Query, []string)
	BuildSort        func(s string, modelType reflect.Type) map[string]firestore.Direction
	GetSort          func(m interface{}) string
	idIndex          int
	createdTimeIndex int
	updatedTimeIndex int
}

func NewSearchBuilderWithQuery(client *firestore.Client, collectionName string, modelType reflect.Type, buildQuery func(interface{}) ([]Query, []string), getSort func(interface{}) string, buildSort func(s string, modelType reflect.Type) map[string]firestore.Direction, createdTimeFieldName string, updatedTimeFieldName string, options ...string) *SearchBuilder {
	idx := -1
	var idFieldName string
	if len(options) > 0 && len(options[0]) > 0 {
		idFieldName = options[0]
	}
	if len(idFieldName) == 0 {
		idx, _, _ = FindIdField(modelType)
		if idx < 0 {
			log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
		}
	} else {
		idx, _, _ = FindFieldByName(modelType, idFieldName)
		if idx < 0 {
			log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name())
		}
	}
	ctIdx := -1
	if len(createdTimeFieldName) >= 0 {
		ctIdx, _, _ = FindFieldByName(modelType, createdTimeFieldName)
	}
	utIdx := -1
	if len(updatedTimeFieldName) >= 0 {
		utIdx, _, _ = FindFieldByName(modelType, updatedTimeFieldName)
	}
	collection := client.Collection(collectionName)
	return &SearchBuilder{Collection: collection, ModelType: modelType, BuildQuery: buildQuery, BuildSort: buildSort, GetSort: getSort, idIndex: idx, createdTimeIndex: ctIdx, updatedTimeIndex: utIdx}
}
func NewSearchBuilder(client *firestore.Client, collectionName string, modelType reflect.Type, buildQuery func(interface{}) ([]Query, []string), getSort func(interface{}) string, createdTimeFieldName string, updatedTimeFieldName string, options ...string) *SearchBuilder {
	return NewSearchBuilderWithQuery(client, collectionName, modelType, buildQuery, getSort, BuildSort, createdTimeFieldName, updatedTimeFieldName, options...)
}
func (b *SearchBuilder) Search(ctx context.Context, m interface{}, results interface{}, limit int64, nextPageToken string) (string, error) {
	query, fields := b.BuildQuery(m)

	s := b.GetSort(m)
	sort := b.BuildSort(s, b.ModelType)
	refId, err := BuildSearchResult(ctx, b.Collection, results, query, fields, sort, limit, nextPageToken, b.idIndex, b.createdTimeIndex, b.updatedTimeIndex)
	return refId, err
}

func BuildSearchResult(ctx context.Context, collection *firestore.CollectionRef, results interface{}, query []Query, fields []string, sort map[string]firestore.Direction, limit int64, refId string, idIndex int, createdTimeIndex int, updatedTimeIndex int) (string, error) {
	/*
		var limit, skip int64
		if initPageSize > 0 {
			if pageIndex == 1 {
				skip = 0
				limit = initPageSize
			} else {
				skip = pageSize*(pageIndex-2) + initPageSize
				limit = pageSize
			}
		} else {
			skip = pageSize * (pageIndex - 1)
			limit = pageSize
		}

		queries, er0 := BuildQuerySearch(ctx, collection, query, fields, sort, int(limit), int(skip), refId)
	*/
	var ilimit int
	if len(refId) > 0 {
		ilimit = int(limit)
	}
	queries, er0 := BuildQuerySearch(ctx, collection, query, fields, sort, ilimit, refId)
	if er0 != nil {
		return "", er0
	}
	modelType := reflect.TypeOf(results).Elem().Elem()
	iter := queries.Documents(ctx)
	var lastId string
	for {
		doc, er2 := iter.Next()
		if er2 == iterator.Done {
			break
		}
		if er2 != nil {
			return "", er2
		}
		result := reflect.New(modelType).Interface()
		lastId = doc.Ref.ID
		er3 := doc.DataTo(&result)
		if er3 != nil {
			return lastId, er3
		}
		BindCommonFields(result, doc, idIndex, createdTimeIndex, updatedTimeIndex)
		//SetValue(result, idIndex, doc.Ref.ID)
		results = appendToArray(results, result)
	}
	/*
		queryCount := BuildQuerySearch(ctx, collection, query, fields, nil, 0, 0, refId).Documents(ctx)
		countResult, er4 := queryCount.GetAll()
		if er4 != nil {
			return results, 0, er4
		}
		count := int64(len(countResult))
	*/
	return lastId, nil
}

func BuildQuerySearch(ctx context.Context, collection *firestore.CollectionRef, queries []Query, fields []string, sort map[string]firestore.Direction, limit int, refId string, options...int) (firestore.Query, error) {
	q := collection.Query
	if len(sort) > 0 {
		i := 0
		for k, v := range sort {
			if i == 0 {
				q = collection.OrderBy(k, v)
				i++
				continue
			}
			q = q.OrderBy(k, v)
		}
	}
	var skip = 0
	if len(options) > 0 && options[0] > 0 {
		skip = options[0]
	}
	if limit != 0 {
		q = q.Limit(limit).Offset(skip)
	}
	if len(queries) > 0 {
		for _, p := range queries {
			q = q.Where(p.Key, p.Operator, p.Value)
		}
	}
	if len(refId) > 0 {
		_, err := collection.Doc(refId).Get(ctx)
		if err != nil {
			return q, fmt.Errorf("failed to retrieve document with id: %s, %v", refId, err)
		}
		q = q.StartAfter(refId)
	}
	if len(fields) > 0 {
		q = q.Select(fields...)
	}
	return q, nil
}

func BuildSort(s string, modelType reflect.Type) map[string]firestore.Direction {
	var sort = make(map[string]firestore.Direction)

	if len(s) == 0 {
		return sort
	}
	sorts := strings.Split(s, ",")
	for i := 0; i < len(sorts); i++ {
		sortField := strings.TrimSpace(sorts[i])
		fieldName := sortField
		c := sortField[0:1]
		if c == "-" || c == "+" {
			fieldName = sortField[1:]
		}
		columnName := GetColumnName(modelType, fieldName)
		sortType := GetSortType(c)
		sort[columnName] = sortType
	}
	return sort
}
func GetColumnName(modelType reflect.Type, sortField string) string {
	sortField = strings.TrimSpace(sortField)
	idx, fieldName, name := GetFieldByJson(modelType, sortField)
	if len(name) > 0 {
		return name
	}
	if idx >= 0 {
		return fieldName
	}
	return sortField
}

func GetSortType(sortType string) firestore.Direction {
	if sortType == "-" {
		return firestore.Desc
	} else {
		return firestore.Asc
	}
}
