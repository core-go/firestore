package query

import (
	"cloud.google.com/go/firestore"
	"context"
	"log"
	"reflect"

	f "github.com/core-go/firestore"
)

type SearchBuilder[T any, F any] struct {
	Collection       *firestore.CollectionRef
	ModelType        reflect.Type
	BuildQuery       func(F) ([]f.Query, []string)
	BuildSort        func(s string, modelType reflect.Type) map[string]firestore.Direction
	GetSort          func(interface{}) string
	Map              func(*T)
	idIndex          int
	createdTimeIndex int
	updatedTimeIndex int
}

func NewSearchBuilderWithSort[T any, F any](client *firestore.Client, collectionName string, buildQuery func(F) ([]f.Query, []string), getSort func(interface{}) string, buildSort func(s string, modelType reflect.Type) map[string]firestore.Direction, mp func(*T), opts ...string) *SearchBuilder[T, F] {
	idx := -1
	var idFieldName string
	var createdTimeFieldName string
	var updatedTimeFieldName string
	if len(opts) > 0 && len(opts[0]) > 0 {
		createdTimeFieldName = opts[0]
	}
	if len(opts) > 1 && len(opts[1]) > 0 {
		updatedTimeFieldName = opts[1]
	}
	if len(opts) > 2 && len(opts[2]) > 0 {
		idFieldName = opts[2]
	}
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		panic("T must be a struct")
	}
	if len(idFieldName) == 0 {
		idx, _, _ = f.FindIdField(modelType)
		if idx < 0 {
			log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
		}
	} else {
		idx, _, _ = f.FindFieldByName(modelType, idFieldName)
		if idx < 0 {
			log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name())
		}
	}
	ctIdx := -1
	if len(createdTimeFieldName) >= 0 {
		ctIdx, _, _ = f.FindFieldByName(modelType, createdTimeFieldName)
	}
	utIdx := -1
	if len(updatedTimeFieldName) >= 0 {
		utIdx, _, _ = f.FindFieldByName(modelType, updatedTimeFieldName)
	}
	collection := client.Collection(collectionName)
	return &SearchBuilder[T, F]{Collection: collection, ModelType: modelType, BuildQuery: buildQuery, BuildSort: buildSort, GetSort: getSort, Map: mp, idIndex: idx, createdTimeIndex: ctIdx, updatedTimeIndex: utIdx}
}
func NewSearchBuilderWithMap[T any, F any](client *firestore.Client, collectionName string, buildQuery func(F) ([]f.Query, []string), getSort func(interface{}) string, mp func(*T), opts ...string) *SearchBuilder[T, F] {
	return NewSearchBuilderWithSort[T, F](client, collectionName, buildQuery, getSort, f.BuildSort, mp, opts...)
}
func NewSearchBuilder[T any, F any](client *firestore.Client, collectionName string, buildQuery func(F) ([]f.Query, []string), getSort func(interface{}) string, opts ...string) *SearchBuilder[T, F] {
	return NewSearchBuilderWithSort[T, F](client, collectionName, buildQuery, getSort, f.BuildSort, nil, opts...)
}
func (b *SearchBuilder[T, F]) Search(ctx context.Context, filter F, limit int64, nextPageToken string) ([]T, string, error) {
	query, fields := b.BuildQuery(filter)

	s := b.GetSort(filter)
	sort := b.BuildSort(s, b.ModelType)
	var objs []T
	refId, err := f.BuildSearchResult(ctx, b.Collection, &objs, query, fields, sort, limit, nextPageToken, b.idIndex, b.createdTimeIndex, b.updatedTimeIndex)
	if b.Map != nil {
		l := len(objs)
		for i := 0; i < l; i++ {
			b.Map(&objs[i])
		}
	}
	return objs, refId, err
}
