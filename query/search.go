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
	idIndex          int
	createdTimeIndex int
	updatedTimeIndex int
}

func NewSearchBuilderWithSort[T any, F any](client *firestore.Client, collectionName string, buildQuery func(F) ([]f.Query, []string), getSort func(interface{}) string, buildSort func(s string, modelType reflect.Type) map[string]firestore.Direction, options ...string) *SearchBuilder[T, F] {
	idx := -1
	var idFieldName string
	var createdTimeFieldName string
	var updatedTimeFieldName string
	if len(options) > 0 && len(options[0]) > 0 {
		createdTimeFieldName = options[0]
	}
	if len(options) > 1 && len(options[1]) > 0 {
		updatedTimeFieldName = options[1]
	}
	if len(options) > 2 && len(options[2]) > 0 {
		idFieldName = options[2]
	}
	var t T
	modelType := reflect.TypeOf(t)
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
	return &SearchBuilder[T, F]{Collection: collection, ModelType: modelType, BuildQuery: buildQuery, BuildSort: buildSort, GetSort: getSort, idIndex: idx, createdTimeIndex: ctIdx, updatedTimeIndex: utIdx}
}
func NewSearchBuilder[T any, F any](client *firestore.Client, collectionName string, buildQuery func(F) ([]f.Query, []string), getSort func(interface{}) string, options ...string) *SearchBuilder[T, F] {
	return NewSearchBuilderWithSort[T, F](client, collectionName, buildQuery, getSort, f.BuildSort, options...)
}
func (b *SearchBuilder[T, F]) Search(ctx context.Context, filter F, limit int64, nextPageToken string) ([]T, string, error) {
	query, fields := b.BuildQuery(filter)

	s := b.GetSort(filter)
	sort := b.BuildSort(s, b.ModelType)
	var objs []T
	refId, err := f.BuildSearchResult(ctx, b.Collection, &objs, query, fields, sort, limit, nextPageToken, b.idIndex, b.createdTimeIndex, b.updatedTimeIndex)
	return objs, refId, err
}
