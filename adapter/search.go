package adapter

import (
	"context"
	"reflect"

	"cloud.google.com/go/firestore"
	f "github.com/core-go/firestore"
)

type SearchAdapter[T any, F any] struct {
	*Adapter[T]
	BuildQuery func(F) ([]f.Query, []string)
	BuildSort  func(s string, modelType reflect.Type) map[string]firestore.Direction
	GetSort    func(interface{}) string
}

func NewSearchAdapter[T any, F any](client *firestore.Client, collectionName string, buildQuery func(F) ([]f.Query, []string), getSort func(interface{}) string, options ...string) *SearchAdapter[T, F] {
	return NewSearchAdapterWithSort[T, F](client, collectionName, buildQuery, f.BuildSort, getSort, options...)
}
func NewSearchAdapterWithSort[T any, F any](client *firestore.Client, collectionName string, buildQuery func(F) ([]f.Query, []string), buildSort func(string, reflect.Type) map[string]firestore.Direction, getSort func(interface{}) string, options ...string) *SearchAdapter[T, F] {
	var versionField string
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
		versionField = options[2]
	}
	if len(options) > 3 && len(options[3]) > 0 {
		idFieldName = options[3]
	}
	adapter := NewAdapter[T](client, collectionName, createdTimeFieldName, updatedTimeFieldName, versionField, versionField, idFieldName)
	return &SearchAdapter[T, F]{Adapter: adapter, BuildQuery: buildQuery, BuildSort: buildSort, GetSort: getSort}
}
func (b *SearchAdapter[T, F]) Search(ctx context.Context, filter F, limit int64, nextPageToken string) ([]T, string, error) {
	query, fields := b.BuildQuery(filter)

	s := b.GetSort(filter)
	sort := b.BuildSort(s, b.ModelType)
	var objs []T
	refId, err := f.BuildSearchResult(ctx, b.Collection, &objs, query, fields, sort, limit, nextPageToken, b.idIndex, b.createdTimeIndex, b.updatedTimeIndex)
	return objs, refId, err
}
