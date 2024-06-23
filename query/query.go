package query

import (
	"context"
	"reflect"

	"cloud.google.com/go/firestore"
	f "github.com/core-go/firestore"
)

type Query[T any, F any] struct {
	*Loader[T]
	ModelType  reflect.Type
	BuildQuery func(F) ([]f.Query, []string)
	BuildSort  func(s string, modelType reflect.Type) map[string]firestore.Direction
	GetSort    func(interface{}) string
}

func NewQueryWithSort[T any, F any](client *firestore.Client, collectionName string, buildQuery func(F) ([]f.Query, []string), buildSort func(string, reflect.Type) map[string]firestore.Direction, getSort func(interface{}) string, mp func(*T), opts ...string) *Query[T, F] {
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
	loader := NewLoaderWithMap[T](client, collectionName, mp, createdTimeFieldName, updatedTimeFieldName, idFieldName)
	var t T
	modelType := reflect.TypeOf(t)
	return &Query[T, F]{Loader: loader, ModelType: modelType, BuildQuery: buildQuery, BuildSort: buildSort, GetSort: getSort}
}
func NewQueryWithMap[T any, F any](client *firestore.Client, collectionName string, buildQuery func(F) ([]f.Query, []string), getSort func(interface{}) string, mp func(*T), opts ...string) *Query[T, F] {
	return NewQueryWithSort[T, F](client, collectionName, buildQuery, f.BuildSort, getSort, mp, opts...)
}
func NewQuery[T any, F any](client *firestore.Client, collectionName string, buildQuery func(F) ([]f.Query, []string), getSort func(interface{}) string, opts ...string) *Query[T, F] {
	return NewQueryWithSort[T, F](client, collectionName, buildQuery, f.BuildSort, getSort, nil, opts...)
}
func (b *Query[T, F]) Search(ctx context.Context, filter F, limit int64, nextPageToken string) ([]T, string, error) {
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
