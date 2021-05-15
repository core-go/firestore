package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"reflect"
)

type Searcher struct {
	search func(context.Context, interface{}, interface{}, int64, int64, ...int64) (int64, error)
}

func NewSearcher(search func(context.Context, interface{}, interface{}, int64, int64, ...int64) (int64, error)) *Searcher {
	return &Searcher{search: search}
}

func (s *Searcher) Search(ctx context.Context, m interface{}, results interface{}, pageIndex int64, pageSize int64, options ...int64) (int64, error) {
	return s.search(ctx, m, results, pageIndex, pageSize, options...)
}

func NewSearcherWithQueryAndSort(client *firestore.Client, collectionName string, modelType reflect.Type, buildQuery func(interface{}) ([]Query, []string), getSortAndRefId func(interface{}) (string, string), buildSort func(s string, modelType reflect.Type) map[string]firestore.Direction, createdTimeFieldName string, updatedTimeFieldName string, options ...string) *Searcher {
	builder := NewSearchBuilderWithQuery(client, collectionName, modelType, buildQuery, getSortAndRefId, buildSort, createdTimeFieldName, updatedTimeFieldName, options...)
	return NewSearcher(builder.Search)
}

func NewSearcherWithQuery(client *firestore.Client, collectionName string, modelType reflect.Type, buildQuery func(interface{}) ([]Query, []string), getSortAndRefId func(interface{}) (string, string), createdTimeFieldName string, updatedTimeFieldName string, options ...string) *Searcher {
	return NewSearcherWithQueryAndSort(client, collectionName, modelType, buildQuery, getSortAndRefId, BuildSort, createdTimeFieldName, updatedTimeFieldName, options...)
}
