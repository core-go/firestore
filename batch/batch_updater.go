package batch

import (
	"cloud.google.com/go/firestore"
	"context"
	"reflect"
)

type BatchUpdater[T any] struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	Idx        int
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewBatchUpdaterWithIdName[T any](client *firestore.Client, collectionName string, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *BatchUpdater[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	var idx int
	if len(fieldName) == 0 {
		idx, _, _ = FindIdField(modelType)
	} else {
		idx, _, _ = FindFieldByName(modelType, fieldName)
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	collection := client.Collection(collectionName)
	return &BatchUpdater[T]{client, collection, idx, mp}
}

func NewBatchUpdater[T any](client *firestore.Client, collectionName string) *BatchUpdater[T] {
	return NewBatchUpdaterWithIdName[T](client, collectionName, "")
}

func (w *BatchUpdater[T]) Write(ctx context.Context, models []T) ([]int, error) {
	if w.Map != nil {
		_, er0 := MapModels(ctx, models, w.Map)
		if er0 != nil {
			return nil, er0
		} else {
			return UpdateMany(ctx, w.client, w.collection, models, w.Idx)
		}
	} else {
		return UpdateMany(ctx, w.client, w.collection, models, w.Idx)
	}
}
