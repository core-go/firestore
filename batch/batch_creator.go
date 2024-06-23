package batch

import (
	"cloud.google.com/go/firestore"
	"context"
	"reflect"
)

type BatchInserter[T any] struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	Idx        int
	modelsType reflect.Type
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewBatchInserterWithIdName[T any](client *firestore.Client, collectionName string, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *BatchInserter[T] {
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
	if len(fieldName) == 0 {
		_, idName, _ := FindIdField(modelType)
		fieldName = idName
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	collection := client.Collection(collectionName)
	return &BatchInserter[T]{client: client, collection: collection, Idx: idx, Map: mp}
}

func NewBatchInserter[T any](client *firestore.Client, collectionName string, options ...func(context.Context, interface{}) (interface{}, error)) *BatchInserter[T] {
	return NewBatchInserterWithIdName[T](client, collectionName, "", options...)
}

func (w *BatchInserter[T]) Write(ctx context.Context, models []T) ([]int, error) {
	if len(models) == 0 {
		return nil, nil
	}
	if w.Map != nil {
		_, er0 := MapModels(ctx, models, w.Map)
		if er0 != nil {
			return nil, er0
		} else {
			return InsertMany(ctx, w.client, w.collection, models, w.Idx)
		}
	} else {
		return InsertMany(ctx, w.client, w.collection, models, w.Idx)
	}
}
