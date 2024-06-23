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
	Map        func(*T)
}

func NewBatchUpdater[T any](client *firestore.Client, collectionName string, opts ...func(*T)) *BatchUpdater[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		panic("T must be a struct")
	}
	idx := FindIdField(modelType)
	if idx < 0 {
		panic("Require Id field of " + modelType.Name() + " struct define _id bson tag.")
	}
	var mp func(*T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	collection := client.Collection(collectionName)
	return &BatchUpdater[T]{client, collection, idx, mp}
}

func (w *BatchUpdater[T]) Write(ctx context.Context, models []T) (int, error) {
	if len(models) == 0 {
		return -1, nil
	}
	if w.Map != nil {
		l := len(models)
		for i := 0; i < l; i++ {
			w.Map(&models[i])
		}
	}
	return UpdateMany(ctx, w.client, w.collection, models, w.Idx)
}
