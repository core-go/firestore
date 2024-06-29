package batch

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"reflect"
)

type BatchWriter[T any] struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	Idx        int
	Map        func(*T)
}

func NewBatchWriterWithIdName[T any](client *firestore.Client, collectionName string, opts ...func(*T)) *BatchWriter[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		panic("T must be a struct")
	}
	idx := FindIdField(modelType)
	if idx < 0 {
		panic("Require Id field of " + modelType.Name() + " struct define _id bson tag.")
	}
	idField := modelType.Field(idx)
	if idField.Type.String() != "string" {
		panic(fmt.Sprintf("%s type of %s struct must be string", modelType.Field(idx).Name, modelType.Name()))
	}
	var mp func(*T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	collection := client.Collection(collectionName)
	return &BatchWriter[T]{client: client, collection: collection, Idx: idx, Map: mp}
}
func (w *BatchWriter[T]) Write(ctx context.Context, models []T) (int, error) {
	if len(models) == 0 {
		return -1, nil
	}
	if w.Map != nil {
		l := len(models)
		for i := 0; i < l; i++ {
			w.Map(&models[i])
		}
	}
	return SaveMany[T](ctx, w.client, w.collection, models, w.Idx)
}
