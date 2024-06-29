package batch

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"reflect"
)

type StreamCreator[T any] struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	Idx        int
	batch      []T
	batchSize  int
	Map        func(T)
}

func NewStreamCreator[T any](client *firestore.Client, collectionName string, batchSize int, opts ...func(T)) *StreamCreator[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	idx := FindIdField(modelType)
	if idx < 0 {
		panic("Require Id field of " + modelType.Name() + " struct define _id bson tag.")
	}
	idField := modelType.Field(idx)
	if idField.Type.String() != "string" {
		panic(fmt.Sprintf("%s type of %s struct must be string", modelType.Field(idx).Name, modelType.Name()))
	}
	var mp func(T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	collection := client.Collection(collectionName)
	batch := make([]T, 0)
	return &StreamCreator[T]{client: client, collection: collection, Idx: idx, Map: mp, batchSize: batchSize, batch: batch}
}

func (w *StreamCreator[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	w.batch = append(w.batch, model)
	le := len(w.batch)
	if le >= w.batchSize {
		return w.Flush(ctx)
	}
	return nil
}
func (w *StreamCreator[T]) Flush(ctx context.Context) error {
	if len(w.batch) == 0 {
		return nil
	}
	_, err := CreateMany[T](ctx, w.client, w.collection, w.batch, w.Idx)
	w.batch = make([]T, 0)
	return err
}
