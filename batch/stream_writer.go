package batch

import (
	"cloud.google.com/go/firestore"
	"context"
	"reflect"
)

type StreamWriter[T any] struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	Idx        int
	batch      []T
	batchSize  int
	Map        func(T)
}

func NewStreamWriter[T any](client *firestore.Client, collectionName string, batchSize int, opts ...func(T)) *StreamWriter[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	idx := FindIdField(modelType)
	if idx < 0 {
		panic("Require Id field of " + modelType.Name() + " struct define _id bson tag.")
	}
	var mp func(T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	collection := client.Collection(collectionName)
	batch := make([]T, 0)
	return &StreamWriter[T]{client: client, collection: collection, Idx: idx, Map: mp, batchSize: batchSize, batch: batch}
}

func (w *StreamWriter[T]) Write(ctx context.Context, model T) error {
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
func (w *StreamWriter[T]) Flush(ctx context.Context) error {
	if len(w.batch) == 0 {
		return nil
	}
	_, err := SaveMany(ctx, w.client, w.collection, w.batch, w.Idx)
	w.batch = make([]T, 0)
	return err
}
