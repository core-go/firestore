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
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewStreamWriterWithIdName[T any](client *firestore.Client, collectionName string, batchSize int, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *StreamWriter[T] {
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
	batch := make([]T, 0)
	return &StreamWriter[T]{client: client, collection: collection, Idx: idx, Map: mp, batchSize: batchSize, batch: batch}
}
func NewStreamWriter[T any](client *firestore.Client, collectionName string, batchSize int, options ...func(context.Context, interface{}) (interface{}, error)) *StreamWriter[T] {
	return NewStreamWriterWithIdName[T](client, collectionName, batchSize, "", options...)
}

func (w *StreamWriter[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		w.batch = append(w.batch, m2.(T))
	} else {
		w.batch = append(w.batch, model)
	}
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
