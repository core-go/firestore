package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"reflect"
)

type StreamBatchWriter struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	IdName     string
	modelType  reflect.Type
	modelsType reflect.Type
	batch       []interface{}
	batchSize    int
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewStreamBatchWriterWithIdName(client *firestore.Client, collectionName string, modelType reflect.Type, batchSize int, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *StreamBatchWriter {
	if len(fieldName) == 0 {
		_, idName, _ := FindIdField(modelType)
		fieldName = idName
	}
	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	collection := client.Collection(collectionName)
	batch := make([]interface{}, 0)
	return &StreamBatchWriter{client: client, collection: collection, IdName: fieldName, modelType: modelType, modelsType: modelsType, Map: mp, batchSize: batchSize, batch: batch}
}
func NewStreamBatchWriter(client *firestore.Client, collectionName string, modelType reflect.Type, batchSize int, options ...func(context.Context, interface{}) (interface{}, error)) *StreamBatchWriter {
	return NewStreamBatchWriterWithIdName(client, collectionName, modelType, batchSize, "", options...)
}

func (w *StreamBatchWriter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		w.batch = append(w.batch, m2)
	} else {
		w.batch = append(w.batch, model)
	}
	if len(w.batch) >= w.batchSize {
		_, _, err := w.flush(ctx, w.batch)
		return err
	}
	return nil
}
func (w *StreamBatchWriter) Flush(ctx context.Context) error {
	_, _, err := w.flush(ctx, w.batch)
	return err
}
func (w *StreamBatchWriter) flush(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)

	s := reflect.ValueOf(models)
	var err error
	if w.Map != nil {
		m2, er0 := MapModels(ctx, models, w.Map)
		if er0 != nil {
			err = er0
		} else {
			_, err = SaveMany(ctx, w.collection, w.client, w.IdName, m2)
		}
	} else {
		_, err = SaveMany(ctx, w.collection, w.client, w.IdName, models)
	}

	if err == nil {
		// Return full success
		for i := 0; i < s.Len(); i++ {
			successIndices = append(successIndices, i)
		}
		return successIndices, failIndices, err
	}
	//fail
	for i := 0; i < s.Len(); i++ {
		failIndices = append(failIndices, i)
	}
	w.batch = make([]interface{}, 0)
	return successIndices, failIndices, nil
}
