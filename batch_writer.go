package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"reflect"
)

type BatchWriter struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	IdName     string
	modelType  reflect.Type
	modelsType reflect.Type
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewBatchWriterWithIdName(client *firestore.Client, collectionName string, modelType reflect.Type, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *BatchWriter {
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
	return &BatchWriter{client: client, collection: collection, IdName: fieldName, modelType: modelType, modelsType: modelsType, Map: mp}
}
func NewBatchWriter(client *firestore.Client, collectionName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *BatchWriter {
	return NewBatchWriterWithIdName(client, collectionName, modelType, "", options...)
}
func (w *BatchWriter) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
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
	return successIndices, failIndices, nil
}
