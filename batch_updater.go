package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"reflect"
)

type BatchUpdater struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	IdName     string
	modelType  reflect.Type
	modelsType reflect.Type
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewFireStoreUpdateBatchWriterWithIdName(client *firestore.Client, collectionName string, modelType reflect.Type, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *BatchUpdater {
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
	return &BatchUpdater{client, collection, fieldName, modelType, modelsType, mp}
}

func NewFireStoreUpdateBatchWriter(client *firestore.Client, collectionName string, modelType reflect.Type) *BatchUpdater {
	return NewFireStoreUpdateBatchWriterWithIdName(client, collectionName, modelType, "")
}

func (w *BatchUpdater) Write(ctx context.Context, models interface{}) ([]int, []int, error) {
	successIndices := make([]int, 0)
	failIndices := make([]int, 0)

	s := reflect.ValueOf(models)
	var err error
	if w.Map != nil {
		m2, er0 := MapModels(ctx, models, w.Map)
		if er0 != nil {
			err = er0
		} else {
			_, err = UpdateMany(ctx, w.collection, w.client, m2, w.IdName)
		}
	} else {
		_, err = UpdateMany(ctx, w.collection, w.client, models, w.IdName)
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
