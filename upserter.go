package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"log"
	"reflect"
)

type Upserter struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	IdName     string
	idx        int
	modelType  reflect.Type
	modelsType reflect.Type
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewUpserterWithIdName(client *firestore.Client, collectionName string, modelType reflect.Type, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *Upserter {
	var idx int
	if len(fieldName) == 0 {
		idx, fieldName, _ = FindIdField(modelType)
		if idx < 0 {
			log.Println("Require Id value (Ex Load, Exist, Save, Upsert) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
		}
	} else {
		idx, _, _ = FindFieldByName(modelType, fieldName)
	}

	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	collection := client.Collection(collectionName)
	return &Upserter{client: client, collection: collection, IdName: fieldName, idx: idx, modelType: modelType, modelsType: modelsType, Map: mp}
}

func NewUpserter(client *firestore.Client, collectionName string, modelType reflect.Type, options ...func(context.Context, interface{}) (interface{}, error)) *Upserter {
	return NewUpserterWithIdName(client, collectionName, modelType, "", options...)
}

func (w *Upserter) Write(ctx context.Context, model interface{}) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		_, er1 := UpsertOne(ctx, w.collection, m2, w.idx)
		return er1
	}
	_, er2 := UpsertOne(ctx, w.collection, model, w.idx)
	return er2
}
