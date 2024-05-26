package writer

import (
	"cloud.google.com/go/firestore"
	"context"
	"log"
	"reflect"

	fs "github.com/core-go/firestore"
)

type FirestoreWriter[T any] struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	IdName     string
	idx        int
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewFirestoreWriterWithId[T any](client *firestore.Client, collectionName string, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *FirestoreWriter[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	var idx int
	if len(fieldName) == 0 {
		idx, fieldName, _ = fs.FindIdField(modelType)
		if idx < 0 {
			log.Println("Require Id value (Ex Load, Exist, Update, Save) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
		}
	} else {
		idx, _, _ = fs.FindFieldByName(modelType, fieldName)
	}

	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	collection := client.Collection(collectionName)
	return &FirestoreWriter[T]{client: client, collection: collection, IdName: fieldName, idx: idx, Map: mp}
}

func NewFirestoreWriter[T any](client *firestore.Client, collectionName string, options ...func(context.Context, interface{}) (interface{}, error)) *FirestoreWriter[T] {
	return NewFirestoreWriterWithId[T](client, collectionName, "", options...)
}

func (w *FirestoreWriter[T]) Write(ctx context.Context, model T) error {
	id := fs.GetIdValueFromModel(model, w.idx)
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		if len(id) == 0 {
			doc := w.collection.NewDoc()
			_, er1 := doc.Create(ctx, m2)
			return er1
		}
		_, er1 := fs.UpdateOne(ctx, w.collection, id, m2)
		return er1
	}
	if len(id) == 0 {
		doc := w.collection.NewDoc()
		_, er2 := doc.Create(ctx, model)
		return er2
	}
	_, er2 := fs.UpdateOne(ctx, w.collection, id, model)
	return er2
}
