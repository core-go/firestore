package writer

import (
	"cloud.google.com/go/firestore"
	"context"
	"log"
	"reflect"

	fs "github.com/core-go/firestore"
)

type Inserter[T any] struct {
	client     *firestore.Client
	collection *firestore.CollectionRef
	IdName     string
	idx        int
	Map        func(ctx context.Context, model interface{}) (interface{}, error)
}

func NewInserterWithIdName[T any](client *firestore.Client, collectionName string, fieldName string, options ...func(context.Context, interface{}) (interface{}, error)) *Inserter[T] {
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}
	var idx int
	if len(fieldName) == 0 {
		idx, fieldName, _ = fs.FindIdField(modelType)
		if idx < 0 {
			log.Println("Require Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
		}
	} else {
		idx, _, _ = fs.FindFieldByName(modelType, fieldName)
	}

	var mp func(context.Context, interface{}) (interface{}, error)
	if len(options) >= 1 {
		mp = options[0]
	}
	collection := client.Collection(collectionName)
	return &Inserter[T]{client: client, collection: collection, IdName: fieldName, idx: idx, Map: mp}
}

func NewInserter[T any](client *firestore.Client, collectionName string, options ...func(context.Context, interface{}) (interface{}, error)) *Inserter[T] {
	return NewInserterWithIdName[T](client, collectionName, "", options...)
}

func (w *Inserter[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		m2, er0 := w.Map(ctx, model)
		if er0 != nil {
			return er0
		}
		return fs.Insert(ctx, w.collection, w.idx, m2)
	}
	return fs.Insert(ctx, w.collection, w.idx, model)
}
