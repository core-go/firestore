package writer

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"reflect"
)

type Writer[T any] struct {
	collection *firestore.CollectionRef
	idx        int
	Map        func(T)
	isPointer  bool
}

func NewWriter[T any](client *firestore.Client, collectionName string, opts ...func(T)) *Writer[T] {
	var t T
	modelType := reflect.TypeOf(t)
	isPointer := false
	if modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
		isPointer = true
	}
	idx := FindIdField(modelType)
	if idx < 0 {
		panic("Require Id field of " + modelType.Name() + " struct define _id bson tag.")
	}
	initModel := reflect.New(modelType).Interface()
	vo := reflect.Indirect(reflect.ValueOf(initModel))
	id := vo.Field(idx).Interface()
	_, ok := id.(string)
	if !ok {
		panic(fmt.Sprintf("%s type of %s struct must be string", modelType.Field(idx).Name, modelType.Name()))
	}
	var mp func(T)
	if len(opts) >= 1 {
		mp = opts[0]
	}
	collection := client.Collection(collectionName)
	return &Writer[T]{collection: collection, idx: idx, Map: mp, isPointer: isPointer}
}

func (w *Writer[T]) Write(ctx context.Context, model T) error {
	if w.Map != nil {
		w.Map(model)
	}
	vo := reflect.ValueOf(model)
	if w.isPointer {
		vo = reflect.Indirect(vo)
	}
	id := vo.Field(w.idx).Interface().(string)
	return Save(ctx, w.collection, id, model)
}
