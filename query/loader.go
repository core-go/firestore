package query

import (
	"cloud.google.com/go/firestore"
	"context"
	"google.golang.org/api/iterator"
	"log"
	"reflect"

	f "github.com/core-go/firestore"
)

type Loader[T any] struct {
	Collection       *firestore.CollectionRef
	ModelType        reflect.Type
	idIndex          int
	createdTimeIndex int
	updatedTimeIndex int
}

func NewLoader[T any](client *firestore.Client, collectionName string, options ...string) *Loader[T] {
	idx := -1
	var idFieldName string
	var createdTimeFieldName string
	var updatedTimeFieldName string
	if len(options) > 0 && len(options[0]) > 0 {
		createdTimeFieldName = options[0]
	}
	if len(options) > 1 && len(options[1]) > 0 {
		updatedTimeFieldName = options[1]
	}
	if len(options) > 2 && len(options[2]) > 0 {
		idFieldName = options[2]
	}
	var t T
	modelType := reflect.TypeOf(t)
	if len(idFieldName) == 0 {
		idx, _, _ = f.FindIdField(modelType)
		if idx < 0 {
			log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
		}
	} else {
		idx, _, _ = f.FindFieldByName(modelType, idFieldName)
		if idx < 0 {
			log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex Load, Exist, Save, Update) because don't have any fields of " + modelType.Name())
		}
	}
	ctIdx := -1
	if len(createdTimeFieldName) >= 0 {
		ctIdx, _, _ = f.FindFieldByName(modelType, createdTimeFieldName)
	}
	utIdx := -1
	if len(updatedTimeFieldName) >= 0 {
		utIdx, _, _ = f.FindFieldByName(modelType, updatedTimeFieldName)
	}
	return &Loader[T]{Collection: client.Collection(collectionName), ModelType: modelType, idIndex: idx, createdTimeIndex: ctIdx, updatedTimeIndex: utIdx}
}

func (s *Loader[T]) All(ctx context.Context) ([]T, error) {
	iter := s.Collection.Documents(ctx)
	var objs []T
	for {
		doc, er1 := iter.Next()
		if er1 == iterator.Done {
			break
		}
		if er1 != nil {
			return nil, er1
		}
		var obj T
		er2 := doc.DataTo(&obj)
		if er2 != nil {
			return objs, er2
		}

		f.BindCommonFields(&obj, doc, s.idIndex, s.createdTimeIndex, s.updatedTimeIndex)

		objs = append(objs, obj)
	}
	return objs, nil
}

func (s *Loader[T]) Load(ctx context.Context, id string) (*T, error) {
	var obj T
	ok, err := f.FindOneAndDecodeWithIdIndexAndTracking(ctx, s.Collection, id, &obj, s.idIndex, s.createdTimeIndex, s.updatedTimeIndex)
	if err != nil {
		return nil, err
	}
	if ok {
		return &obj, nil
	}
	return nil, nil
}

func (s *Loader[T]) Exist(ctx context.Context, id string) (bool, error) {
	return f.Exist(ctx, s.Collection, id)
}
