package query

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"reflect"

	f "github.com/core-go/firestore"
)

type Loader[T any] struct {
	Collection       *firestore.CollectionRef
	Map              func(*T)
	idIndex          int
	createdTimeIndex int
	updatedTimeIndex int
}

func NewLoader[T any](client *firestore.Client, collectionName string, opts ...string) *Loader[T] {
	return NewLoaderWithMap[T](client, collectionName, nil, opts...)
}
func NewLoaderWithMap[T any](client *firestore.Client, collectionName string, mp func(*T), opts ...string) *Loader[T] {
	idx := -1
	var idFieldName string
	var createdTimeFieldName string
	var updatedTimeFieldName string
	if len(opts) > 0 && len(opts[0]) > 0 {
		createdTimeFieldName = opts[0]
	}
	if len(opts) > 1 && len(opts[1]) > 0 {
		updatedTimeFieldName = opts[1]
	}
	if len(opts) > 2 && len(opts[2]) > 0 {
		idFieldName = opts[2]
	}
	var t T
	modelType := reflect.TypeOf(t)
	if modelType.Kind() != reflect.Struct {
		panic("T must be a struct")
	}
	if len(idFieldName) == 0 {
		idx, _, _ = f.FindIdField(modelType)
		if idx < 0 {
			panic(fmt.Sprintf("%s struct requires id field which has bson tag '_id'", modelType.Name()))
		}
	} else {
		idx, _, _ = f.FindFieldByName(modelType, idFieldName)
		if idx < 0 {
			panic(fmt.Sprintf("%s struct requires id field which id name is '%s'", modelType.Name(), idFieldName))
		}
	}
	mv := reflect.ValueOf(t)
	id := mv.Field(idx).Interface()
	_, ok := id.(string)
	if !ok {
		panic(fmt.Sprintf("%s type of %s struct must be string", modelType.Field(idx).Name, modelType.Name()))
	}
	ctIdx := -1
	if len(createdTimeFieldName) >= 0 {
		ctIdx, _, _ = f.FindFieldByName(modelType, createdTimeFieldName)
	}
	utIdx := -1
	if len(updatedTimeFieldName) >= 0 {
		utIdx, _, _ = f.FindFieldByName(modelType, updatedTimeFieldName)
	}
	return &Loader[T]{Collection: client.Collection(collectionName), Map: mp, idIndex: idx, createdTimeIndex: ctIdx, updatedTimeIndex: utIdx}
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
		if s.Map != nil {
			s.Map(&obj)
		}
		objs = append(objs, obj)
	}
	return objs, nil
}

func (s *Loader[T]) Load(ctx context.Context, id string) (*T, error) {
	var obj T
	ok, doc, err := f.Load(ctx, s.Collection, id, &obj)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	f.BindCommonFields(&obj, doc, s.idIndex, s.createdTimeIndex, s.updatedTimeIndex)
	if s.Map != nil {
		s.Map(&obj)
	}
	return &obj, nil
}

func (s *Loader[T]) Exist(ctx context.Context, id string) (bool, error) {
	return f.Exist(ctx, s.Collection, id)
}
