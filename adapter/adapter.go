package adapter

import (
	"context"
	"log"
	"reflect"

	"cloud.google.com/go/firestore"
	f "github.com/core-go/firestore"
	"google.golang.org/api/iterator"
)

type Adapter[T any] struct {
	Collection       *firestore.CollectionRef
	ModelType        reflect.Type
	idIndex          int
	jsonIdName       string
	createdTimeIndex int
	updatedTimeIndex int
	Map              map[string]string
	versionField     string
	versionJson      string
	versionFirestore string
	versionIndex     int
}

func NewAdapter[T any](client *firestore.Client, collectionName string, options ...string) *Adapter[T] {
	idx := -1
	versionIndex := -1
	var versionField string
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
		versionField = options[2]
	}
	if len(options) > 3 && len(options[3]) > 0 {
		idFieldName = options[3]
	}
	var t T
	modelType := reflect.TypeOf(t)
	var jsonIdName string
	if len(idFieldName) == 0 {
		idx, _, jsonIdName = f.FindIdField(modelType)
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
	maps := f.MakeFirestoreMap(modelType)
	adapter := &Adapter[T]{Collection: client.Collection(collectionName), ModelType: modelType, idIndex: idx, jsonIdName: jsonIdName, Map: maps, createdTimeIndex: ctIdx, updatedTimeIndex: utIdx, versionIndex: versionIndex}
	if len(versionField) > 0 {
		index, versionJson, versionFirestore := f.FindFieldByName(modelType, versionField)
		if index >= 0 {
			adapter.versionField = versionField
			adapter.versionIndex = index
			adapter.versionJson = versionJson
			adapter.versionFirestore = versionFirestore
		}
	}
	return adapter
}

func (s *Adapter[T]) All(ctx context.Context) ([]T, error) {
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

func (s *Adapter[T]) Load(ctx context.Context, id string) (*T, error) {
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

func (s *Adapter[T]) Exist(ctx context.Context, id string) (bool, error) {
	return f.Exist(ctx, s.Collection, id)
}
func (s *Adapter[T]) Create(ctx context.Context, model *T) (int64, error) {
	mv := reflect.ValueOf(model)
	id := reflect.Indirect(mv).Field(s.idIndex).Interface().(string)
	if s.versionIndex >= 0 {
		return f.InsertOneWithVersion(ctx, s.Collection, id, model, s.versionIndex)
	}
	return f.InsertOne(ctx, s.Collection, id, model)
}
func (s *Adapter[T]) Update(ctx context.Context, model *T) (int64, error) {
	mv := reflect.ValueOf(model)
	id := reflect.Indirect(mv).Field(s.idIndex).Interface().(string)
	if s.versionIndex >= 0 {
		return f.UpdateOneWithVersion(ctx, s.Collection, model, s.versionIndex, s.versionField, s.idIndex)
	}
	return f.UpdateOne(ctx, s.Collection, id, model)
}

func (s *Adapter[T]) Patch(ctx context.Context, data map[string]interface{}) (int64, error) {
	id := data[s.jsonIdName]
	if s.versionIndex >= 0 {
		return f.PatchOneWithVersion(ctx, s.Collection, id.(string), data, s.Map, s.versionJson)
	}
	delete(data, s.jsonIdName)
	return f.PatchOne(ctx, s.Collection, id.(string), data, s.Map)
}
func (s *Adapter[T]) Save(ctx context.Context, model interface{}) (int64, error) {
	mv := reflect.ValueOf(model)
	id := reflect.Indirect(mv).Field(s.idIndex).Interface().(string)
	if s.versionIndex >= 0 {
		return f.SaveOneWithVersion(ctx, s.Collection, id, model, s.versionIndex, s.versionField)
	}
	exist, er1 := f.Exist(ctx, s.Collection, id)
	if er1 != nil {
		return 0, er1
	}
	if !exist {
		return f.InsertOne(ctx, s.Collection, id, model)
	}
	return f.UpdateOne(ctx, s.Collection, id, model)
}
func (s *Adapter[T]) Delete(ctx context.Context, id string) (int64, error) {
	return f.DeleteOne(ctx, s.Collection, id)
}
