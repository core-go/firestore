package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"reflect"
)

type Writer struct {
	*Loader
	maps         map[string]string
	versionField string
	versionIndex int
}

func NewWriter(client *firestore.Client, collectionName string, modelType reflect.Type, createdTimeFieldName string, updatedTimeFieldName string, options ...string) *Writer {
	return NewWriterWithVersion(client, collectionName, modelType, createdTimeFieldName, updatedTimeFieldName, "", options...)
}
func NewWriterWithVersion(client *firestore.Client, collectionName string, modelType reflect.Type, createdTimeFieldName string, updatedTimeFieldName string, versionField string, options ...string) *Writer {
	defaultViewService := NewLoader(client, collectionName, modelType, createdTimeFieldName, updatedTimeFieldName, options...)
	if len(versionField) > 0 {
		index, _, _ := FindFieldByName(modelType, versionField)
		if index >= 0 {
			return &Writer{Loader: defaultViewService, maps: nil, versionField: versionField, versionIndex: index}
		}
	}
	return &Writer{defaultViewService, nil, "", -1}
}

func (s *Writer) Insert(ctx context.Context, model interface{}) (int64, error) {
	if s.versionIndex >= 0 {
		return InsertOneWithVersion(ctx, s.Collection, model, s.idIndex, s.versionIndex)
	}
	return InsertOne(ctx, s.Collection, model, s.idIndex)
}

func (s *Writer) Update(ctx context.Context, model interface{}) (int64, error) {
	if s.versionIndex >= 0 {
		return UpdateOneWithVersion(ctx, s.Collection, model, s.versionIndex, s.versionField, s.idIndex)
	}
	query := BuildQueryByIdFromObject(model, s.modelType, s.idIndex)
	return UpdateOne(ctx, s.Collection, model, query)
}

func (s *Writer) Patch(ctx context.Context, data map[string]interface{}) (int64, error) {
	if s.versionIndex >= 0 {
		return PatchOneWithVersion(ctx, s.Collection, data, s.versionIndex, s.versionField)
	}
	id := data[s.jsonIdName]
	delete(data, s.jsonIdName)
	return PatchOne(ctx, s.Collection, data, id.(string), s.Loader.Client)
}

func (s *Writer) Save(ctx context.Context, model interface{}) (int64, error) {
	if s.versionIndex >= 0 {
		return UpsertOneWithVersion(ctx, s.Collection, model, s.versionIndex, s.versionField, s.idIndex)
	}
	query := BuildQueryByIdFromObject(model, s.modelType, s.idIndex)
	id := reflect.ValueOf(model).Field(s.idIndex).Interface().(string)
	exist, er1 := Exist(ctx, s.Collection, id)
	if er1 != nil {
		return 0, er1
	}
	if !exist {
		return InsertOne(ctx, s.Collection, model, s.idIndex)
	}
	return UpdateOne(ctx, s.Collection, model, query)
}

func (s *Writer) Delete(ctx context.Context, id interface{}) (int64, error) {
	sid := id.(string)
	return DeleteOne(ctx, s.Collection, sid)
}
