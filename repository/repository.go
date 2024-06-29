package repository

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"cloud.google.com/go/firestore"
	f "github.com/core-go/firestore"
	"google.golang.org/api/iterator"
)

type Repository[T any] struct {
	Collection       *firestore.CollectionRef
	ModelType        reflect.Type
	idIndex          int
	idJson           string
	createdTimeIndex int
	updatedTimeIndex int
	updatedTimeJson  string
	Map              map[string]string
	versionField     string
	versionJson      string
	versionFirestore string
	versionIndex     int
}

func NewRepository[T any](client *firestore.Client, collectionName string, options ...string) *Repository[T] {
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
	if modelType.Kind() != reflect.Struct {
		panic("T must be a struct")
	}
	var idJson string
	if len(idFieldName) == 0 {
		idx, _, idJson = f.FindIdField(modelType)
		if idx < 0 {
			panic(fmt.Sprintf("%s struct requires id field which has bson tag '_id'", modelType.Name()))
		}
	} else {
		idx, _, _ = f.FindFieldByName(modelType, idFieldName)
		if idx < 0 {
			panic(fmt.Sprintf("%s struct requires id field which id name is '%s'", modelType.Name(), idFieldName))
		}
	}
	idField := modelType.Field(idx)
	if idField.Type.String() != "string" {
		panic(fmt.Sprintf("%s type of %s struct must be string", modelType.Field(idx).Name, modelType.Name()))
	}
	ctIdx := -1
	if len(createdTimeFieldName) >= 0 {
		ctIdx, _, _ = f.FindFieldByName(modelType, createdTimeFieldName)
		if ctIdx >= 0 {
			ctn := modelType.Field(ctIdx).Type.String()
			if ctn != "*time.Time" {
				panic(fmt.Sprintf("%s type of %s struct must be *time.Time", modelType.Field(ctIdx).Name, modelType.Name()))
			}
		}
	}
	utIdx := -1
	var updatedTimeJson string
	if len(updatedTimeFieldName) >= 0 {
		utIdx, updatedTimeJson, _ = f.FindFieldByName(modelType, updatedTimeFieldName)
		if utIdx >= 0 {
			ctn := modelType.Field(utIdx).Type.String()
			if ctn != "*time.Time" {
				panic(fmt.Sprintf("%s type of %s struct must be *time.Time", modelType.Field(utIdx).Name, modelType.Name()))
			}
		}
	}
	maps := f.MakeFirestoreMap(modelType)
	adapter := &Repository[T]{Collection: client.Collection(collectionName), ModelType: modelType, idIndex: idx, idJson: idJson, Map: maps, createdTimeIndex: ctIdx, updatedTimeIndex: utIdx, updatedTimeJson: updatedTimeJson, versionIndex: versionIndex}
	if len(versionField) > 0 {
		index, versionJson, versionFirestore := f.FindFieldByName(modelType, versionField)
		if index >= 0 {
			vn := modelType.Field(index).Type.String()
			if !(vn == "int" || vn == "int32" || vn == "int64") {
				panic(fmt.Sprintf("%s type of %s struct must be int or int32 or int64", versionField, modelType.Name()))
			}
			adapter.versionField = versionField
			adapter.versionIndex = index
			adapter.versionJson = versionJson
			adapter.versionFirestore = versionFirestore
		}
	}
	return adapter
}

func (a *Repository[T]) All(ctx context.Context) ([]T, error) {
	iter := a.Collection.Documents(ctx)
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

		f.BindCommonFields(&obj, doc, a.idIndex, a.createdTimeIndex, a.updatedTimeIndex)

		objs = append(objs, obj)
	}
	return objs, nil
}

func (a *Repository[T]) Load(ctx context.Context, id string) (*T, error) {
	var obj T
	ok, doc, err := f.Load(ctx, a.Collection, id, &obj)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	f.BindCommonFields(&obj, doc, a.idIndex, a.createdTimeIndex, a.updatedTimeIndex)
	return &obj, nil
}

func (a *Repository[T]) Exist(ctx context.Context, id string) (bool, error) {
	return f.Exist(ctx, a.Collection, id)
}
func (a *Repository[T]) Create(ctx context.Context, model *T) (int64, error) {
	mv := reflect.Indirect(reflect.ValueOf(model))
	id := mv.Field(a.idIndex).Interface().(string)
	if a.versionIndex >= 0 {
		setVersion(mv, a.versionIndex)
	}
	res, rid, updateTime, err := f.Create(ctx, a.Collection, id, model)
	if len(id) == 0 && len(rid) > 0 {
		fv := mv.Field(a.idIndex)
		fv.Set(reflect.ValueOf(rid))
	}
	if updateTime != nil {
		if a.createdTimeIndex >= 0 {
			cv := mv.Field(a.createdTimeIndex)
			cv.Set(reflect.ValueOf(updateTime))
		}
		if a.updatedTimeIndex >= 0 {
			cv := mv.Field(a.updatedTimeIndex)
			cv.Set(reflect.ValueOf(updateTime))
		}
	}
	return res, err
}
func (a *Repository[T]) Save(ctx context.Context, model *T) (int64, error) {
	mv := reflect.Indirect(reflect.ValueOf(model))
	id := mv.Field(a.idIndex).Interface().(string)
	if len(id) == 0 {
		return a.Create(ctx, model)
	}
	if a.versionIndex < 0 {
		res, updateTime, err := f.Save(ctx, a.Collection, id, model)
		if updateTime != nil {
			if a.updatedTimeIndex >= 0 {
				cv := mv.Field(a.createdTimeIndex)
				cv.Set(reflect.ValueOf(updateTime))
			}
		}
		return res, err
	}
	docRef := a.Collection.Doc(id)
	doc, er0 := docRef.Get(ctx)
	if er0 != nil {
		if strings.HasSuffix(er0.Error(), " not found") {
			setVersion(mv, a.versionIndex)
			res, _, updateTime, err := f.Create(ctx, a.Collection, id, model)
			if updateTime != nil {
				if a.createdTimeIndex >= 0 {
					cv := mv.Field(a.createdTimeIndex)
					cv.Set(reflect.ValueOf(updateTime))
				}
				if a.updatedTimeIndex >= 0 {
					cv := mv.Field(a.updatedTimeIndex)
					cv.Set(reflect.ValueOf(updateTime))
				}
			}
			return res, err
		}
		return -1, er0
	}
	dbMap := doc.Data()
	currentVersion := mv.Field(a.versionIndex).Interface()
	scurrentVer := fmt.Sprintf("%v", currentVersion)
	dbVer := fmt.Sprintf("%v", dbMap[a.versionFirestore])
	if scurrentVer != dbVer {
		return -1, nil
	}
	increaseVersion(mv, a.versionIndex, currentVersion)
	res, err := docRef.Set(ctx, model)
	if err != nil {
		return -1, err
	}
	if a.createdTimeIndex >= 0 {
		cv := mv.Field(a.createdTimeIndex)
		cv.Set(reflect.ValueOf(&doc.CreateTime))
	}
	if a.updatedTimeIndex >= 0 {
		cv := mv.Field(a.updatedTimeIndex)
		cv.Set(reflect.ValueOf(&res.UpdateTime))
	}
	return 1, nil
}

func (a *Repository[T]) Update(ctx context.Context, model *T) (int64, error) {
	mv := reflect.Indirect(reflect.ValueOf(model))
	id := mv.Field(a.idIndex).Interface().(string)
	if a.versionIndex >= 0 {
		docRef := a.Collection.Doc(id)
		doc, er0 := docRef.Get(ctx)
		if er0 != nil {
			if strings.HasSuffix(er0.Error(), " not found") {
				return 0, nil
			}
			return -1, er0
		}
		dbMap := doc.Data()
		currentVersion := mv.Field(a.versionIndex).Interface()
		scurrentVer := fmt.Sprintf("%v", currentVersion)
		dbVer := fmt.Sprintf("%v", dbMap[a.versionFirestore])
		if scurrentVer != dbVer {
			return -1, nil
		}
		increaseVersion(mv, a.versionIndex, currentVersion)
		res, err := docRef.Set(ctx, model)
		if err != nil {
			return -1, err
		}
		if a.createdTimeIndex >= 0 {
			cv := mv.Field(a.createdTimeIndex)
			cv.Set(reflect.ValueOf(&doc.CreateTime))
		}
		if a.updatedTimeIndex >= 0 {
			cv := mv.Field(a.updatedTimeIndex)
			cv.Set(reflect.ValueOf(&res.UpdateTime))
		}
		return 1, nil
	}
	res, updateTime, err := f.Update(ctx, a.Collection, id, model)
	if updateTime != nil {
		if a.updatedTimeIndex >= 0 {
			cv := mv.Field(a.createdTimeIndex)
			cv.Set(reflect.ValueOf(updateTime))
		}
	}
	return res, err
}

func (a *Repository[T]) Patch(ctx context.Context, data map[string]interface{}) (int64, error) {
	sid, ok := data[a.idJson]
	if !ok {
		return -1, fmt.Errorf("%s must be in map[string]interface{} for patch", a.idJson)
	}
	id := sid.(string)
	delete(data, a.idJson)
	docRef := a.Collection.Doc(id)
	doc, er0 := docRef.Get(ctx)
	if er0 != nil {
		if strings.HasSuffix(er0.Error(), " not found") {
			return 0, nil
		}
		return -1, er0
	}
	dbMap := doc.Data()
	if a.versionIndex >= 0 {
		currentVersion, vok := data[a.versionJson]
		if !vok {
			return -1, fmt.Errorf("%s must be in model for patch", a.versionJson)
		}
		scurrentVer := fmt.Sprintf("%v", currentVersion)
		dbVer := fmt.Sprintf("%v", dbMap[a.versionFirestore])
		if scurrentVer != dbVer {
			return -1, nil
		}
		increaseMapVersion(data, a.versionJson, currentVersion)
	}
	fsMap := f.MapToFirestore(data, dbMap, a.Map)
	res, err := docRef.Set(ctx, fsMap)
	if err != nil {
		return -1, err
	}
	if len(a.updatedTimeJson) >= 0 {
		data[a.updatedTimeJson] = res.UpdateTime
	}
	return 1, nil
}

func (a *Repository[T]) Delete(ctx context.Context, id string) (int64, error) {
	return f.Delete(ctx, a.Collection, id)
}

func setVersion(vo reflect.Value, versionIndex int) bool {
	versionType := vo.Field(versionIndex).Type().String()
	switch versionType {
	case "int32":
		vo.Field(versionIndex).Set(reflect.ValueOf(int32(1)))
		return true
	case "int":
		vo.Field(versionIndex).Set(reflect.ValueOf(1))
		return true
	case "int64":
		vo.Field(versionIndex).Set(reflect.ValueOf(int64(1)))
		return true
	default:
		return false
	}
}
func increaseVersion(vo reflect.Value, versionIndex int, curVer interface{}) bool {
	versionType := vo.Field(versionIndex).Type().String()
	switch versionType {
	case "int32":
		nextVer := curVer.(int32) + 1
		vo.Field(versionIndex).Set(reflect.ValueOf(nextVer))
		return true
	case "int":
		nextVer := curVer.(int) + 1
		vo.Field(versionIndex).Set(reflect.ValueOf(nextVer))
		return true
	case "int64":
		nextVer := curVer.(int64) + 1
		vo.Field(versionIndex).Set(reflect.ValueOf(nextVer))
		return true
	default:
		return false
	}
}
func increaseMapVersion(model map[string]interface{}, name string, currentVersion interface{}) bool {
	if versionI32, ok := currentVersion.(int32); ok {
		model[name] = versionI32 + 1
		return true
	} else if versionI, ok := currentVersion.(int); ok {
		model[name] = versionI + 1
		return true
	} else if versionI64, ok := currentVersion.(int64); ok {
		model[name] = versionI64 + 1
		return true
	} else {
		return false
	}
}
