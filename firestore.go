package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"errors"
	"firebase.google.com/go"
	"fmt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	"reflect"
	"strings"
)

func Connect(ctx context.Context, credentials []byte) (*firestore.Client, error) {
	app, er1 := firebase.NewApp(ctx, nil, option.WithCredentialsJSON(credentials))
	if er1 != nil {
		log.Fatalf("Could not create admin client: %v", er1)
		return nil, er1
	}

	client, er2 := app.Firestore(ctx)
	if er2 != nil {
		log.Fatalf("Could not create data operations client: %v", er2)
		return nil, er2
	}
	return client, nil
}

func BuildQuery(collection *firestore.CollectionRef, queries []Query, limit int, selectFields ...string) firestore.Query {
	var q firestore.Query
	if limit != 0 {
		q = collection.Limit(limit)
	}
	if len(queries) == 0 {
		return collection.Select(selectFields...)
	}
	for i, p := range queries {
		if i == 0 {
			q = collection.Where(p.Key, p.Operator, p.Value)
		}
		q = q.Where(p.Key, p.Operator, p.Value)
	}
	return q
}

func GetDocuments(ctx context.Context, collection *firestore.CollectionRef, where []Query, limit int) *firestore.DocumentIterator {
	if len(where) > 0 {
		return BuildQuery(collection, where, limit).Documents(ctx)
	}
	if limit != 0 {
		return collection.Limit(limit).Documents(ctx)
	}
	return collection.Documents(ctx)
}

func BuildObjectToDotNotationUpdate(data interface{}) []firestore.Update {
	var q []firestore.Update
	items, _ := Notation(data, SkipEmpty, ".")
	for i := range items {
		q = append(q, firestore.Update{Path: items[i].Key, Value: items[i].Value})
	}
	return q
}

func Exist(ctx context.Context, collection *firestore.CollectionRef, docID string) (bool, error) {
	_, err := collection.Doc(docID).Get(ctx)
	if err != nil {
		return false, err
	}
	return true, nil
}

func Find(ctx context.Context, collection *firestore.CollectionRef, where []Query, modelType reflect.Type) (interface{}, error) {
	idx, _, _ := FindIdField(modelType)
	if idx < 0 {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex GetById, ExistsById, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	return FindWithIdIndexAndTracking(ctx, collection, where, modelType, idx, -1, -1)
}

func FindWithIdIndex(ctx context.Context, collection *firestore.CollectionRef, where []Query, modelType reflect.Type, idIndex int) (interface{}, error) {
	return FindWithIdIndexAndTracking(ctx, collection, where, modelType, idIndex, -1, -1)
}

func FindWithTracking(ctx context.Context, collection *firestore.CollectionRef, where []Query, modelType reflect.Type, createdTimeIndex int, updatedTimeIndex int) (interface{}, error) {
	idx, _, _ := FindIdField(modelType)
	if idx < 0 {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex GetById, ExistsById, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	return FindWithIdIndexAndTracking(ctx, collection, where, modelType, idx, createdTimeIndex, updatedTimeIndex)
}

func FindWithIdIndexAndTracking(ctx context.Context, collection *firestore.CollectionRef, where []Query, modelType reflect.Type, idIndex int, createdTimeIndex int, updatedTimeIndex int) (interface{}, error) {
	iter := GetDocuments(ctx, collection, where, 0)
	modelsType := reflect.Zero(reflect.SliceOf(modelType)).Type()
	arr := reflect.New(modelsType).Interface()
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		result := reflect.New(modelType).Interface()
		err = doc.DataTo(&result)
		if err != nil {
			return nil, err
		}
		BindCommonFields(result, doc, idIndex, createdTimeIndex, updatedTimeIndex)
		//SetValue(result, idIndex, doc.Ref.ID)
		arr = appendToArray(arr, result)
	}
	return arr, nil
}

func FindOne(ctx context.Context, collection *firestore.CollectionRef, docID string, modelType reflect.Type) (interface{}, error) {
	idx, _, _ := FindIdField(modelType)
	if idx < 0 {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex GetById, ExistsById, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	return FindOneWithIdIndexAndTracking(ctx, collection, docID, modelType, idx, -1, -1)
}

func FindOneWithIdIndex(ctx context.Context, collection *firestore.CollectionRef, docID string, modelType reflect.Type, idIndex int) (interface{}, error) {
	return FindOneWithIdIndexAndTracking(ctx, collection, docID, modelType, idIndex, -1, -1)
}

func FindOneWithTracking(ctx context.Context, collection *firestore.CollectionRef, docID string, modelType reflect.Type, createdTimeIndex int, updatedTimeIndex int) (interface{}, error) {
	idx, _, _ := FindIdField(modelType)
	if idx < 0 {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex GetById, ExistsById, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	return FindOneWithIdIndexAndTracking(ctx, collection, docID, modelType, idx, createdTimeIndex, updatedTimeIndex)
}

func FindOneWithIdIndexAndTracking(ctx context.Context, collection *firestore.CollectionRef, docID string, modelType reflect.Type, idIndex int, createdTimeIndex int, updatedTimeIndex int) (interface{}, error) {
	doc, er1 := collection.Doc(docID).Get(ctx)
	if er1 != nil {
		return nil, er1
	}
	result := reflect.New(modelType).Interface()
	er2 := doc.DataTo(&result)
	if er2 != nil {
		return nil, er2
	}
	BindCommonFields(result, doc, idIndex, createdTimeIndex, updatedTimeIndex)
	//SetValue(result, idIndex, doc.Ref.ID)
	return result, nil
}

func BindCommonFields(result interface{}, doc *firestore.DocumentSnapshot, idIndex int, createdTimeIndex int, updatedTimeIndex int) {
	rv := reflect.Indirect(reflect.ValueOf(result))
	fv := rv.Field(idIndex)
	fv.Set(reflect.ValueOf(doc.Ref.ID))

	if createdTimeIndex >= 0 {
		cv := rv.Field(createdTimeIndex)
		cv.Set(reflect.ValueOf(doc.CreateTime))
	}
	if updatedTimeIndex >= 0 {
		uv := rv.Field(updatedTimeIndex)
		uv.Set(reflect.ValueOf(doc.UpdateTime))
	}
}

func FindOneAndDecode(ctx context.Context, collection *firestore.CollectionRef, docID string, result interface{}) (bool, error) {
	modelType := reflect.Indirect(reflect.ValueOf(result)).Type()
	//modelType := reflect.TypeOf(result)
	idx, _, _ := FindIdField(modelType)
	if idx < 0 {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex GetById, ExistsById, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	return FindOneAndDecodeWithIdIndexAndTracking(ctx, collection, docID, result, idx, -1, -1)
}

func FindOneAndDecodeWithIdIndex(ctx context.Context, collection *firestore.CollectionRef, docID string, result interface{}, idIndex int) (interface{}, error) {
	return FindOneAndDecodeWithIdIndexAndTracking(ctx, collection, docID, result, idIndex, -1, -1)
}

func FindOneAndDecodeWithTracking(ctx context.Context, collection *firestore.CollectionRef, docID string, result interface{}, createdTimeIndex int, updatedTimeIndex int) (interface{}, error) {
	modelType := reflect.TypeOf(result)
	idx, _, _ := FindIdField(modelType)
	if idx < 0 {
		log.Println(modelType.Name() + " repository can't use functions that need Id value (Ex GetById, ExistsById, Save, Update) because don't have any fields of " + modelType.Name() + " struct define _id bson tag.")
	}
	return FindOneAndDecodeWithIdIndexAndTracking(ctx, collection, docID, modelType, idx, createdTimeIndex, updatedTimeIndex)
}

func FindOneAndDecodeWithIdIndexAndTracking(ctx context.Context, collection *firestore.CollectionRef, docID string, result interface{}, idIndex int, createdTimeIndex int, updatedTimeIndex int) (bool, error) {
	doc, err := collection.Doc(docID).Get(ctx)
	if err != nil {
		return false, err
	}
	err = doc.DataTo(result)
	if err != nil {
		return false, err
	}
	BindCommonFields(result, doc, idIndex, createdTimeIndex, updatedTimeIndex)
	return true, nil
}

func FindOneMap(ctx context.Context, collection *firestore.CollectionRef, docID string) (bool, map[string]interface{}, error) {
	doc, err := collection.Doc(docID).Get(ctx)
	if err != nil {
		return false, nil, err
	}
	return true, doc.Data(), nil
}
func FindOneDoc(ctx context.Context, collection *firestore.CollectionRef, docID string) (bool, *firestore.DocumentSnapshot, error) {
	doc, err := collection.Doc(docID).Get(ctx)
	if err != nil {
		return false, nil, err
	}
	return true, doc, nil
}
func FindOneWithQueries(ctx context.Context, collection *firestore.CollectionRef, where []Query, modelType reflect.Type, createdTimeIndex int, updatedTimeIndex int) (interface{}, error) {
	return FindOneWithQueriesAndTracking(ctx, collection, where, modelType, -1, -1)
}

func FindOneWithQueriesAndTracking(ctx context.Context, collection *firestore.CollectionRef, where []Query, modelType reflect.Type, createdTimeIndex int, updatedTimeIndex int) (interface{}, error) {
	iter := GetDocuments(ctx, collection, where, 1)
	idx, _, _ := FindIdField(modelType)
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		result := reflect.New(modelType).Interface()
		err = doc.DataTo(&result)
		if err != nil {
			return nil, err
		}
		BindCommonFields(result, doc, idx, createdTimeIndex, updatedTimeIndex)
		return result, nil
	}
	return nil, status.Errorf(codes.NotFound, "not found")
}

func FindByField(ctx context.Context, collection *firestore.CollectionRef, values []string, modelType reflect.Type, jsonName string) (interface{}, []error) {
	return FindByFieldWithTracking(ctx, collection, values, modelType, jsonName, -1, -1)
}

func FindByFieldWithTracking(ctx context.Context, collection *firestore.CollectionRef, values []string, modelType reflect.Type, jsonName string, createdTimeIndex int, updatedTimeIndex int) (interface{}, []error) {
	idx, _, firestoreField := GetFieldByJson(modelType, jsonName)
	iter := collection.Where(firestoreField, "in", values).Documents(ctx)
	var result []interface{}
	var failure []error
	var keySuccess []string
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			failure = append(failure, err)
			break
		}
		data := reflect.New(modelType).Interface()
		err = doc.DataTo(&data)
		if err != nil {
			failure = append(failure, err)
			break
		}
		BindCommonFields(data, doc, idx, createdTimeIndex, updatedTimeIndex)
		keySuccess = append(keySuccess, doc.Ref.ID)
		result = append(result, data)
	}
	// keyFailure := difference(keySuccess, values)
	return result, failure
}

func FindAndDecode(ctx context.Context, collection *firestore.CollectionRef, ids []string, result interface{}, jsonField string) ([]string, []string, []error) {
	return FindAndDecodeWithTracking(ctx, collection, ids, result, jsonField, -1, -1)
}

func FindAndDecodeWithTracking(ctx context.Context, collection *firestore.CollectionRef, ids []string, result interface{}, jsonField string, createdTimeIndex int, updatedTimeIndex int) ([]string, []string, []error) {
	var failure []error
	var keySuccess []string
	var keyFailure []string
	if reflect.TypeOf(result).Kind() != reflect.Slice {
		failure = append(failure, errors.New("result must be a slice"))
		return keySuccess, keyFailure, failure
	}
	modelType := reflect.TypeOf(result).Elem()
	idx, _, firestoreField := GetFieldByJson(modelType, jsonField)
	iter := collection.Where(firestoreField, "in", ids).Documents(ctx)
	data := reflect.New(modelType).Interface()

	var sliceData []interface{}
	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			failure = append(failure, err)
			break
		}
		doc.DataTo(&data)
		if err != nil {
			failure = append(failure, err)
			keyFailure = append(keyFailure, doc.Ref.ID)
			break
		}
		BindCommonFields(data, doc, idx, createdTimeIndex, updatedTimeIndex)
		keySuccess = append(keySuccess, doc.Ref.ID)
		sliceData = append(sliceData, data)
	}
	valueResult := reflect.ValueOf(result)
	valueData := reflect.ValueOf(sliceData)
	reflect.Copy(valueResult, valueData)
	result = valueResult.Interface()
	// keyFailure := difference(keySuccess, ids)
	return keySuccess, keyFailure, failure
}

func difference(slice1 []string, slice2 []string) []string {
	var diff []string

	// Loop two times, first to find slice1 strings not in slice2,
	// second loop to find slice2 strings not in slice1
	for i := 0; i < 2; i++ {
		for _, s1 := range slice1 {
			found := false
			for _, s2 := range slice2 {
				if s1 == s2 {
					found = true
					break
				}
			}
			// String not found. We add it to return slice
			if !found {
				diff = append(diff, s1)
			}
		}
		// Swap the slices, only if it was the first loop
		if i == 0 {
			slice1, slice2 = slice2, slice1
		}
	}

	return diff
}

func FindFieldByName(modelType reflect.Type, fieldName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		if field.Name == fieldName {
			name1 := fieldName
			name2 := fieldName
			tag1, ok1 := field.Tag.Lookup("json")
			tag2, ok2 := field.Tag.Lookup("firestore")
			if ok1 {
				name1 = strings.Split(tag1, ",")[0]
			}
			if ok2 {
				name2 = strings.Split(tag2, ",")[0]
			}
			return i, name1, name2
		}
	}
	return -1, fieldName, fieldName
}

func FindField(modelType reflect.Type, firestoreName string) (int, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		if field.Name == firestoreName {
			firestoreTag := field.Tag.Get("firestore")
			tags := strings.Split(firestoreTag, ",")
			for _, tag := range tags {
				if strings.Compare(strings.TrimSpace(tag), firestoreName) == 0 {
					return i, field.Name
				}
			}
		}
	}
	return -1, ""
}

func GetFieldByJson(modelType reflect.Type, jsonName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		tag1, ok1 := field.Tag.Lookup("json")
		if ok1 && strings.Split(tag1, ",")[0] == jsonName {
			if tag2, ok2 := field.Tag.Lookup("firestore"); ok2 {
				return i, field.Name, strings.Split(tag2, ",")[0]
			}
			return i, field.Name, ""
		}
	}
	return -1, jsonName, jsonName
}

func GetFirestoreName(modelType reflect.Type, fieldName string) string {
	field, _ := modelType.FieldByName(fieldName)
	bsonTag := field.Tag.Get("firestore")
	tags := strings.Split(bsonTag, ",")
	if len(tags) > 0 {
		return tags[0]
	}
	return fieldName
}

func findId(queries []Query) string {
	for _, p := range queries {
		if p.Key == "_id" || p.Key == "" {
			return p.Value.(string)
		}
	}
	return ""
}

func MapFieldId(value interface{}, fieldNameId string, doc *firestore.DocumentSnapshot) {
	// fmt.Println(reflect.TypeOf(value))
	rv := reflect.Indirect(reflect.ValueOf(value))
	fv := rv.FieldByName(fieldNameId)
	if fv.IsValid() && fv.CanAddr() { //TODO handle set , now error no set id
		fv.Set(reflect.ValueOf(doc.Ref.ID))
	}
}

// for get all and search
func appendToArray(arr interface{}, item interface{}) interface{} {
	arrValue := reflect.ValueOf(arr)
	elemValue := arrValue.Elem()

	itemValue := reflect.ValueOf(item)
	if itemValue.Kind() == reflect.Ptr {
		itemValue = reflect.Indirect(itemValue)
	}
	elemValue.Set(reflect.Append(elemValue, itemValue))
	return arr
}

// Update
func BuildQueryByIdFromObject(object interface{}, modelType reflect.Type, idIndex int) (query []Query) {
	value := reflect.Indirect(reflect.ValueOf(object)).Field(idIndex).Interface()
	return BuildQueryById(value)
}

func BuildQueryById(id interface{}) (query []Query) {
	query = []Query{{Key: "_id", Operator: "==", Value: id.(string)}}
	return query
}

func DeleteOne(ctx context.Context, collection *firestore.CollectionRef, docID string) (int64, error) {
	_, err := collection.Doc(docID).Delete(ctx, firestore.Exists)
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") {
			return 0, nil
		}
		return 0, err
	}
	return 1, err
}

func Insert(ctx context.Context, collection *firestore.CollectionRef, idIndex int, model interface{}) error {
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	idField := modelValue.Field(idIndex)
	if reflect.Indirect(idField).Kind() != reflect.String {
		return fmt.Errorf("the ID field must be string")
	}
	var doc *firestore.DocumentRef
	// TODO apply idField.IsZero() for golang 13 or above
	if idField.Len() == 0 {
		doc = collection.NewDoc()
		idField.Set(reflect.ValueOf(doc.ID))
	} else {
		doc = collection.Doc(idField.String())
	}
	_, err := doc.Create(ctx, model)
	return err
}
func InsertOne(ctx context.Context, collection *firestore.CollectionRef, id string, model interface{}) (int64, error) {
	var doc *firestore.DocumentRef
	// TODO apply idField.IsZero() for golang 13 or above
	if len(id) > 0 {
		doc = collection.NewDoc()
	} else {
		doc = collection.Doc(id)
	}
	_, err := doc.Create(ctx, model)
	if err != nil {
		if strings.Index(err.Error(), "Document already exists") >= 0 {
			return 0, nil
		} else {
			return 0, err
		}
	}
	return 1, nil
}

func InsertOneWithVersion(ctx context.Context, collection *firestore.CollectionRef, id string, model interface{}, versionIndex int) (int64, error) {
	var defaultVersion interface{}
	modelType := reflect.TypeOf(model).Elem()
	versionType := modelType.Field(versionIndex).Type
	switch versionType.String() {
	case "int":
		defaultVersion = int(1)
	case "int32":
		defaultVersion = int32(1)
	case "int64":
		defaultVersion = int64(1)
	default:
		panic("not support type's version")
	}
	model, err := setValueWithIndex(model, versionIndex, defaultVersion)
	if err != nil {
		return 0, err
	}
	return InsertOne(ctx, collection, id, model)
}

func UpdateOne(ctx context.Context, collection *firestore.CollectionRef, id string, model interface{}) (int64, error) {
	if len(id) == 0 {
		return 0, fmt.Errorf("cannot update one an object that do not have id field")
	}
	_, err := collection.Doc(id).Set(ctx, model)
	if err != nil {
		return 0, err
	}
	return 1, nil
}

func UpdateOneWithVersion(ctx context.Context, collection *firestore.CollectionRef, model interface{}, versionIndex int, versionFieldName string, idIndex int) (int64, error) {
	id := getIdValueFromModel(model, idIndex)
	if len(id) == 0 {
		return 0, fmt.Errorf("cannot update one an Object that do not have id field")
	}
	itemExist, oldModel, err := FindOneMap(ctx, collection, id)
	if err != nil {
		return 0, err
	}
	if itemExist {
		currentVersion := getFieldValueAtIndex(model, versionIndex)
		oldVersion := oldModel[versionFieldName]
		switch reflect.TypeOf(currentVersion).String() {
		case "int":
			oldVersion = int(oldVersion.(int64))
		case "int32":
			oldVersion = int32(oldVersion.(int64))
		}
		if currentVersion == oldVersion {
			updateModelVersion(model, versionIndex)
			_, err := collection.Doc(id).Set(ctx, model)
			if err != nil {
				return 0, err
			} else {
				return 1, nil
			}
		} else {
			return -1, fmt.Errorf("wrong version")
		}
	} else {
		return 0, fmt.Errorf("not found")
	}
}

func PatchOneWithVersion(ctx context.Context, collection *firestore.CollectionRef, id string, json map[string]interface{}, maps map[string]string, jsonVersion string) (int64, error) {
	/*
	id := getIdValueFromMap(json)
	if len(id) == 0 {
		return 0, fmt.Errorf("cannot update one an Object that do not have id field")
	}
	 */
	itemExist, doc, err := FindOneDoc(ctx, collection, id)
	if err != nil {
		return 0, err
	}
	if itemExist {
		fs := MapToFirestore(json, doc, maps)
		currentVersion := json[jsonVersion]
		firestoreVersion, ok := maps[jsonVersion]
		if !ok {
			return -1, fmt.Errorf("cannot map version between json and firestore")
		}
		oldVersion := fs[firestoreVersion]
		switch currentVersion.(type) {
		case int:
			oldVersion = int(oldVersion.(int64))
		case int32:
			oldVersion = int32(oldVersion.(int64))
		}
		if currentVersion == oldVersion {
			updateMapVersion(fs, firestoreVersion)
			_, err := collection.Doc(id).Set(ctx, fs)
			if err != nil {
				return 0, err
			} else {
				return 1, nil
			}
		} else {
			return -1, fmt.Errorf("wrong version")
		}
	} else {
		return 0, fmt.Errorf("not found")
	}
}
func SaveOne(ctx context.Context, collection *firestore.CollectionRef, idIndex int, model interface{}) (int64, error) {
	id := getIdValueFromModel(model, idIndex)
	oldModel := reflect.New(reflect.TypeOf(model))
	itemExist, err := FindOneAndDecode(ctx, collection, id, &oldModel)
	if err != nil {
		if errNotFound := strings.Contains(err.Error(), "not found"); !errNotFound {
			return 0, err
		}
	}
	if itemExist {
		return UpdateOne(ctx, collection, id, model)
	} else {
		return InsertOne(ctx, collection, id, model)
	}
}

func SaveOneWithVersion(ctx context.Context, collection *firestore.CollectionRef, model interface{}, versionIndex int, versionFieldName string, idIndex int) (int64, error) {
	id := getIdValueFromModel(model, idIndex)
	itemExist, oldModel, err := FindOneMap(ctx, collection, id)
	if err != nil {
		if errNotFound := strings.Contains(err.Error(), "not found"); !errNotFound {
			return 0, err
		}
	}
	if itemExist {
		currentVersion := getFieldValueAtIndex(model, versionIndex)
		oldVersion := oldModel[versionFieldName]
		switch reflect.TypeOf(currentVersion).String() {
		case "int":
			oldVersion = int(oldVersion.(int64))
		case "int32":
			oldVersion = int32(oldVersion.(int64))
		}
		if currentVersion == oldVersion {
			updateModelVersion(model, versionIndex)
			_, err := collection.Doc(id).Set(ctx, model)
			if err != nil {
				return 0, err
			} else {
				return 1, nil
			}
		} else {
			return -1, fmt.Errorf("wrong version")
		}
	} else {
		return InsertOneWithVersion(ctx, collection, id, model, versionIndex)
	}
}

func getIdValueFromModel(model interface{}, idIndex int) string {
	if id, exist := getFieldValueAtIndex(model, idIndex).(string); exist {
		return id
	}
	return ""
}

func getIdValueFromMap(m map[string]interface{}) string {
	if id, exist := m["id"].(string); exist {
		return id
	}
	return ""
}

func updateModelVersion(model interface{}, versionIndex int) {
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	currentVersion := getFieldValueAtIndex(model, versionIndex)

	switch reflect.ValueOf(currentVersion).Kind().String() {
	case "int":
		nextVersion := reflect.ValueOf(currentVersion.(int) + 1)
		modelValue.Field(versionIndex).Set(nextVersion)
	case "int32":
		nextVersion := reflect.ValueOf(currentVersion.(int32) + 1)
		modelValue.Field(versionIndex).Set(nextVersion)
	case "int64":
		nextVersion := reflect.ValueOf(currentVersion.(int64) + 1)
		modelValue.Field(versionIndex).Set(nextVersion)
	default:
		panic("version's type not supported")
	}
}

func updateMapVersion(m map[string]interface{}, version string) {
	if currentVersion, exist := m[version]; exist {
		switch currentVersion.(type) {
		case int:
			m[version] = currentVersion.(int) + 1
		case int32:
			m[version] = currentVersion.(int32) + 1
		case int64:
			m[version] = currentVersion.(int64) + 1
		default:
			panic("version's type not supported")
		}
	}
}

func getFieldValueAtIndex(model interface{}, index int) interface{} {
	modelValue := reflect.Indirect(reflect.ValueOf(model))
	return modelValue.Field(index).Interface()
}

func setValueWithIndex(model interface{}, index int, value interface{}) (interface{}, error) {
	vo := reflect.Indirect(reflect.ValueOf(model))
	numField := vo.NumField()
	if index >= 0 && index < numField {
		vo.Field(index).Set(reflect.ValueOf(value))
		return model, nil
	}
	return nil, fmt.Errorf("error no found field index: %v", index)
}

func PatchOne(ctx context.Context, collection *firestore.CollectionRef, id string, json map[string]interface{}, maps map[string]string) (int64, error) {
	if len(id) == 0 {
		return 0, fmt.Errorf("cannot patch one an Object that do not have id field")
	}
	docRef := collection.Doc(id)
	doc, er1 := docRef.Get(ctx)
	if er1 != nil{
		return -1, er1
	}
	fs := MapToFirestore(json, doc, maps)
	_, er2 := docRef.Set(ctx, fs)
	if er2 != nil {
		return 0, er2
	}
	return 1, nil
}

func FindIdField(modelType reflect.Type) (int, string, string) {
	return findBsonField(modelType, "_id")
}
func findBsonField(modelType reflect.Type, bsonName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		bsonTag := field.Tag.Get("bson")
		tags := strings.Split(bsonTag, ",")
		json := field.Name
		if tag1, ok1 := field.Tag.Lookup("json"); ok1 {
			json = strings.Split(tag1, ",")[0]
		}
		for _, tag := range tags {
			if strings.TrimSpace(tag) == bsonName {
				return i, field.Name, json
			}
		}
	}
	return -1, "", ""
}
func FindFieldName(modelType reflect.Type, firestoreName string) (int, string, string) {
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		bsonTag := field.Tag.Get("firestore")
		tags := strings.Split(bsonTag, ",")
		json := field.Name
		if tag1, ok1 := field.Tag.Lookup("json"); ok1 {
			json = strings.Split(tag1, ",")[0]
		}
		for _, tag := range tags {
			if strings.TrimSpace(tag) == firestoreName {
				return i, field.Name, json
			}
		}
	}
	return -1, "", ""
}

// ref : https://stackoverflow.com/questions/46725357/firestore-batch-add-is-not-a-function
func InsertMany(ctx context.Context, collection *firestore.CollectionRef, client *firestore.Client, idName string, models interface{}) (int64, error) {
	batch := client.Batch()
	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		models_ := reflect.ValueOf(models)
		for i := 0; i < models_.Len(); i++ {
			value := models_.Index(i).Interface()
			ref := collection.NewDoc()
			if idName != "" {
				id, _ := getValue(value, idName)
				if id != nil && len(id.(string)) > 0 {
					ref = collection.Doc(id.(string))
				}
			}
			batch.Create(ref, value)
		}
	}
	// Commit the batch.
	writeResult, err := batch.Commit(ctx)
	if err != nil {
		// Handle any errors in an appropriate way, such as returning them.
		log.Printf("An error has occurred: %s", err)
		return 0, err
	}
	// fmt.Println(len(writeResult))
	return int64(len(writeResult)), nil
}

func PatchMany(ctx context.Context, collection *firestore.CollectionRef, client *firestore.Client, idName string, models interface{}) (int64, error) {
	batch := client.Batch()
	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		models_ := reflect.ValueOf(models)
		for i := 0; i < models_.Len(); i++ {
			value := models_.Index(i).Interface()
			id, errId := getValue(value, idName)
			if errId != nil {
				ref := collection.Doc(id.(string))
				batch.Set(ref, value)
			}
		}
	}
	// Commit the batch.
	writeResult, err := batch.Commit(ctx)
	if err != nil {
		// Handle any errors in an appropriate way, such as returning them.
		log.Printf("An error has occurred: %s", err)
		return 0, err
	}
	// fmt.Println(writeResult)
	return int64(len(writeResult)), nil
}

func SaveMany(ctx context.Context, collection *firestore.CollectionRef, client *firestore.Client, idName string, models interface{}) (int64, error) {
	batch := client.Batch()
	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		models_ := reflect.ValueOf(models)
		for i := 0; i < models_.Len(); i++ {
			value := models_.Index(i).Interface()
			id, errId := getValue(value, idName)
			ref := collection.NewDoc()
			if errId == nil && len(id.(string)) > 0 {
				ref = collection.Doc(id.(string))
				data, _ := ref.Get(ctx)
				if data != nil || data.Exists() {
					batch.Set(ref, value)
					continue
				}
			}
			// fmt.Println("insert id: ", id.(string))
			batch.Create(ref, value)
		}
	}
	// Commit the batch.
	writeResult, err := batch.Commit(ctx)
	if err != nil {
		// Handle any errors in an appropriate way, such as returning them.
		log.Printf("An error has occurred: %s", err)
		return 0, err
	}
	// fmt.Println(writeResult)
	return int64(len(writeResult)), nil
}

func UpdateMany(ctx context.Context, collection *firestore.CollectionRef, client *firestore.Client, idName string, models interface{}) (int64, error) {
	batch := client.Batch()
	size := 0
	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		models_ := reflect.ValueOf(models)
		for i := 0; i < models_.Len(); i++ {
			value := models_.Index(i).Interface()
			id, errId := getValue(value, idName)
			if errId == nil {
				ref := collection.Doc(id.(string))
				data, _ := ref.Get(ctx)
				// fmt.Println(data.Exists())
				if data == nil || data.Exists() {
					// fmt.Println("ID", id.(string))
					batch.Update(ref, BuildObjectToDotNotationUpdate(value))
					size++
				}
			}
		}
	}
	if size == 0 {
		return 0, nil
	}
	// Commit the batch.
	writeResult, err := batch.Commit(ctx)
	if err != nil {
		// Handle any errors in an appropriate way, such as returning them.
		log.Printf("An error has occurred: %s", err)
		return 0, err
	}
	return int64(len(writeResult)), nil
}

func initArrayResults(modelsType reflect.Type) interface{} {
	return reflect.New(modelsType).Interface()
}

func MapToFirestoreObjects(model interface{}, idName string, modelType reflect.Type) interface{} {
	var results = initArrayResults(modelType)
	switch reflect.TypeOf(model).Kind() {
	case reflect.Slice:
		values := reflect.ValueOf(model)
		for i := 0; i < values.Len(); i++ {
			// fmt.Println(values.Index(i))
			model := MapToFirestoreObject(values.Index(i).Interface(), idName)
			results = appendToArray(results, model)
		}
	}
	return results
}

func MapToFirestoreObject(model interface{}, idName string) interface{} {
	id, _ := getValue(model, idName)
	setValue(model, idName, id)
	return model
}

func getValue(model interface{}, fieldName string) (interface{}, error) {
	vo := reflect.Indirect(reflect.ValueOf(model))
	numField := vo.NumField()
	for i := 0; i < numField; i++ {
		if fieldName == vo.Type().Field(i).Name {
			return reflect.Indirect(vo).FieldByName(fieldName).Interface(), nil
		}
	}
	return nil, fmt.Errorf("Error no found field: " + fieldName)
}

func setValue(model interface{}, fieldName string, value interface{}) (interface{}, error) {
	vo := reflect.Indirect(reflect.ValueOf(model))
	numField := vo.NumField()
	for i := 0; i < numField; i++ {
		if fieldName == vo.Type().Field(i).Name {
			reflect.Indirect(vo).FieldByName(fieldName).Set(reflect.ValueOf(value))
			return model, nil
		}
	}
	return nil, fmt.Errorf("Error no found field: " + fieldName)
}

func MapModels(ctx context.Context, models interface{}, mp func(context.Context, interface{}) (interface{}, error)) (interface{}, error) {
	vo := reflect.Indirect(reflect.ValueOf(models))
	if vo.Kind() == reflect.Ptr {
		vo = reflect.Indirect(vo)
	}
	if vo.Kind() == reflect.Slice {
		le := vo.Len()
		for i := 0; i < le; i++ {
			x := vo.Index(i)
			k := x.Kind()
			if k == reflect.Struct {
				y := x.Addr().Interface()
				mp(ctx, y)
			} else {
				y := x.Interface()
				mp(ctx, y)
			}

		}
	}
	return models, nil
}
func SetValue(model interface{}, index int, value interface{}) (interface{}, error) {
	v := reflect.Indirect(reflect.ValueOf(model))
	if v.Kind() == reflect.Ptr {
		v = reflect.Indirect(v)
	}

	v.Field(index).Set(reflect.ValueOf(value))
	return model, nil
}


func MakeFirestoreMap(modelType reflect.Type) map[string]string {
	maps := make(map[string]string)
	numField := modelType.NumField()
	for i := 0; i < numField; i++ {
		field := modelType.Field(i)
		key1 := field.Name
		if tag0, ok0 := field.Tag.Lookup("json"); ok0 {
			if strings.Contains(tag0, ",") {
				a := strings.Split(tag0, ",")
				key1 = a[0]
			} else {
				key1 = tag0
			}
		}
		if tag, ok := field.Tag.Lookup("firestore"); ok {
			if tag != "-" {
				if strings.Contains(tag, ",") {
					a := strings.Split(tag, ",")
					if key1 == "-" {
						key1 = a[0]
					}
					maps[key1] = a[0]
				} else {
					if key1 == "-" {
						key1 = tag
					}
					maps[key1] = tag
				}
			}
		} else {
			if key1 == "-" {
				key1 = field.Name
			}
			maps[key1] = key1
		}
	}
	return maps
}
func MapToFirestore(json map[string]interface{}, doc *firestore.DocumentSnapshot, maps map[string]string) map[string]interface{} {
	fs := doc.Data()
	for k, v := range json {
		fk, ok := maps[k]
		if ok {
			fs[fk] = v
		}
	}
	return fs
}
