package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"firebase.google.com/go"
	"fmt"
	"google.golang.org/api/option"
	"log"
	"reflect"
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
