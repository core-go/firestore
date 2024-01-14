package firestore

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"log"
	"reflect"
)

func BuildObjectToDotNotationUpdate(data interface{}) []firestore.Update {
	var q []firestore.Update
	items, _ := Notation(data, SkipEmpty, ".")
	for i := range items {
		q = append(q, firestore.Update{Path: items[i].Key, Value: items[i].Value})
	}
	return q
}

// ref : https://stackoverflow.com/questions/46725357/firestore-batch-add-is-not-a-function
func InsertMany(ctx context.Context, collection *firestore.CollectionRef, client *firestore.Client, idName string, models interface{}) (int64, error) {
	count := 0
	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
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
				if err := tx.Set(ref, modelToMap(value), firestore.MergeAll) ; err != nil {
					return err
				}
				count++
			}
			return nil
		})
		if err != nil {
			log.Printf("An error has occurred: %s", err)
			return -1 , err
		}
	}
	return int64(count), nil
}

func PatchMany(ctx context.Context, collection *firestore.CollectionRef, client *firestore.Client, idName string, models interface{}) (int64, error) {
	count := 0
	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
			models_ := reflect.ValueOf(models)
			for i := 0; i < models_.Len(); i++ {
				value := models_.Index(i).Interface()
				id, errId := getValue(value, idName)
				if errId != nil {
					ref := collection.Doc(id.(string))
					if err := tx.Set(ref, modelToMap(value), firestore.MergeAll) ; err != nil {
						return err
					}
					count++
				}
			}
			return nil
		})
		if err != nil {
			log.Printf("An error has occurred: %s", err)
			return 0, err
		}
	}
	return int64(count), nil
}

func SaveMany(ctx context.Context, collection *firestore.CollectionRef, client *firestore.Client, idName string, models interface{}) (int64, error) {
	count := 0
	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
			models_ := reflect.ValueOf(models)
			for i := 0; i < models_.Len(); i++ {
				value := models_.Index(i).Interface()
				id, errId := getValue(value, idName)
				ref := collection.NewDoc()
				if errId == nil && len(id.(string)) > 0 {
					ref = collection.Doc(id.(string))
					data, _ := ref.Get(ctx)
					if data != nil || data.Exists() {
						tx.Set(ref, value)
						continue
					}
				}
				// fmt.Println("insert id: ", id.(string))
				tx.Create(ref, value)
			}
			return nil
		})
		if err != nil {
			// Handle any errors in an appropriate way, such as returning them.
			log.Printf("An error has occurred: %s", err)
			return 0, err
		}
	}

	return int64(count), nil
}

func UpdateMany(ctx context.Context, collection *firestore.CollectionRef, client *firestore.Client, idName string, models interface{}) (int64, error) {
	size := 0
	switch reflect.TypeOf(models).Kind() {
	case reflect.Slice:
		err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
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
						tx.Update(ref, BuildObjectToDotNotationUpdate(value))
						size++
					}
				}
			}
			return nil
		})
		if size == 0 {
			return 0, nil
		}
		if err != nil {
			log.Printf("An error has occurred: %s", err)
			return 0, err
		}

	}
	return int64(size), nil
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

func modelToMap(input interface{}) map[string]interface{} {
	result := make(map[string]interface{})

	t := reflect.TypeOf(input)
	v := reflect.ValueOf(input)

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		fieldValue := v.Field(i).Interface()

		tagName := field.Tag.Get("firestore")
		if tagName == "" {
			tagName = field.Name
		}
		result[tagName] = fieldValue
	}
	return result
}
