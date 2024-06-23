package batch

import (
	"cloud.google.com/go/firestore"
	"context"
	"reflect"
	"strings"
)

// ref : https://stackoverflow.com/questions/46725357/firestore-batch-add-is-not-a-function
func CreateMany[T any](ctx context.Context, client *firestore.Client, collection *firestore.CollectionRef, models []T, opts ...int) (int, error) {
	le := len(models)
	if le <= 0 {
		return -1, nil
	}
	var idx int
	if len(opts) > 0 && opts[0] >= 0 {
		idx = opts[0]
	} else {
		var t T
		modelType := reflect.TypeOf(t)
		if modelType.Kind() == reflect.Ptr {
			modelType = modelType.Elem()
		}
		idx = FindIdField(modelType)
	}
	i := -1
	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		for i = 0; i < le; i++ {
			value := models[i]
			id := GetValueByIndex(value, idx)
			sid := id.(string)
			ref := collection.NewDoc()
			if len(id.(string)) > 0 {
				ref = collection.Doc(sid)
				_, err := ref.Get(ctx)
				if err != nil {
					if strings.HasSuffix(err.Error(), " not found") {
						er2 := tx.Create(ref, value)
						if er2 != nil {
							return er2
						}
					}
				}
			} else {
				er2 := tx.Create(ref, value)
				if er2 != nil {
					return er2
				}
			}
		}
		return nil
	})
	if err != nil {
		return i, err
	}
	return -1, nil
}

func SaveMany[T any](ctx context.Context, client *firestore.Client, collection *firestore.CollectionRef, models []T, opts ...int) (int, error) {
	i := -1
	le := len(models)
	if le <= 0 {
		return i, nil
	}
	var idx int
	if len(opts) > 0 && opts[0] >= 0 {
		idx = opts[0]
	} else {
		var t T
		modelType := reflect.TypeOf(t)
		if modelType.Kind() == reflect.Ptr {
			modelType = modelType.Elem()
		}
		idx = FindIdField(modelType)
	}
	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		for i = 0; i < le; i++ {
			value := models[i]
			id := GetValueByIndex(value, idx)
			sid := id.(string)
			ref := collection.NewDoc()
			if len(id.(string)) > 0 {
				_, er0 := collection.Doc(sid).Set(ctx, value)
				if er0 != nil {
					return er0
				}
			} else {
				// fmt.Println("insert id: ", id.(string))
				er2 := tx.Create(ref, value)
				if er2 != nil {
					return er2
				}
			}
		}
		return nil
	})
	if err != nil {
		return i, err
	}
	return -1, nil
}

func UpdateMany[T any](ctx context.Context, client *firestore.Client, collection *firestore.CollectionRef, models []T, opts ...int) (int, error) {
	le := len(models)
	if le <= 0 {
		return -1, nil
	}
	var idx int
	if len(opts) > 0 && opts[0] >= 0 {
		idx = opts[0]
	} else {
		var t T
		modelType := reflect.TypeOf(t)
		if modelType.Kind() == reflect.Ptr {
			modelType = modelType.Elem()
		}
		idx = FindIdField(modelType)
	}
	i := -1
	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		for i = 0; i < le; i++ {
			value := models[i]
			id := GetValueByIndex(value, idx)
			sid := id.(string)
			ref := collection.NewDoc()
			if len(id.(string)) > 0 {
				ref = collection.Doc(sid)
				data, err := ref.Get(ctx)
				if err != nil {
					if !strings.HasSuffix(err.Error(), " not found") {
						er2 := tx.Set(ref, value)
						if er2 != nil {
							return er2
						}
					}
				}
				var er2 error
				if data != nil || data.Exists() {
					er2 = tx.Set(ref, value)
					if er2 != nil {
						return er2
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return i, err
	}
	return -1, nil
}

func GetValueByIndex(model interface{}, idx int) interface{} {
	v := reflect.ValueOf(model)
	if IsPointer(model) {
		vo := reflect.Indirect(v)
		return vo.Field(idx).Interface()
	} else {
		return v.Field(idx).Interface()
	}
}
func IsPointer(i interface{}) bool {
	return reflect.ValueOf(i).Type().Kind() == reflect.Ptr
}
