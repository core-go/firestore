package firestore

import (
	"cloud.google.com/go/firestore"
	"reflect"
	"strings"
)

func BuildObjectToDotNotationUpdate(data interface{}) []firestore.Update {
	var q []firestore.Update
	items, _ := Notation(data, SkipEmpty, ".")
	for i := range items {
		q = append(q, firestore.Update{Path: items[i].Key, Value: items[i].Value})
	}
	return q
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
		if p.Path == "_id" || p.Path == "" {
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
