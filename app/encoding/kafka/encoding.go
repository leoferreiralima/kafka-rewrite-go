package kafka

import (
	"reflect"
	"sort"
	"sync"
)

type structField struct {
	fieldIdx  int
	fieldType reflect.Type
	tagOps    *tagOpts
}

func typeFields(v *reflect.Value) (fields []structField, err error) {
	t := realType(v.Type())

	for i := range t.NumField() {
		field := t.Field(i)

		if !field.IsExported() {
			continue
		}

		tag, found := field.Tag.Lookup("kafka")

		if !found {
			continue
		}

		var tagOpts tagOpts

		if tagOpts, err = parseTag(tag); err != nil {
			return fields, err
		}

		structField := new(structField)
		structField.fieldIdx = i
		structField.fieldType = realType(field.Type)
		structField.tagOps = &tagOpts

		fields = append(fields, *structField)
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i].tagOps.order < fields[j].tagOps.order
	})

	return fields, nil
}

var fieldCache sync.Map // map[reflect.Type][]structField

func cachedTypeFields(v *reflect.Value) ([]structField, error) {
	t := realType(v.Type())
	if f, ok := fieldCache.Load(t); ok {
		return f.([]structField), nil
	}
	fields, err := typeFields(v)
	if err != nil {
		return nil, err
	}
	f, _ := fieldCache.LoadOrStore(t, fields)
	return f.([]structField), nil
}

func realType(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Pointer {
		return t.Elem()
	}
	return t
}
