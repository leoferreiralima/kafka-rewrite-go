package kafka

import (
	"encoding/binary"
	"io"
	"reflect"
	"sort"
	"strconv"
	"sync"
)

type InvalidDecodeError struct {
	Type reflect.Type
}

func (e *InvalidDecodeError) Error() string {
	if e.Type == nil {
		return "kafka: Decode(nil)"
	}

	if e.Type.Kind() != reflect.Pointer {
		return "kafka: Decode(non-pointer " + e.Type.String() + ")"
	}

	return "kafka: Decode(nil " + e.Type.String() + ")"
}

type Decoder struct {
	reader io.Reader
}

type decodeFunc func(d *Decoder, v reflect.Value) error

func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{
		reader: reader,
	}
}

func (d *Decoder) Decode(data any) (err error) {
	v := reflect.ValueOf(data)
	t := reflect.TypeOf(data)

	if v.Kind() != reflect.Pointer || v.IsNil() {
		return &InvalidDecodeError{t}
	}

	v = v.Elem()

	decode := getDecoder(v)

	if err = decode(d, v); err != nil {
		return err
	}

	return nil
}

func getDecoder(v reflect.Value) decodeFunc {
	switch v.Kind() {
	case reflect.Struct:
		return structDecode
	case reflect.Int16:
		return int16Decode
	case reflect.Int32:
		return int32Decode
	default:
		return nil
	}
}

func int16Decode(d *Decoder, v reflect.Value) error {
	var value int16
	err := binary.Read(d.reader, binary.BigEndian, &value)
	if err != nil {
		return err
	}

	v.SetInt(int64(value))
	return nil
}

func int32Decode(d *Decoder, v reflect.Value) error {
	var value int32
	err := binary.Read(d.reader, binary.BigEndian, &value)
	if err != nil {
		return err
	}

	v.SetInt(int64(value))
	return nil
}

type structField struct {
	v          reflect.Value
	order      int
	decodeFunc decodeFunc
}

func structDecode(d *Decoder, v reflect.Value) (err error) {
	fields, err := cachedTypeFields(v)

	if err != nil {
		return err
	}

	for _, field := range fields {
		if err = field.decodeFunc(d, field.v); err != nil {
			return nil
		}
	}

	return nil
}

func typeFields(v reflect.Value) (fields []structField, err error) {
	t := v.Type()

	for i := range t.NumField() {
		field := t.Field(i)
		val := v.Field(i)

		if val.Kind() == reflect.Pointer {
			val = val.Elem()
		}

		tag, found := field.Tag.Lookup("kafka")

		if !found {
			continue
		}

		structField := new(structField)
		structField.v = val
		structField.decodeFunc = getDecoder(val)

		order, _ := parseTag(tag)

		if structField.order, err = strconv.Atoi(order); err != nil {
			return fields, err
		}

		fields = append(fields, *structField)
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i].order < fields[j].order
	})

	return fields, nil
}

var fieldCache sync.Map // map[reflect.Type][]structField

func cachedTypeFields(v reflect.Value) ([]structField, error) {
	t := v.Type()
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
