package kafka

import (
	"io"
	"reflect"
	"sort"
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
	reader *KafkaReader
}

type DecoderOpts struct {
	Version  int
	Compact  bool // TODO: compact to be not shared between array and string
	Nullable bool
}

func (d *DecoderOpts) withTagOps(tagOpts *tagOpts) *DecoderOpts {
	return &DecoderOpts{
		d.Version,
		tagOpts.compact,
		tagOpts.nullable,
	}
}

type decoderFunc func(d *Decoder, opts *DecoderOpts, v *reflect.Value) error

func NewDecoder(reader io.Reader) *Decoder {
	return &Decoder{
		reader: NewKafkaReader(reader),
	}
}

func (d *Decoder) Decode(data any) (err error) {
	return d.DecodeWithOpts(data, new(DecoderOpts))
}

func (d *Decoder) DecodeWithOpts(data any, opts *DecoderOpts) (err error) {
	v := reflect.ValueOf(data)

	if v.Kind() != reflect.Pointer || v.IsNil() {
		return &InvalidDecodeError{reflect.TypeOf(data)}
	}

	v = v.Elem()

	decode := cachedDecoder(v.Type())

	if err = decode(d, opts, &v); err != nil {
		return err
	}

	return nil
}

func getDecoder(t reflect.Type) decoderFunc {
	switch t.Kind() {
	case reflect.Uint8:
		return byteDecoder
	case reflect.Int16:
		return int16Decoder
	case reflect.Int32:
		return int32Decoder
	case reflect.Uint32:
		return uint32Decoder
	case reflect.String:
		return stringDecoder
	case reflect.Array:
		return arrayDecoder
	case reflect.Slice:
		return sliceDecoder
	case reflect.Struct:
		return structDecoder
	default:
		return nil
	}
}

var decodeFuncCache sync.Map // map[reflect.Kind][]decodeFunc

func cachedDecoder(t reflect.Type) decoderFunc {
	k := t.Kind()
	if f, ok := decodeFuncCache.Load(k); ok {
		return f.(decoderFunc)
	}

	f, _ := decodeFuncCache.LoadOrStore(k, getDecoder(t))
	return f.(decoderFunc)
}

func byteDecoder(d *Decoder, _ *DecoderOpts, v *reflect.Value) (err error) {
	var value byte

	if value, err = d.reader.ReadByte(); err != nil {
		return err
	}

	v.SetUint(uint64(value))
	return nil
}

func int16Decoder(d *Decoder, _ *DecoderOpts, v *reflect.Value) (err error) {
	var value int16

	if value, err = d.reader.ReadInt16(); err != nil {
		return err
	}

	v.SetInt(int64(value))
	return nil
}

func int32Decoder(d *Decoder, _ *DecoderOpts, v *reflect.Value) (err error) {
	var value int32

	if value, err = d.reader.ReadInt32(); err != nil {
		return err
	}

	v.SetInt(int64(value))
	return nil
}

func uint32Decoder(d *Decoder, _ *DecoderOpts, v *reflect.Value) (err error) {
	var value uint32

	if value, err = d.reader.ReadUint32(); err != nil {
		return err
	}

	v.SetUint(uint64(value))
	return nil
}

func stringDecoder(d *Decoder, opts *DecoderOpts, v *reflect.Value) (err error) {
	var lenght int16

	if lenght, err = readStringLenght(d, opts); err != nil {
		return err
	}

	if lenght < 0 {
		return
	}

	var str string

	if str, err = d.reader.ReadString(lenght); err != nil {
		return err
	}

	v.SetString(str)
	return nil
}

func readStringLenght(d *Decoder, opts *DecoderOpts) (lenght int16, err error) {
	if opts.Compact {
		var compactLenght uint8
		if compactLenght, err = d.reader.ReadUint8(); err != nil {
			return 0, err
		}
		return int16(compactLenght) - 1, nil
	} else {
		if lenght, err = d.reader.ReadInt16(); err != nil {
			return 0, err
		}
	}

	return lenght, nil
}

type structField struct {
	fieldIdx   int
	tagOps     *tagOpts
	decodeFunc decoderFunc
}

func structDecoder(d *Decoder, opts *DecoderOpts, v *reflect.Value) (err error) {
	if opts.Nullable {
		var nullableByte byte

		if nullableByte, err = d.reader.ReadByte(); err != nil {
			return err
		}

		if nullableByte == 0xff {
			return nil
		}
	}

	fields, err := cachedTypeFields(v)

	if err != nil {
		return err
	}

	for _, field := range fields {
		if field.tagOps.minVersion > opts.Version {
			continue
		}
		fv := v.Field(field.fieldIdx)
		if err = field.decodeFunc(d, opts.withTagOps(field.tagOps), &fv); err != nil {
			return err
		}
	}

	return nil
}

func typeFields(v *reflect.Value) (fields []structField, err error) {
	t := v.Type()

	for i := range t.NumField() {
		field := t.Field(i)
		val := v.Field(i)

		if !field.IsExported() {
			continue
		}

		if val.Kind() == reflect.Pointer {
			val = val.Elem()
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
		structField.tagOps = &tagOpts
		structField.decodeFunc = getDecoder(val.Type())

		fields = append(fields, *structField)
	}

	sort.Slice(fields, func(i, j int) bool {
		return fields[i].tagOps.order < fields[j].tagOps.order
	})

	return fields, nil
}

var fieldCache sync.Map // map[reflect.Type][]structField

func cachedTypeFields(v *reflect.Value) ([]structField, error) {
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

func arrayDecoder(d *Decoder, opts *DecoderOpts, v *reflect.Value) (err error) {
	var lenght int32

	if lenght, err = readArrayLenght(d, opts); err != nil {
		return err
	}

	if lenght < 0 {
		return
	}

	elemType := v.Type().Elem()
	elemDecoder := cachedDecoder(elemType)
	for i := range int(lenght) {
		elemValue := v.Index(i)
		if err = elemDecoder(d, opts, &elemValue); err != nil {
			return err
		}
	}

	return nil
}

func sliceDecoder(d *Decoder, opts *DecoderOpts, v *reflect.Value) (err error) {
	var lenght int32

	if lenght, err = readArrayLenght(d, opts); err != nil {
		return err
	}

	if lenght < 0 {
		return
	}

	elemType := v.Type().Elem()
	elemDecoder := cachedDecoder(elemType)
	for range int(lenght) {
		elemValue := reflect.New(elemType).Elem()
		if err = elemDecoder(d, opts, &elemValue); err != nil {
			return err
		}

		v.Set(reflect.Append(*v, elemValue))
	}

	return nil
}

func readArrayLenght(d *Decoder, opts *DecoderOpts) (lenght int32, err error) {
	if opts.Compact {
		var compactLenght uint8
		if compactLenght, err = d.reader.ReadUint8(); err != nil {
			return 0, err
		}
		return int32(compactLenght) - 1, nil
	} else {
		if lenght, err = d.reader.ReadInt32(); err != nil {
			return 0, err
		}
	}

	return lenght, nil
}
