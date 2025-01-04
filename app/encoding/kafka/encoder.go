package kafka

import (
	"io"
	"reflect"
	"sync"
)

type InvalidEncodeError struct {
	Type reflect.Type
}

func (e *InvalidEncodeError) Error() string {
	if e.Type == nil {
		return "kafka: Encode(nil)"
	}

	return "kafka: Encode(nil " + e.Type.String() + ")"
}

type Encoder struct {
	writer KafkaWriter
}

type EncoderOpts struct {
	Version      int
	Compact      bool
	Nilable      bool
	ArrayCompact bool
	ArrayNilable bool
}

func (e *EncoderOpts) withTagOps(tagOpts *tagOpts) *EncoderOpts {
	return &EncoderOpts{
		e.Version,
		tagOpts.compact,
		tagOpts.nilable,
		e.ArrayCompact,
		e.ArrayNilable,
	}
}

type encoderFunc func(e *Encoder, opts *EncoderOpts, v *reflect.Value) error

func NewEncoder(writer io.Writer) *Encoder {
	return &Encoder{
		writer: NewKafkaWriter(writer),
	}
}

func (e *Encoder) Encode(data any) (err error) {
	return e.EncodeWithOpts(data, new(EncoderOpts))
}

func (e *Encoder) EncodeWithOpts(data any, opts *EncoderOpts) (err error) {
	v := reflect.ValueOf(data)

	if data == nil {
		return &InvalidEncodeError{reflect.TypeOf(data)}
	}

	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return &InvalidEncodeError{reflect.TypeOf(data)}
		}
		v = v.Elem()
	}

	encoder := cachedEncoder(v.Type())

	if err = encoder(e, opts, &v); err != nil {
		return err
	}

	return nil
}

func getEncoder(t reflect.Type) encoderFunc {
	switch t.Kind() {
	case reflect.Uint8:
		return byteEncoder
	case reflect.Int16:
		return int16Encoder
	case reflect.Int32:
		return int32Encoder
	case reflect.Uint32:
		return uint32Encoder
	case reflect.String:
		return stringEncoder
	case reflect.Array, reflect.Slice:
		return arrayEncoder
	// case reflect.Slice:
	// 	return sliceEncoder
	// case reflect.Struct:
	// 	return structEncoder
	default:
		return nil
	}
}

var encodeFuncCache sync.Map // map[reflect.Kind][]encodeFunc

func cachedEncoder(t reflect.Type) encoderFunc {
	k := t.Kind()
	if f, ok := encodeFuncCache.Load(k); ok {
		return f.(encoderFunc)
	}

	f, _ := encodeFuncCache.LoadOrStore(k, getEncoder(t))
	return f.(encoderFunc)
}

func byteEncoder(e *Encoder, _ *EncoderOpts, v *reflect.Value) (err error) {
	value := byte(v.Uint())

	if err = e.writer.WriteByte(value); err != nil {
		return err
	}

	return nil
}

func int16Encoder(e *Encoder, _ *EncoderOpts, v *reflect.Value) (err error) {
	value := int16(v.Int())

	if err = e.writer.WriteInt16(value); err != nil {
		return err
	}

	return nil
}

func int32Encoder(e *Encoder, _ *EncoderOpts, v *reflect.Value) (err error) {
	value := int32(v.Int())

	if err = e.writer.WriteInt32(value); err != nil {
		return err
	}

	return nil
}

func uint32Encoder(e *Encoder, _ *EncoderOpts, v *reflect.Value) (err error) {
	value := uint32(v.Uint())

	if err = e.writer.WriteUint32(value); err != nil {
		return err
	}

	return nil
}

func stringEncoder(e *Encoder, opts *EncoderOpts, v *reflect.Value) (err error) {
	str := v.String()
	lenght := int16(len(str))

	switch {
	case lenght == 0 && opts.Nilable && opts.Compact:
		if err = e.writer.WriteByte(byte(0)); err != nil {
			return err
		}
	case lenght == 0 && opts.Nilable:
		if err = e.writer.WriteInt16(-1); err != nil {
			return err
		}
	case opts.Compact:
		if err = e.writer.WriteByte(byte(lenght + 1)); err != nil {
			return err
		}
	default:
		if err = e.writer.WriteInt16(lenght); err != nil {
			return err
		}
	}

	if lenght == 0 {
		return nil
	}

	if err = e.writer.WriteString(str); err != nil {
		return err
	}

	return nil
}

func arrayEncoder(e *Encoder, opts *EncoderOpts, v *reflect.Value) (err error) {
	lenght := v.Len()

	switch {
	case lenght == 0 && opts.ArrayNilable && opts.ArrayCompact:
		if err = e.writer.WriteByte(byte(0)); err != nil {
			return err
		}
	case lenght == 0 && opts.ArrayNilable:
		if err = e.writer.WriteInt32(-1); err != nil {
			return err
		}
	case opts.ArrayCompact:
		if err = e.writer.WriteByte(byte(lenght + 1)); err != nil {
			return err
		}
	default:
		if err = e.writer.WriteInt32(int32(lenght)); err != nil {
			return err
		}
	}

	if lenght == 0 {
		return nil
	}

	for i := range lenght {
		elem := v.Index(i)

		elemEncoder := cachedEncoder(elem.Type())

		if err = elemEncoder(e, opts, &elem); err != nil {
			return err
		}
	}

	return nil
}

// func sliceEncoder(e *Encoder, opts *EncoderOpts, v *reflect.Value) (err error) {
// 	lenght := v.Len()

// 	switch {
// 	case lenght == 0 && opts.ArrayNilable && opts.ArrayCompact:
// 		if err = e.writer.WriteByte(byte(0)); err != nil {
// 			return err
// 		}
// 	case lenght == 0 && opts.ArrayNilable:
// 		if err = e.writer.WriteInt32(-1); err != nil {
// 			return err
// 		}
// 	case opts.ArrayCompact:
// 		if err = e.writer.WriteByte(byte(lenght + 1)); err != nil {
// 			return err
// 		}
// 	default:
// 		if err = e.writer.WriteInt32(int32(lenght)); err != nil {
// 			return err
// 		}
// 	}

// 	if lenght == 0 {
// 		return nil
// 	}

// 	elemType := v.Type().Elem()
// 	for range lenght {
// 		elemValue := reflect.New(elemType).Elem()

// 		elemEncoder := cachedEncoder(elem.Type())

// 		if err = elemEncoder(e, opts, &elem); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// type structField struct {
// 	fieldIdx   int
// 	tagOps     *tagOpts
// 	encodeFunc encoderFunc
// }

// func structEncoder(e *Encoder, opts *EncoderOpts, v *reflect.Value) (err error) {
// 	if opts.Nilable {
// 		var nilableByte byte

// 		if nilableByte, err = e.writer.WriteByte(value); err != nil {
// 			return err
// 		}

// 		if nilableByte == 0xff {
// 			return nil
// 		}
// 	}

// 	fields, err := cachedTypeFields(v)

// 	if err != nil {
// 		return err
// 	}

// 	for _, field := range fields {
// 		if field.tagOps.minVersion > opts.Version {
// 			continue
// 		}
// 		fv := v.Field(field.fieldIdx)
// 		if err = field.encodeFunc(d, opts.withTagOps(field.tagOps), &fv); err != nil {
// 			return err
// 		}
// 	}

// 	return nil
// }

// func typeFields(v *reflect.Value) (fields []structField, err error) {
// 	t := v.Type()

// 	for i := range t.NumField() {
// 		field := t.Field(i)
// 		val := v.Field(i)

// 		if !field.IsExported() {
// 			continue
// 		}

// 		if val.Kind() == reflect.Pointer {
// 			val = val.Elem()
// 		}

// 		tag, found := field.Tag.Lookup("kafka")

// 		if !found {
// 			continue
// 		}

// 		var tagOpts tagOpts

// 		if tagOpts, err = parseTag(tag); err != nil {
// 			return fields, err
// 		}

// 		structField := new(structField)
// 		structField.fieldIdx = i
// 		structField.tagOps = &tagOpts
// 		structField.encodeFunc = getEncoder(val.Type())

// 		fields = append(fields, *structField)
// 	}

// 	sort.Slice(fields, func(i, j int) bool {
// 		return fields[i].tagOps.order < fields[j].tagOps.order
// 	})

// 	return fields, nil
// }

// var fieldCache sync.Map // map[reflect.Type][]structField

// func cachedTypeFields(v *reflect.Value) ([]structField, error) {
// 	t := v.Type()
// 	if f, ok := fieldCache.Load(t); ok {
// 		return f.([]structField), nil
// 	}
// 	fields, err := typeFields(v)
// 	if err != nil {
// 		return nil, err
// 	}
// 	f, _ := fieldCache.LoadOrStore(t, fields)
// 	return f.([]structField), nil
// }
