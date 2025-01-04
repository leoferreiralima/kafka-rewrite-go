package kafka

import (
	"errors"
	"fmt"
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
	Version int
	Compact bool
	Nilable bool
	Raw     bool
}

func (e *EncoderOpts) withTagOps(tagOpts *tagOpts) *EncoderOpts {
	return &EncoderOpts{
		e.Version,
		tagOpts.compact,
		tagOpts.nilable,
		tagOpts.raw,
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
	case reflect.Bool:
		return boolEncoder
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
	case reflect.Struct:
		return structEncoder
	default:
		fmt.Println(t.Kind())
		panic("type not supported on encoder")
	}
}

var encodeFuncCache sync.Map // map[reflect.Kind][]encodeFunc

func cachedEncoder(t reflect.Type) encoderFunc {
	k := realType(t).Kind()
	if f, ok := encodeFuncCache.Load(k); ok {
		return f.(encoderFunc)
	}

	f, _ := encodeFuncCache.LoadOrStore(k, getEncoder(t))
	return f.(encoderFunc)
}

func boolEncoder(e *Encoder, _ *EncoderOpts, v *reflect.Value) (err error) {
	value := byte(0)
	if v.Bool() {
		value = 1
	}

	if err = e.writer.WriteByte(value); err != nil {
		return err
	}

	return nil
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
	if !opts.Raw {
		if err = arrayLengthEncoder(e, opts, v); err != nil {
			return nil
		}
	}

	lenght := v.Len()

	if v.Len() == 0 {
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

func arrayLengthEncoder(e *Encoder, opts *EncoderOpts, v *reflect.Value) (err error) {
	lenght := v.Len()

	switch {
	case lenght == 0 && opts.Nilable && opts.Compact:
		if err = e.writer.WriteByte(byte(0)); err != nil {
			return err
		}
	case lenght == 0 && opts.Nilable:
		if err = e.writer.WriteInt32(-1); err != nil {
			return err
		}
	case opts.Compact:
		if err = e.writer.WriteByte(byte(lenght + 1)); err != nil {
			return err
		}
	default:
		if err = e.writer.WriteInt32(int32(lenght)); err != nil {
			return err
		}
	}

	return nil
}

var ErrNonNilableStruct = errors.New("nil struct without nilable opt, should be `kafka:\"orderNumberHere,nilable\"`")

func structEncoder(e *Encoder, opts *EncoderOpts, v *reflect.Value) (err error) {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			if opts.Nilable {
				if err = e.writer.WriteByte(0xff); err != nil {
					return err
				}

				return nil
			}

			return ErrNonNilableStruct
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
		encoder := cachedEncoder(field.fieldType)

		if err = encoder(e, opts.withTagOps(field.tagOps), &fv); err != nil {
			return err
		}
	}

	return nil
}
