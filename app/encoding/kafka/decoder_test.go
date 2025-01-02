package kafka

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"testing"
)

func TestInvalidDecode(t *testing.T) {
	type p struct{}

	reader := bytes.NewReader(make([]byte, 0))
	decoder := NewDecoder(reader)

	var err error

	if err = decoder.Decode(nil); err == nil {
		t.Error("nil value cannot be decoded")
	}

	t.Log(err)

	if err = decoder.Decode(p{}); err == nil {
		t.Error("non pointer value cannot be decoded")
	}

	t.Log(err)

	var v *p
	if err = decoder.Decode(v); err == nil {
		t.Error("nil value cannot be decoded")
	}

	t.Log(err)
}

func TestDecodeInt32(t *testing.T) {
	expected := int32(1024)

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, expected)

	var result int32
	if err := NewDecoder(buffer).Decode(&result); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if result != expected {
		t.Fatalf("expect %d, result %d", expected, result)
	}
}

type s struct {
	I16         int16 `kafka:"0"`
	I32         int32 `kafka:"1"`
	NotMapped   int32
	InnerStruct struct {
		I16 int16 `kafka:"0"`
		I32 int32 `kafka:"1"`
	} `kafka:"2"`
}

func (s *s) toString() string {
	return fmt.Sprintf(
		"s[I16=%d, I32=%d, notMapped=%d,innerStruct.I16=%d, innerStruct.I32=%d]",
		s.I16,
		s.I32,
		s.NotMapped,
		s.InnerStruct.I16,
		s.InnerStruct.I32,
	)
}

func TestDecodeStruct(t *testing.T) {

	expected := s{
		I16: 100,
		I32: 1000,
	}

	expected.InnerStruct.I16 = 200
	expected.InnerStruct.I32 = 2000

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, expected.I16)
	binary.Write(buffer, binary.BigEndian, expected.I32)
	binary.Write(buffer, binary.BigEndian, expected.InnerStruct.I16)
	binary.Write(buffer, binary.BigEndian, expected.InnerStruct.I32)

	var result s

	if err := NewDecoder(buffer).Decode(&result); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("expected: %s, result: %s", expected.toString(), result.toString())
	}
}
