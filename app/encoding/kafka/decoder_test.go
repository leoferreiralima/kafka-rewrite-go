package kafka_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/encoding/kafka"
)

func TestInvalidDecode(t *testing.T) {
	type p struct{}

	reader := bytes.NewReader(make([]byte, 0))
	decoder := kafka.NewDecoder(reader)

	var err error

	if err = decoder.Decode(nil); err == nil {
		t.Error("nil value cannot be decoded")
	}

	if err = decoder.Decode(p{}); err == nil {
		t.Error("non pointer value cannot be decoded")
	}

	var v *p
	if err = decoder.Decode(v); err == nil {
		t.Error("nil value cannot be decoded")
	}
}

func TestDecodeInt32(t *testing.T) {
	expected := int32(1024)

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, expected)

	var result int32
	if err := kafka.NewDecoder(buffer).Decode(&result); err != nil {
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
		I32 int32 `kafka:"1"`
		I16 int16 `kafka:"0"`
	} `kafka:"2"`
	privateField int16 `kafka:"3"`
}

func (s *s) toString() string {
	return fmt.Sprintf(
		"s[I16=%d, I32=%d, notMapped=%d,innerStruct.I16=%d, innerStruct.I32=%d, privateField=%d]",
		s.I16,
		s.I32,
		s.NotMapped,
		s.InnerStruct.I16,
		s.InnerStruct.I32,
		s.privateField,
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

	if err := kafka.NewDecoder(buffer).Decode(&result); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if !reflect.DeepEqual(expected, result) {
		t.Fatalf("expected: %s, result: %s", expected.toString(), result.toString())
	}
}

type vs struct {
	V0 string `kafka:"0,minVersion=0"`
	V1 string `kafka:"1,minVersion=1"`
	V2 string `kafka:"2,minVersion=2"`
}

var testDecodeVersionedStructCases = []struct {
	version  int
	expected vs
}{
	{
		version:  -1,
		expected: vs{},
	},
	{
		version: 0,
		expected: vs{
			V0: "V0",
		},
	},
	{
		version: 1,
		expected: vs{
			V0: "V0",
			V1: "V1",
		},
	},
	{
		version: 2,
		expected: vs{
			V0: "V0",
			V1: "V1",
			V2: "V2",
		},
	},
}

func TestDecodeVersionedStruct(t *testing.T) {
	var newBuffer = func() *bytes.Buffer {
		buffer := new(bytes.Buffer)
		for _, str := range []string{
			"V0",
			"V1",
			"V2",
		} {
			binary.Write(buffer, binary.BigEndian, int16(len(str)))
			binary.Write(buffer, binary.BigEndian, []byte(str))
		}

		return buffer
	}

	for _, testCase := range testDecodeVersionedStructCases {
		buffer := newBuffer()

		var result vs

		if err := kafka.NewDecoder(buffer).DecodeWithOpts(&result, &kafka.DecoderOpts{
			Version: testCase.version,
		}); err != nil {
			t.Errorf("unexpecte error %s", err)
		}

		if !reflect.DeepEqual(testCase.expected, result) {
			t.Errorf("expected: %s, result: %s", fmt.Sprint(testCase.expected), fmt.Sprint(result))
		}
	}
}

func TestDecodeNullableStruct(t *testing.T) {
	type ns struct {
		Nullable struct {
			Text string `kafka: "0"`
		} `kafka: "0,nullable"`
	}

	expected := ns{}
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, byte(0xff))
	var result ns

	if err := kafka.NewDecoder(buffer).DecodeWithOpts(&result, &kafka.DecoderOpts{
		Nullable: true,
	}); err != nil {
		t.Errorf("unexpecte error %s", err)
	}

	if !reflect.DeepEqual(expected, result) {
		t.Errorf("expected: %s, result: %s", fmt.Sprint(expected), fmt.Sprint(result))
	}

}

func TestDecodeString(t *testing.T) {
	expected := "this is a string."

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int16(len(expected)))
	binary.Write(buffer, binary.BigEndian, []byte(expected))

	var result string

	if err := kafka.NewDecoder(buffer).Decode(&result); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", expected, result)
	}
}

func TestDecodeEmptyString(t *testing.T) {
	expected := ""

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int16(len(expected)))

	var result string

	if err := kafka.NewDecoder(buffer).Decode(&result); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", expected, result)
	}
}

func TestDecodeCompactString(t *testing.T) {
	expected := "this is a compact string."

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, uint8(len(expected)+1))
	binary.Write(buffer, binary.BigEndian, []byte(expected))

	var result string

	if err := kafka.NewDecoder(buffer).DecodeWithOpts(&result, &kafka.DecoderOpts{
		Compact: true,
	}); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", expected, result)
	}
}

func TestDecodeEmptyCompactString(t *testing.T) {
	expected := ""

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, uint8(len(expected)+1))

	var result string

	if err := kafka.NewDecoder(buffer).DecodeWithOpts(&result, &kafka.DecoderOpts{
		Compact: true,
	}); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", expected, result)
	}
}

func TestDecodeUint32(t *testing.T) {
	expected := uint32(1000)

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, expected)

	var result uint32

	if err := kafka.NewDecoder(buffer).DecodeWithOpts(&result, &kafka.DecoderOpts{
		Compact: true,
	}); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if expected != result {
		t.Errorf("expected: %s, result: %s", fmt.Sprint(expected), fmt.Sprint(result))
	}
}

func TestDecodeByte(t *testing.T) {
	expected := byte(10)

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, expected)

	var result byte

	if err := kafka.NewDecoder(buffer).DecodeWithOpts(&result, &kafka.DecoderOpts{
		Compact: true,
	}); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if expected != result {
		t.Errorf("expected: %s, result: %s", fmt.Sprint(expected), fmt.Sprint(result))
	}
}

func TestDecodeInt32Array(t *testing.T) {
	expected := [3]int32{0, 1, 2}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(len(expected)))

	for _, i := range expected {
		binary.Write(buffer, binary.BigEndian, i)
	}

	var result [3]int32

	if err := kafka.NewDecoder(buffer).Decode(&result); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", fmt.Sprint(expected), fmt.Sprint(result))
	}
}

func TestDecodeInt32CompactArray(t *testing.T) {
	expected := [3]int32{0, 1, 2}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, uint8(len(expected)+1))

	for _, i := range expected {
		binary.Write(buffer, binary.BigEndian, i)
	}

	var result [3]int32

	if err := kafka.NewDecoder(buffer).DecodeWithOpts(&result, &kafka.DecoderOpts{
		Compact: true,
	}); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", fmt.Sprint(expected), fmt.Sprint(result))
	}
}

func TestDecodeStringArray(t *testing.T) {
	expected := [3]string{"0", "1", "2"}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(len(expected)))

	for _, str := range expected {
		binary.Write(buffer, binary.BigEndian, int16(len(str)))
		binary.Write(buffer, binary.BigEndian, []byte(str))
	}

	var result [3]string

	if err := kafka.NewDecoder(buffer).Decode(&result); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", fmt.Sprint(expected), fmt.Sprint(result))
	}
}

func TestDecodeStuctArray(t *testing.T) {
	type sa struct {
		I32  int32     `kafka:"0"`
		Strs [2]string `kafka:"1"`
	}

	expected := [3]sa{
		{
			I32: 1,
			Strs: [2]string{
				"abc",
				"def",
			},
		},
		{
			I32: 2,
			Strs: [2]string{
				"ghi",
				"jkl",
			},
		},
		{
			I32: 3,
			Strs: [2]string{
				"mno",
				"pqr",
			},
		},
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(len(expected)))

	for _, i := range expected {
		binary.Write(buffer, binary.BigEndian, i.I32)

		binary.Write(buffer, binary.BigEndian, int32(len(i.Strs)))

		for _, str := range i.Strs {
			binary.Write(buffer, binary.BigEndian, int16(len(str)))
			binary.Write(buffer, binary.BigEndian, []byte(str))
		}
	}

	var result [3]sa

	if err := kafka.NewDecoder(buffer).Decode(&result); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", fmt.Sprint(expected), fmt.Sprint(result))
	}
}

func TestDecodeInt32Slice(t *testing.T) {
	expected := []int32{0, 1, 2}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(len(expected)))

	for _, i := range expected {
		binary.Write(buffer, binary.BigEndian, i)
	}

	var result []int32

	if err := kafka.NewDecoder(buffer).Decode(&result); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if !slices.Equal(expected, result) {
		t.Fatalf("expected: %s, result: %s", fmt.Sprint(expected), fmt.Sprint(result))
	}
}

func TestDecodeInt32CompactSlice(t *testing.T) {
	expected := []int32{0, 1, 2}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, uint8(len(expected)+1))

	for _, i := range expected {
		binary.Write(buffer, binary.BigEndian, i)
	}

	var result []int32

	if err := kafka.NewDecoder(buffer).DecodeWithOpts(&result, &kafka.DecoderOpts{
		Compact: true,
	}); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if !slices.Equal(expected, result) {
		t.Fatalf("expected: %s, result: %s", fmt.Sprint(expected), fmt.Sprint(result))
	}
}

func TestDecodeStringSlice(t *testing.T) {
	expected := []string{"0", "1", "2"}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(len(expected)))

	for _, str := range expected {
		binary.Write(buffer, binary.BigEndian, int16(len(str)))
		binary.Write(buffer, binary.BigEndian, []byte(str))
	}

	var result []string

	if err := kafka.NewDecoder(buffer).Decode(&result); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if !slices.Equal(expected, result) {
		t.Fatalf("expected: %s, result: %s", fmt.Sprint(expected), fmt.Sprint(result))
	}
}

type ss struct {
	I32  int32    `kafka:"0"`
	Strs []string `kafka:"1"`
}

func (d ss) Equals(other ss) bool {
	if d.I32 != other.I32 {
		return false
	}
	return slices.Equal(d.Strs, other.Strs)
}
func TestDecodeStuctSlice(t *testing.T) {

	expected := []ss{
		{
			I32: 1,
			Strs: []string{
				"abc",
				"def",
			},
		},
		{
			I32: 2,
			Strs: []string{
				"ghi",
				"jkl",
			},
		},
		{
			I32: 3,
			Strs: []string{
				"mno",
				"pqr",
			},
		},
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int32(len(expected)))

	for _, i := range expected {
		binary.Write(buffer, binary.BigEndian, i.I32)

		binary.Write(buffer, binary.BigEndian, int32(len(i.Strs)))

		for _, str := range i.Strs {
			binary.Write(buffer, binary.BigEndian, int16(len(str)))
			binary.Write(buffer, binary.BigEndian, []byte(str))
		}
	}

	var result []ss

	if err := kafka.NewDecoder(buffer).Decode(&result); err != nil {
		t.Fatalf("unexpecte error %s", err)
	}

	if !Equals(expected, result) {
		t.Fatalf("expected: %s, result: %s", fmt.Sprint(expected), fmt.Sprint(result))
	}
}

func Equals(thisList []ss, otherList []ss) bool {
	if len(thisList) != len(otherList) {
		return false
	}

	for i := range len(thisList) {
		if !thisList[i].Equals(otherList[i]) {
			return false
		}
	}

	return true
}
