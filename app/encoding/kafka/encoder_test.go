package kafka_test

import (
	"bytes"
	"testing"

	"github.com/codecrafters-io/kafka-starter-go/app/encoding/kafka"
)

func TestInvalidEncode(t *testing.T) {

	buffer := new(bytes.Buffer)
	encoder := kafka.NewEncoder(buffer)

	var err error

	if err = encoder.Encode(nil); err == nil {
		t.Error("nil value cannot be decoded")
	}

	type p struct{}

	var v *p
	if err = encoder.Encode(v); err == nil {
		t.Error("nil value cannot be decoded")
	}
}

func TestEncodeByte(t *testing.T) {
	buffer := new(bytes.Buffer)

	expected := byte(10)

	var err error
	if err = kafka.NewEncoder(buffer).Encode(expected); err != nil {
		t.Errorf("unexpected encode error: %s", err)
	}

	var result byte

	if result, err = kafka.NewKafkaReader(buffer).ReadByte(); err != nil {
		t.Errorf("unexpected read error: %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %d, result: %d", expected, result)
	}
}

func TestEncodeInt16(t *testing.T) {
	buffer := new(bytes.Buffer)

	expected := int16(10)

	var err error
	if err = kafka.NewEncoder(buffer).Encode(expected); err != nil {
		t.Errorf("unexpected encode error: %s", err)
	}

	var result int16

	if result, err = kafka.NewKafkaReader(buffer).ReadInt16(); err != nil {
		t.Errorf("unexpected read error: %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %d, result: %d", expected, result)
	}
}

func TestEncodeInt32(t *testing.T) {
	buffer := new(bytes.Buffer)

	expected := int32(10)

	var err error
	if err = kafka.NewEncoder(buffer).Encode(expected); err != nil {
		t.Errorf("unexpected encode error: %s", err)
	}

	var result int32

	if result, err = kafka.NewKafkaReader(buffer).ReadInt32(); err != nil {
		t.Errorf("unexpected read error: %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %d, result: %d", expected, result)
	}
}

func TestEncodeUint32(t *testing.T) {
	buffer := new(bytes.Buffer)

	expected := uint32(10)

	var err error
	if err = kafka.NewEncoder(buffer).Encode(expected); err != nil {
		t.Errorf("unexpected encode error: %s", err)
	}

	var result uint32

	if result, err = kafka.NewKafkaReader(buffer).ReadUint32(); err != nil {
		t.Errorf("unexpected read error: %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %d, result: %d", expected, result)
	}
}

func TestEncodeString(t *testing.T) {
	buffer := new(bytes.Buffer)

	expected := "this is a string."
	expectedLenght := int16(len(expected))

	var err error
	if err = kafka.NewEncoder(buffer).Encode(expected); err != nil {
		t.Errorf("unexpected encode error: %s", err)
	}

	var resultLenght int16

	if resultLenght, err = kafka.NewKafkaReader(buffer).ReadInt16(); err != nil {
		t.Errorf("unexpected read length error: %s", err)
	}

	if expectedLenght != resultLenght {
		t.Fatalf("expectedLenght: %d, resultLenght: %d", expectedLenght, resultLenght)
	}

	var result string

	if result, err = kafka.NewKafkaReader(buffer).ReadString(resultLenght); err != nil {
		t.Errorf("unexpected read string error: %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", expected, result)
	}
}

func TestEncodeEmptyString(t *testing.T) {
	buffer := new(bytes.Buffer)

	expected := ""
	expectedLenght := int16(len(expected))

	var err error
	if err = kafka.NewEncoder(buffer).Encode(expected); err != nil {
		t.Errorf("unexpected encode error: %s", err)
	}

	var resultLenght int16

	if resultLenght, err = kafka.NewKafkaReader(buffer).ReadInt16(); err != nil {
		t.Errorf("unexpected read length error: %s", err)
	}

	if expectedLenght != resultLenght {
		t.Fatalf("expectedLenght: %d, resultLenght: %d", expectedLenght, resultLenght)
	}

	var result string

	if result, err = kafka.NewKafkaReader(buffer).ReadString(resultLenght); err != nil {
		t.Errorf("unexpected read string error: %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", expected, result)
	}
}

func TestEncodeNullString(t *testing.T) {
	buffer := new(bytes.Buffer)

	expected := ""
	expectedLenght := int16(-1)

	var err error
	if err = kafka.NewEncoder(buffer).EncodeWithOpts(expected, &kafka.EncoderOpts{
		Nullable: true,
	}); err != nil {
		t.Errorf("unexpected encode error: %s", err)
	}

	var resultLenght int16

	if resultLenght, err = kafka.NewKafkaReader(buffer).ReadInt16(); err != nil {
		t.Errorf("unexpected read length error: %s", err)
	}

	if expectedLenght != resultLenght {
		t.Fatalf("expectedLenght: %d, resultLenght: %d", expectedLenght, resultLenght)
	}
}

func TestEncodeCompactString(t *testing.T) {
	buffer := new(bytes.Buffer)

	expected := "this is a compact string."
	expectedLenght := byte(len(expected) + 1)

	var err error
	if err = kafka.NewEncoder(buffer).EncodeWithOpts(expected, &kafka.EncoderOpts{
		Compact: true,
	}); err != nil {
		t.Errorf("unexpected encode error: %s", err)
	}

	var resultLenght byte

	if resultLenght, err = kafka.NewKafkaReader(buffer).ReadByte(); err != nil {
		t.Errorf("unexpected read length error: %s", err)
	}

	if expectedLenght != resultLenght {
		t.Fatalf("expectedLenght: %d, resultLenght: %d", expectedLenght, resultLenght)
	}

	var result string

	if result, err = kafka.NewKafkaReader(buffer).ReadString(int16(resultLenght) - 1); err != nil {
		t.Errorf("unexpected read string error: %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", expected, result)
	}
}

func TestEncodeEmptyCompactString(t *testing.T) {
	buffer := new(bytes.Buffer)

	expected := ""
	expectedLenght := byte(len(expected) + 1)

	var err error
	if err = kafka.NewEncoder(buffer).EncodeWithOpts(expected, &kafka.EncoderOpts{
		Compact: true,
	}); err != nil {
		t.Errorf("unexpected encode error: %s", err)
	}

	var resultLenght byte

	if resultLenght, err = kafka.NewKafkaReader(buffer).ReadByte(); err != nil {
		t.Errorf("unexpected read length error: %s", err)
	}

	if expectedLenght != resultLenght {
		t.Fatalf("expectedLenght: %d, resultLenght: %d", expectedLenght, resultLenght)
	}

	var result string

	if result, err = kafka.NewKafkaReader(buffer).ReadString(int16(resultLenght) - 1); err != nil {
		t.Errorf("unexpected read string error: %s", err)
	}

	if expected != result {
		t.Fatalf("expected: %s, result: %s", expected, result)
	}
}

func TestEncodeNullCompactString(t *testing.T) {
	buffer := new(bytes.Buffer)

	expected := ""
	expectedLenght := byte(0)

	var err error
	if err = kafka.NewEncoder(buffer).EncodeWithOpts(expected, &kafka.EncoderOpts{
		Nullable: true,
		Compact:  true,
	}); err != nil {
		t.Errorf("unexpected encode error: %s", err)
	}

	var resultLenght byte

	if resultLenght, err = kafka.NewKafkaReader(buffer).ReadByte(); err != nil {
		t.Errorf("unexpected read length error: %s", err)
	}

	if expectedLenght != resultLenght {
		t.Fatalf("expectedLenght: %d, resultLenght: %d", expectedLenght, resultLenght)
	}
}
