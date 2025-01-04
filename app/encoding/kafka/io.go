package kafka

import (
	"encoding/binary"
	"io"
)

type kafkaReader struct {
	reader io.Reader
}

func newKafkaReader(reader io.Reader) kafkaReader {
	return kafkaReader{reader}
}
func (kr *kafkaReader) Read(p []byte) (n int, err error) {
	return kr.reader.Read(p)
}

func (kr *kafkaReader) ReadUint8() (uint8, error) {
	var value uint8
	err := binary.Read(kr, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (kr *kafkaReader) ReadInt16() (int16, error) {
	var value int16
	err := binary.Read(kr, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (kr *kafkaReader) ReadUint32() (uint32, error) {
	var value uint32
	err := binary.Read(kr, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (kr *kafkaReader) ReadInt32() (int32, error) {
	var value int32
	err := binary.Read(kr, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (kr *kafkaReader) ReadByte() (byte, error) {
	var tagBuffer byte
	err := binary.Read(kr, binary.BigEndian, &tagBuffer)
	if err != nil {
		return 0, err
	}

	return tagBuffer, nil
}

func (kr *kafkaReader) ReadString(lenght int16) (string, error) {
	bytes := make([]byte, lenght)

	if _, err := kr.Read(bytes); err != nil {
		return "", err
	}

	return string(bytes), nil
}
