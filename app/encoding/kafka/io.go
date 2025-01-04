package kafka

import (
	"encoding/binary"
	"io"
)

type KafkaReader struct {
	reader io.Reader
}

func NewKafkaReader(reader io.Reader) *KafkaReader {
	return &KafkaReader{reader}
}

func (kr *KafkaReader) Read(p []byte) (n int, err error) {
	return kr.reader.Read(p)
}

func (kr *KafkaReader) ReadUint8() (uint8, error) {
	var value uint8
	err := binary.Read(kr, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (kr *KafkaReader) ReadInt16() (int16, error) {
	var value int16
	err := binary.Read(kr, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (kr *KafkaReader) ReadUint32() (uint32, error) {
	var value uint32
	err := binary.Read(kr, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (kr *KafkaReader) ReadInt32() (int32, error) {
	var value int32
	err := binary.Read(kr, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (kr *KafkaReader) ReadByte() (byte, error) {
	var value byte
	err := binary.Read(kr, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}

	return value, nil
}

func (kr *KafkaReader) ReadString(lenght int16) (string, error) {
	bytes := make([]byte, lenght)

	if _, err := kr.Read(bytes); err != nil {
		return "", err
	}

	return string(bytes), nil
}

type KafkaWriter struct {
	reader io.Writer
}

func NewKafkaWriter(reader io.Writer) KafkaWriter {
	return KafkaWriter{reader}
}

func (kw *KafkaWriter) Write(p []byte) (n int, err error) {
	return kw.reader.Write(p)
}

func (kw *KafkaWriter) WriteByte(value byte) error {
	err := binary.Write(kw, binary.BigEndian, value)
	if err != nil {
		return err
	}
	return nil
}

func (kw *KafkaWriter) WriteInt16(value int16) error {
	err := binary.Write(kw, binary.BigEndian, value)
	if err != nil {
		return err
	}
	return nil
}

func (kw *KafkaWriter) WriteInt32(value int32) error {
	err := binary.Write(kw, binary.BigEndian, value)
	if err != nil {
		return err
	}
	return nil
}

func (kw *KafkaWriter) WriteUint32(value uint32) error {
	err := binary.Write(kw, binary.BigEndian, value)
	if err != nil {
		return err
	}
	return nil
}

func (kw *KafkaWriter) WriteString(value string) error {
	err := binary.Write(kw, binary.BigEndian, []byte(value))
	if err != nil {
		return err
	}
	return nil
}
