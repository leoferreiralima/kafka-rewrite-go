package utils

import (
	"encoding/binary"
	"io"
)

type ReaderFunc[E any] func(io.Reader) (E, error)

type Writer interface {
	Write(writer io.Writer) error
}

func ReadUint8(reader io.Reader) (uint8, error) {
	var value uint8
	err := binary.Read(reader, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func ReadInt8(reader io.Reader) (int8, error) {
	var value int8
	err := binary.Read(reader, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func WriteInt8(writer io.Writer, value int8) error {
	err := binary.Write(writer, binary.BigEndian, &value)
	if err != nil {
		return err
	}
	return nil
}

func ReadInt16(reader io.Reader) (int16, error) {
	var value int16
	err := binary.Read(reader, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func WriteInt16(writer io.Writer, value int16) error {
	err := binary.Write(writer, binary.BigEndian, &value)
	if err != nil {
		return err
	}
	return nil
}

func ReadUint32(reader io.Reader) (uint32, error) {
	var value uint32
	err := binary.Read(reader, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func ReadInt32(reader io.Reader) (int32, error) {
	var value int32
	err := binary.Read(reader, binary.BigEndian, &value)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func WriteInt32(writer io.Writer, value int32) error {
	err := binary.Write(writer, binary.BigEndian, &value)
	if err != nil {
		return err
	}
	return nil
}

func WriteString(writer io.Writer, value string) (err error) {
	length := int16(len(value)) + 1

	if err = WriteInt16(writer, length); err != nil {
		return err
	}

	if err = WriteByteArray(writer, []byte(value)); err != nil {
		return nil
	}

	return nil
}

func ReadString(reader io.Reader, lenght int16) (string, error) {
	bytes := make([]byte, lenght)

	if _, err := reader.Read(bytes); err != nil {
		return "", err
	}

	return string(bytes), nil
}

func ReadByte(reader io.Reader) (byte, error) {
	var tagBuffer byte
	err := binary.Read(reader, binary.BigEndian, &tagBuffer)
	if err != nil {
		return 0, err
	}

	return tagBuffer, nil
}

func WriteByte(writer io.Writer, value byte) error {
	err := binary.Write(writer, binary.BigEndian, &value)
	if err != nil {
		return err
	}
	return nil
}

func WriteByteArray(writer io.Writer, bytes []byte) (err error) {
	if _, err = writer.Write(bytes); err != nil {
		return err
	}

	return nil
}
