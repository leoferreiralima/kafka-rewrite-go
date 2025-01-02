package support

import (
	"encoding/binary"
	"io"
)

type ReaderFunc[E any] func(io.Reader) (E, error)

type Writer interface {
	Write(writer io.Writer) error
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

func ReadString(reader io.Reader) (result string, err error) {
	var lenght int16
	if lenght, err = ReadInt16(reader); err != nil {
		return "", err
	}

	if result, err = readString(reader, lenght); err != nil {
		return "", err
	}

	return result, nil
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

func ReadCompactString(reader io.Reader) (result string, err error) {
	var lenght int8
	if lenght, err = ReadInt8(reader); err != nil {
		return "", err
	}

	if result, err = readString(reader, int16(lenght)-1); err != nil {
		return "", err
	}

	return result, nil
}

func WriteCompactString(writer io.Writer, value string) (err error) {
	length := int8(len(value)) + 1

	if err = WriteInt8(writer, length); err != nil {
		return err
	}

	if err = WriteByteArray(writer, []byte(value)); err != nil {
		return nil
	}

	return nil
}

func readString(reader io.Reader, lenght int16) (string, error) {
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

func WriteBool(writer io.Writer, value bool) (err error) {
	if value {
		if err = WriteByte(writer, 1); err != nil {
			return err
		}
	} else {
		if err = WriteByte(writer, 0); err != nil {
			return err
		}
	}

	return nil
}

func ReadArray[E any](reader io.Reader, readerFunc ReaderFunc[E]) (array []E, err error) {
	var lenght int32
	if lenght, err = ReadInt32(reader); err != nil {
		return array, err
	}

	if array, err = readArray(reader, readerFunc, lenght); err != nil {
		return array, err
	}

	return array, nil
}

func WriteArray(writer io.Writer, items []Writer) (err error) {
	lenght := int32(len(items)) + 1

	if err = WriteInt32(writer, lenght); err != nil {
		return err
	}

	for _, item := range items {
		if err = item.Write(writer); err != nil {
			return err
		}
	}

	return nil
}

func ReadCompactArray[E any](reader io.Reader, readerFunc ReaderFunc[E]) (array []E, err error) {
	var lenght int8
	if lenght, err = ReadInt8(reader); err != nil {
		return array, err
	}

	if array, err = readArray(reader, readerFunc, int32(lenght)-1); err != nil {
		return array, err
	}

	return array, nil
}

func WriteCompactArray[W Writer](writer io.Writer, items []W) (err error) {
	lenght := int8(len(items)) + 1

	if err = WriteInt8(writer, lenght); err != nil {
		return err
	}

	for _, item := range items {
		if err = item.Write(writer); err != nil {
			return err
		}
	}

	return nil
}

func readArray[E any](reader io.Reader, readerFunc ReaderFunc[E], lenght int32) (array []E, err error) {
	for i := 0; i < int(lenght); i++ {
		var item E

		if item, err = readerFunc(reader); err != nil {
			return array, err
		}

		array = append(array, item)
	}

	if _, err = ReadByte(reader); err != nil {
		return array, err
	}

	return array, err
}
