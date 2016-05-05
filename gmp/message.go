package gmp

import (
	"bytes"
	"encoding/binary"
	"log"
)

func uint64ToByte(n uint64) []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, n)
	return buf.Bytes()
}

func byteToUint64(v []byte) (n uint64) {
	buf := bytes.NewBuffer(v)
	binary.Read(buf, binary.BigEndian, &n)
	return n
}

func WriteUint64(buf *bytes.Buffer, n uint64) {
	binary.Write(buf, binary.BigEndian, n)
}

func ReadUint64(buf *bytes.Buffer) (n uint64) {
	binary.Read(buf, binary.BigEndian, &n)
	return n
}

func WriteUint8(buf *bytes.Buffer, n uint8) {
	binary.Write(buf, binary.BigEndian, n)
}

func ReadUint8(buf *bytes.Buffer) (n uint8) {
	binary.Read(buf, binary.BigEndian, &n)
	return n
}

func WriteFloat64(buf *bytes.Buffer, n float64) {
	binary.Write(buf, binary.BigEndian, n)
}

func ReadFloat64(buf *bytes.Buffer) (n float64) {
	binary.Read(buf, binary.BigEndian, &n)
	return n
}

func WriteBytes(buf *bytes.Buffer, v []byte) {
	l := uint64(len(v))
	WriteUint64(buf, l)
	buf.Write(v)
}

func ReadBytes(buf *bytes.Buffer) (v []byte) {
	l := ReadUint64(buf)
	return buf.Next(int(l))
}

func WriteString(buf *bytes.Buffer, v string) {
	WriteBytes(buf, []byte(v))
}

func ReadString(buf *bytes.Buffer) (v string) {
	return string(ReadBytes(buf))
}

type Message struct {
	Id       uint64
	Data     string
	Type     string
	Key      string
	TryCount uint8
	Timeout  float64
	Storage  *Storage
}

//do job
func (this *Message) Execute() {
	log.Println(this)
}

func NewMessage(storage *Storage, id uint64, data []byte) *Message {
	m := new(Message)
	m.Storage = storage
	buf := bytes.NewBuffer(data)
	m.Id = id
	m.Data = ReadString(buf)
	m.Type = ReadString(buf)
	m.Key = ReadString(buf)
	m.TryCount = ReadUint8(buf)
	m.Timeout = ReadFloat64(buf)
	return m
}

func (this *Message) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	WriteString(buf, this.Data)
	WriteString(buf, this.Type)
	WriteString(buf, this.Key)
	WriteUint8(buf, this.TryCount)
	WriteFloat64(buf, this.Timeout)
	return buf.Bytes()
}
