package gmp

import (
	"github.com/syndtr/goleveldb/leveldb"
)

type Storage struct {
	backend     *leveldb.DB
	messageChan chan *Message
	putSignal   chan struct{}
}

func NewStorage(filename string) (s *Storage, err error) {
	s = new(Storage)
	s.backend, err = leveldb.OpenFile(filename, nil)
	s.messageChan = make(chan *Message)
	s.putSignal = make(chan struct{})
	if err != nil {
		return nil, err
	}
	return
}

func (this *Storage) Start() {
	go this.start()
}

func (this *Storage) start() {
	for {
		iter := this.backend.NewIterator(nil, nil)
		for iter.Next() {
			this.messageChan <- NewMessage(byteToUint64(iter.Key()), iter.Value())
		}
		iter.Release()
		<-this.putSignal
	}
}

func (this *Storage) GetMessageChan() chan *Message {
	return this.messageChan
}

func (this *Storage) Put(message *Message) {
	this.backend.Put(uint64ToByte(message.Id), message.Bytes(), nil)
	select {
	case this.putSignal <- struct{}{}:
	default:
	}
}

func (this *Storage) Delete(id uint64) {
	this.backend.Delete(uint64ToByte(id), nil)
}
