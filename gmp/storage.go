package gmp

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"sync"
)

type Storage struct {
	id          uint64
	backend     *leveldb.DB
	messageChan chan *Message
	putSignal   chan struct{}
	mutex       sync.Mutex
}

func NewStorage(filename string) (s *Storage, err error) {
	s = new(Storage)
	s.backend, err = leveldb.OpenFile(filename, nil)
	iter := s.backend.NewIterator(nil, nil)
	iter.Last()
	if iter.Valid() {
		s.id = byteToUint64(iter.Key())
	}
	iter.Release()
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
	var key uint64 = 0
	for {
		iter := this.backend.NewIterator(&util.Range{Start: uint64ToByte(key + 1)}, nil)
		for iter.Next() {
			key = byteToUint64(iter.Key())
			this.messageChan <- NewMessage(this, key, iter.Value())
		}
		iter.Release()
		<-this.putSignal
	}
}

func (this *Storage) GetMessageChan() chan *Message {
	return this.messageChan
}

func (this *Storage) Put(message *Message) {
	this.mutex.Lock()
	id := this.id + 1
	this.mutex.Unlock()
	this.backend.Put(uint64ToByte(id), message.Bytes(), nil)
	select {
	case this.putSignal <- struct{}{}:
	default:
	}
}

func (this *Storage) Delete(id uint64) {
	this.backend.Delete(uint64ToByte(id), nil)
}
