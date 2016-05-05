package gmp

import (
	"github.com/puper/go-queue/blockqueue"
	"github.com/puper/go-queue/listqueue"
)

type (
	Server struct {
		blockMessageQueue    map[string]*blockqueue.BlockQueue
		nonblockMessageQueue *blockqueue.BlockQueue
		commandQueue         *blockqueue.BlockQueue
		commandChan          chan *Command
		storage              *Storage
	}

	Command struct {
		Type string
		data interface{}
	}
)

func NewServer() *Server {
	return &Server{
		blockMessageQueue:    make(map[string]*blockqueue.BlockQueue),
		nonblockMessageQueue: NewQueue(),
		commandQueue:         NewQueue(),
		commandChan:          make(chan *Command),
	}
}

func (this *Server) Init() {
	go func() {
		for {
			command, err := this.commandQueue.Get(true, 1)
			if err != nil {
				this.commandChan <- command.(*Command)
			}
		}
	}()
}

func (this *Server) Close() {

}

func (this *Server) Start() {
	this.Init()
	messageChan := this.storage.GetMessageChan()
	for {
		select {
		case command := <-this.commandChan:
			if command.Type == "empty" {
				if this.blockMessageQueue[command.data.(string)].IsEmpty() {
					delete(this.blockMessageQueue, command.data.(string))
				} else {
					this.startBlockDispatcher(command.data.(string))
				}
			}
		case message := <-messageChan:
			if message.Key == "" {
				this.nonblockMessageQueue.Put(message, false, 0)
			} else {
				if _, ok := this.blockMessageQueue[message.Key]; ok {
					this.blockMessageQueue[message.Key].Put(message, false, 0)
				} else {
					this.blockMessageQueue[message.Key] = NewQueue()
					this.blockMessageQueue[message.Key].Put(message, false, 0)
					this.startBlockDispatcher(message.Key)
				}
			}
		}
	}
}

func (this *Server) startNonblockDispatcher() {
	for {
		message, err := this.nonblockMessageQueue.Get(true, 1)
		if err == nil {
			go message.(*Message).Execute()
		}
	}
}

func (this *Server) startBlockDispatcher(key string) {
	for {
		message, err := this.blockMessageQueue[key].Get(true, 1)
		if _, ok := err.(*blockqueue.EmptyQueueError); ok {
			this.commandQueue.Put(&Command{}, false, 0)
			break
		} else if err == nil {
			message.(*Message).Execute()
		}
	}
}

func NewQueue() *blockqueue.BlockQueue {
	return blockqueue.NewBlockQueue(listqueue.NewListQueue(), 0)
}
