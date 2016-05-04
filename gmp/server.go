package gmp

import (
	//"github.com/go-ozzo/ozzo-log"
	"github.com/puper/go-queue/blockqueue"
	"github.com/puper/go-queue/listqueue"
)

type (
	Server struct {
		blockMessageQueue    map[string]*blockqueue.BlockQueue
		nonblockMessageQueue *blockqueue.BlockQueue
		messageChan          chan *Message
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
		messageChan:          make(chan *Message),
		commandQueue:         NewQueue(),
		commandChan:          make(chan *Command),
	}
}

func (this *Server) Init() {
	go func() {
		messageChan := this.storage.GetMessageChan()
		for {
			select {
			case message := <-messageChan:
				this.messageChan <- message
			}
			default:
		}
	}()
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
	for {
		select {
		case _ = <-this.commandChan:
		case message := <-this.messageChan:
			if message.Key == "" {
				this.nonblockMessageQueue.Put(message, false, 0)
			} else {
				if _, ok := this.blockMessageQueue[message.Key]; ok {
					this.blockMessageQueue[message.Key].Put(message, false, 0)
				} else {
					this.blockMessageQueue[message.Key] = NewQueue()
				}
			}
		}
	}
}

func (this *Server) StartMessageLoader() {

}

func NewQueue() *blockqueue.BlockQueue {
	return blockqueue.NewBlockQueue(listqueue.NewListQueue(), 0)
}
