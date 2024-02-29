package asynq

type MessagePayload interface{}

type Binder func() interface{}

type MessageHandler func(string, MessagePayload) error

type HandlerData struct {
	Handler MessageHandler
	Binder  Binder
}

type MessageHandlerMap map[string]HandlerData
