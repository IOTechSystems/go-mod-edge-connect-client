// Copyright (C) 2023 IOTech Ltd

package xrt

import "sync"

// RequestMap maintains the mapping between requestId and
// command response channel of []byte type
type RequestMap interface {
	Add(id string)
	Get(id string) (chan []byte, bool)
	Delete(id string)
}

type requestMap struct {
	responseChanMap map[string]chan []byte
	mutex           sync.RWMutex
}

func NewRequestMap() RequestMap {
	return &requestMap{
		responseChanMap: make(map[string]chan []byte),
	}
}

func (m *requestMap) Add(id string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.responseChanMap[id] = make(chan []byte)
}

func (m *requestMap) Get(id string) (chan []byte, bool) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	cmdResponse, ok := m.responseChanMap[id]
	return cmdResponse, ok
}

func (m *requestMap) Delete(id string) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	delete(m.responseChanMap, id)
}
