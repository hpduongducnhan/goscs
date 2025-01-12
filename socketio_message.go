package goscs

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"
)

// [
//   "IY7cNX",
//   {
//     "type": 2,
//     "data": ["message", "fake!"],
//     "nsp": "/"
//   },
//   {
//     "rooms": {},
//     "except": {},
//     "flags": {}
//   }
// ]
// ​

const WsNormalEvent = 2
const WsBinaryEvent = 5

type WSMessage struct {
	Name      string
	Type      int
	Event     string
	Data      interface{}
	Rooms     []string
	Except    interface{}
	Flags     map[string]interface{}
	Namespace string
}

func (m *WSMessage) SetName(name string) {
	m.Name = name
}

func (m *WSMessage) SetType(t int) {
	m.Type = t
}

func (m *WSMessage) Join() *WSMessage {
	m.Flags["join"] = true
	return m
}

func (m *WSMessage) Volatile() *WSMessage {
	m.Flags["volatile"] = true
	return m
}

func (m *WSMessage) Broadcast() *WSMessage {
	m.Flags["broadcast"] = true
	return m
}

func (m *WSMessage) ToRoom(room string) *WSMessage {
	m.Rooms = append(m.Rooms, room)
	return m
}

func (m *WSMessage) ToNamespace(namespace string) *WSMessage {
	if namespace == "" {
		namespace = "/"
	}
	m.Namespace = namespace
	return m
}

func (m *WSMessage) Pack() ([]byte, error) {
	var pack = make([]interface{}, 3)
	var namespace string = m.Namespace
	if namespace == "" {
		namespace = "/"
	}

	pack[0] = m.Name
	pack[1] = map[string]interface{}{
		"type": m.Type,
		"data": []interface{}{m.Event, m.Data},
		"nsp":  namespace,
	}
	pack[2] = map[string]interface{}{
		"rooms": m.Rooms,
		"flags": m.Flags,
	}
	fmt.Printf("pack: %v\n", pack)
	b, err := msgpack.Marshal(pack)
	if err != nil {
		log.Error().Err(err).Msg("msgpack marshal get error")
		return nil, err
	}
	return b, nil
}

func NewWsMessage(event string, data interface{}, namespace string) *WSMessage {
	return &WSMessage{
		Event:     event,
		Data:      data,
		Namespace: namespace,
		Type:      WsNormalEvent, // EVENT type
		Flags:     make(map[string]interface{}),
	}
}
