package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
)

type MapSM struct {
	_map map[string]string
}

func (m *MapSM) splitCommand(command []byte) ([]string, error) {
	parts := strings.Split(string(command), "|")
	if len(parts) != 3 {
		return nil, errors.New("error splitting command")
	}
	return parts, nil
}

func (m *MapSM) Apply(command []byte) error {
	fmt.Println("MapSM Applying command", string(command))
	parts, err := m.splitCommand(command)
	if err != nil {
		return err
	}
	cmd := parts[0]
	key := parts[1]
	value := parts[2]
	switch cmd {
	case "set":
		m._map[key] = value
	case "delete":
		delete(m._map, key)
	default:
		return errors.New("unknown command")
	}
	return nil
}

func (m *MapSM) Read(command []byte) ([]byte, error) {
	key := string(command)
	value, ok := m._map[key]
	if !ok {
		return nil, errors.New("key not found")
	}
	return []byte(value), nil
}

func (m *MapSM) Serialize() ([]byte, error) {
	buffer := new(bytes.Buffer)
	encoder := gob.NewEncoder(buffer)
	err := encoder.Encode(m._map)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (m *MapSM) Deserialize(data []byte) error {
	buffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&m._map)
	if err != nil {
		return err
	}
	return nil
}

func NewMapSM() *MapSM {
	return &MapSM{
		_map: make(map[string]string),
	}
}
