package raft

type StateMachine interface {
	Apply(command []byte) error
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}
