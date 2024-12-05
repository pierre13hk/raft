package raft

type StateMachine interface {
	// Apply a command to the state machine
	Apply(command []byte) error
	// Return a read operation result
	Read(command []byte) ([]byte, error)
	// Serialize the state machine to a byte array that can
	// be stored on disk or sent over the network
	Serialize() ([]byte, error)
	// Deserialize the state machine from a byte array
	Deserialize([]byte) error
}
