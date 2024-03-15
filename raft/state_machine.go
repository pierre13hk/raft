package raft

type StateMachine interface {
	Apply(command []byte) error
	Serialize() ([]byte, error)
	Deserialize([]byte) error
}

type DebugStateMachine struct {
	/* A simple state machine that logs all commands and can be used for testing */
	Log []string
}

func (d *DebugStateMachine) Apply(command []byte) error {
	d.Log = append(d.Log, string(command))
	return nil
}

func (d *DebugStateMachine) Serialize() ([]byte, error) {
	return []byte{}, nil
}

func (d *DebugStateMachine) Deserialize([]byte) error {
	return nil
}
