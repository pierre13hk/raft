module localtest

go 1.22.0

replace raft.com/raft => ../../raft

replace raft.com/simulate/rpc => ../../simulate/rpc

require (
	raft.com/raft v0.0.0-00010101000000-000000000000
	raft.com/simulate/rpc v0.0.0-00010101000000-000000000000
)

require (
	golang.org/x/net v0.29.0 // indirect
	golang.org/x/sys v0.25.0 // indirect
	golang.org/x/text v0.18.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240903143218-8af14fe29dc1 // indirect
	google.golang.org/grpc v1.66.2 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
)
