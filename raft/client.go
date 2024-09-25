package raft

type RaftClient interface {
	ConnectToCluster(servers []string) error
	Write(request ClientRequest) ClientRequestResponse
}


type RaftGrpcClient struct {
	leaderId uint64
	leaderAddr string
}
