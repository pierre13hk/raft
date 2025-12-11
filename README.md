# raft
A raft implementation to learn the famous consensus algorithm and Go.

It is an incomplete implementation and contains many anti-patterns as this was personal learning project.

# implementation
## tools / api
- gRPC is used for node to node communication, but can be replaced by any object implementing the RaftRPC interface in rpc.go
- any state machine can be passed to a raft node as long as it implements the interface
  defined in state_machine.go for applying, serializing, deserializing, and reading state machine commands

## what works and what doesn't
Leader election and log replication are implemented.
Snapshotting and membership changes are not stable enough to be considered implemented.

# try it out
A demo is available under demo/.
The demo gives an example of a simple key/value state machine and where network failures can be simulated.

## simulating network failures
Two types of network failures are simulated: packet loss and partitions.  
To configure packet loss, you can modify the `DROP_RATE` environment variable in the docker compose.
At every packet send / RPC, if a random number between 0 and 1 is less than the defined `DROP_RATE`, an error is returned to the sender / caller.
To configure network partions, two environment variables can be used:
- `PARTITION_INTERVAL_MS`: the next partiton will be scheduled at random time in miliseconds between 0 and `PARTITION_INTERVAL_MS`.
- `PARTITION_OUTAGE_MS`: the maximum time to partion a node and make all its RPCs fail

## other configurations
The following raft related configurations are also available through environment variables in the docker compose.
- `ELECTION_TIMEOUT_MIN`: the number of miliseconds after which a follower becomes a candidate after
  not receiving heartbeats from the leader
- `ELECTION_TIMEOUT_MAX`: when starting an election, a candidate will wait at most ELECTION_TIMEOUT_MAX miliseconds before losing a term
- `HEARTBEAT_TIMEOUT_LEADER`: the number of miliseconds after which a leader sends heartbeats.
For a leader to be elected, `HEARTBEAT_TIMEOUT_LEADER` should be smaller than `ELECTION_TIMEOUT_MIN`

## communicating with the raft cluster and sending state machine commands
The docker compose defines 3 raft nodes and a container that can be used as client.
Since no HTTP API is available to communicate with the cluster, the easiest way to send
commands is to run shell commands in the client container.
Since the client accepts only read or write commands, here are the two types of shell commands you can run:

```bash
docker exec raft-client /app/raftbin write KEY VALUE
docker exec raft-client /app/raftbin read KEY
```

## running the demo
start the raft nodes and client container:
```bash
docker compose up
```

in another terminal, send a write command to the cluster:
```bash
docker exec raft-client /app/raftbin write hello world
```

once that's done, read the value:
```bash
docker exec raft-client /app/raftbin read hello
```

change the key
```bash
docker exec raft-client /app/raftbin write hello there
```

read the key
```bash
docker exec raft-client /app/raftbin read hello
```

set another key
```bash
docker exec raft-client /app/raftbin write specialkey specialvalue
```

read it back:
```bash
docker exec raft-client /app/raftbin read specialkey
```

and so on...

