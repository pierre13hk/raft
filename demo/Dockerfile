FROM golang:1.23

WORKDIR /app

COPY . /app

RUN go build -o raftbin .
RUN mkdir -p /app/raft/snapshots
CMD ["./raftbin"]