FROM golang:1.23 AS build
WORKDIR /app
COPY . /app
RUN CGO_ENABLED=0 GOOS=linux go build -o raftbin .

# latest at time of writing
FROM alpine:3.23.0 AS runtime
WORKDIR /app
COPY --from=build /app/raftbin ./raftbin
RUN mkdir -p /app/raft/snapshots
CMD ["/app/raftbin"]