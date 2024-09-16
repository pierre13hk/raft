FROM golang:1.23

WORKDIR /app

COPY go.mod go.sum .
COPY . /app

RUN go build -o test_bin .
EXPOSE 8080

CMD ["./test_bin"]