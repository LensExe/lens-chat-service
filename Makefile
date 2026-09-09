# app name
APP_NAME := server

# run dev
run:
	go run cmd/${APP_NAME}/main.go

test:
	go test ./...

race:
	go test -race ./...

build:
	go build ./cmd/server
