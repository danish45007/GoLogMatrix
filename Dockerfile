FROM golang:1.21.5

RUN mkdir /app

WORKDIR /app
RUN apt-get update && \
	apt-get install -y entr
	
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .