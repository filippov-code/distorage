FROM golang:1.20-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# protos из папки, расположенной выше
COPY ../protos/ ./protos/

RUN CGO_ENABLED=0 go build -o /gateway ./main.go

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /gateway .

CMD ["./gateway"]