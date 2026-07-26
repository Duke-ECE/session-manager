FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o server ./cmd/server

FROM alpine:3.21
RUN adduser -D -u 10001 app
USER app
COPY --from=build /app/server /server
EXPOSE 50053
ENTRYPOINT ["/server"]
