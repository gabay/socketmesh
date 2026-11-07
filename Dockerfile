# Stage 1: Build
FROM golang:alpine AS builder

WORKDIR /src

COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/socketmesh ./cmd/socketmesh

# Stage 2: Minimal unprivileged runtime
FROM alpine:3.21

RUN apk --no-cache add ca-certificates && \
    addgroup -S socketmesh && \
    adduser -S -G socketmesh socketmesh

COPY --from=builder /out/socketmesh /usr/local/bin/socketmesh

USER socketmesh
EXPOSE 2375

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 CMD ["/usr/local/bin/socketmesh", "--healthcheck"]

ENTRYPOINT ["/usr/local/bin/socketmesh"]
