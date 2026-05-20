FROM golang:1.26-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown

RUN CGO_ENABLED=0 go build \
    -ldflags "-s -w \
      -X github.com/sentinelhq-sh/outpost/internal/version.Version=${VERSION} \
      -X github.com/sentinelhq-sh/outpost/internal/version.Commit=${COMMIT}" \
    -o /outpost \
    ./cmd/outpost

# distroless/static includes CA certificates for HTTPS calls to the SentinelHQ backend.
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /outpost /outpost

EXPOSE 8080 9090

ENTRYPOINT ["/outpost"]
