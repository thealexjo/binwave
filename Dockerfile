# syntax=docker/dockerfile:1

ARG GO_VERSION=1.27.1

FROM golang:${GO_VERSION} AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    make build VERSION=${VERSION} COMMIT=${COMMIT} DATE=${DATE}

# Static binary, no shell, non-root (uid 65532), CA certificates included.
FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=build /src/bin/binwave /usr/local/bin/binwave

# Same contract as the systemd unit: state lives in $STATE_DIRECTORY (file checkpoint store only).
ENV STATE_DIRECTORY=/var/lib/binwave
WORKDIR /var/lib/binwave
VOLUME ["/var/lib/binwave"]

# Metrics and health endpoints; set observability.listen to 0.0.0.0:9187 in the config.
EXPOSE 9187

# SIGTERM (docker stop) triggers graceful shutdown; allow enough time: docker stop -t 120.
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/binwave"]
# No --config: binwave defaults to /etc/binwave/config.yaml, which deploy/docker/compose.example.yaml mounts there.
CMD ["run"]
