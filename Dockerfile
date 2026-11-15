# syntax=docker/dockerfile:1.27@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e

# The compiler runs on the platform of the build and cross-compiles for the target one.
FROM --platform=$BUILDPLATFORM golang:1.27.4@sha256:5e8afd493a7124a8586e134d9b532731161d4b9becf9dee5a5956b107426f93d AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    GOOS=${TARGETOS} GOARCH=${TARGETARCH} make build VERSION=${VERSION} COMMIT=${COMMIT} DATE=${DATE}

# Static binary, no shell, non-root (uid 65532), CA certificates included.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:aafe993665bf6200db38a02c11a9351e782a84b6a45e7f33b6695638c88e3931

ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
LABEL org.opencontainers.image.title="binwave" \
      org.opencontainers.image.description="Real-time replication of MySQL changes into ClickHouse" \
      org.opencontainers.image.source="https://github.com/thealexjo/binwave" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.created="${DATE}"

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
