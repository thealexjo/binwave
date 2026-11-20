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

# Static binary with bash for docker-entrypoint.sh, non-root (uid 65532), CA certificates included.
FROM debian:trixie-slim@sha256:a72b4433de1cf7d6bae0c4cee74d0547f58601a643b3375f070c51600d63b6b2

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

# The CA bundle of the build image: TLS to MySQL and ClickHouse without apt in this one.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# The same user as distroless nonroot, so that existing volumes keep their owner. /run/binwave holds
# the default control socket, as with RuntimeDirectory= of the unit; /docker-entrypoint.d holds the
# *.sh scripts docker-entrypoint.sh sources before the start (empty here, mounted or added in a derived image).
RUN groupadd --gid 65532 nonroot \
    && useradd --uid 65532 --gid 65532 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin nonroot \
    && install -d -o 65532 -g 65532 /run/binwave /var/lib/binwave \
    && install -d /docker-entrypoint.d
COPY --from=build /src/bin/binwave /usr/local/bin/binwave
COPY --chmod=0755 deploy/docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
USER 65532:65532

# A volume, so that the socket is writable in a read-only container.
VOLUME ["/run/binwave"]

# Same contract as the systemd unit: state lives in $STATE_DIRECTORY (file checkpoint store only).
ENV STATE_DIRECTORY=/var/lib/binwave
WORKDIR /var/lib/binwave
VOLUME ["/var/lib/binwave"]

# Metrics and health endpoints; set observability.listen to 0.0.0.0:9187 in the config.
EXPOSE 9187

# SIGTERM (docker stop) triggers graceful shutdown; allow enough time: docker stop -t 120.
STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
# No --config: binwave defaults to /etc/binwave/config.yaml, which deploy/docker/compose.example.yaml mounts there.
# Arguments that are not a command go to binwave: `docker run <image> tables ...` still works.
CMD ["binwave", "run"]
