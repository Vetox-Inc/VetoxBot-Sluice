# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.27-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build
ARG TARGETOS TARGETARCH TARGETVARIANT VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOARM=${TARGETVARIANT#v} \
    go build -trimpath -buildvcs=false \
    -ldflags "-s -w -X github.com/Vetox-Inc/VetoxBot-Sluice/internal/proxy.Version=$VERSION" \
    -o /out/sluice .

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
ARG VERSION=dev
LABEL org.opencontainers.image.title="Sluice" \
      org.opencontainers.image.description="Discord REST rate-limit proxy" \
      org.opencontainers.image.source="https://github.com/Vetox-Inc/VetoxBot-Sluice" \
      org.opencontainers.image.licenses="GPL-3.0-only" \
      org.opencontainers.image.version="$VERSION"
COPY --from=build /out/sluice /sluice
# Inside a container the network namespace is the boundary, so listen on every interface.
ENV BIND_IP=0.0.0.0
EXPOSE 8080 9000
ENTRYPOINT ["/sluice"]
