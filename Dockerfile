ARG BUILDPLATFORM=linux/amd64
ARG TARGETOS=linux
ARG TARGETARCH=amd64
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS app-builder
ARG TARGETOS
ARG TARGETARCH
WORKDIR /go/src/app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY internal ./internal
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags '-extldflags "-static"' -tags timetzdata -buildvcs=false -o /nirn-proxy . && mkdir -m 1777 /runtime-tmp

FROM scratch
COPY --from=app-builder /nirn-proxy /nirn-proxy
COPY --from=app-builder /runtime-tmp /tmp
COPY --from=app-builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
EXPOSE 9000
EXPOSE 8080
EXPOSE 7946/tcp
EXPOSE 7946/udp
EXPOSE 8443
USER 65532:65532
ENTRYPOINT ["/nirn-proxy"]
