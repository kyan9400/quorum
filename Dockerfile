# Build every command as a static binary, after the tests pass. A build that
# fails its own tests produces no image.
FROM golang:1.27.3-alpine3.23 AS build
WORKDIR /src
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOTELEMETRY=off
COPY go.mod ./
COPY client ./client
COPY cmd ./cmd
COPY internal ./internal
RUN go vet ./... && go test ./... && \
    go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/... && \
    mkdir -p /data

# The binaries and the recorded runs, nothing else: no shell, no package
# manager. The dashboard starts the five quorumd processes itself.
FROM scratch
COPY --from=build /out/ /usr/local/bin/
COPY results /srv/results
# An empty /data owned by the runtime user: a new volume mounted here takes
# its ownership, so the nodes can write their logs to it.
COPY --from=build --chown=65534:65534 /data /data
USER 65534:65534
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/dash"]
CMD ["-bin", "/usr/local/bin/quorumd", "-data", "/data/live", "-results", "/srv/results", "-listen", ":8080", "-tick", "100ms", "-clients", "4", "-pause", "300ms"]
