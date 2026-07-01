# Multi-stage build for cdp-api and cdp-worker. Migrations are embedded, so the
# runtime image is self-contained.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/cdp-api ./cmd/cdp-api && \
    CGO_ENABLED=0 go build -o /out/cdp-worker ./cmd/cdp-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/cdp-api /usr/local/bin/cdp-api
COPY --from=build /out/cdp-worker /usr/local/bin/cdp-worker
# Default to cdp-api; compose `command:` overrides CMD to select cdp-worker.
# (Must be CMD, not ENTRYPOINT, so `command:` replaces it rather than appending.)
CMD ["/usr/local/bin/cdp-api"]
