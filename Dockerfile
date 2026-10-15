FROM golang:1.27.2-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY contract ./contract
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
COPY testdata ./testdata
RUN CGO_ENABLED=0 go build -o /orbit-control ./cmd/orbit-control \
  && CGO_ENABLED=0 go build -o /orbit-admin ./cmd/orbit-admin

FROM debian:bookworm-slim
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates curl \
  && rm -rf /var/lib/apt/lists/*
COPY --from=build /orbit-control /usr/local/bin/orbit-control
COPY --from=build /orbit-admin /usr/local/bin/orbit-admin
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=12 \
  CMD curl -fsS http://127.0.0.1:8080/health
ENTRYPOINT ["/usr/local/bin/orbit-control"]
CMD ["api"]
