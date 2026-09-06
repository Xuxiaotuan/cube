# syntax=docker/dockerfile:1

FROM golang:1.25 AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -o /out/cube-operator .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH} go build -o /out/lease-agent ./cmd/lease-agent

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/cube-operator /usr/local/bin/cube-operator
COPY --from=build /out/lease-agent /usr/local/bin/lease-agent
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/cube-operator"]
