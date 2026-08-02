# syntax=docker/dockerfile:1

FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/cube-operator .

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/cube-operator /usr/local/bin/cube-operator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/cube-operator"]
