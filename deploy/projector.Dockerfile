FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /projector ./cmd/projector

FROM gcr.io/distroless/static-debian12
USER 65532:65532
COPY --from=build /projector /projector
ENTRYPOINT ["/projector"]
