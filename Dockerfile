FROM golang:1.17.3-alpine
WORKDIR /opt/estuary-sync
COPY ./ .
RUN go build -o estuary-sync

# FROM scratch
# COPY --from=0 /opt/estuary-sync/estuary-sync ./
ENTRYPOINT ["./estuary-sync"]
