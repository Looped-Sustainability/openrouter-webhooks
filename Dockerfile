FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
ARG REVISION=development
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.buildRevision=${REVISION}" -o /proxy .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && addgroup -g 10001 proxy && adduser -D -H -u 10001 -G proxy proxy && mkdir /data && chown proxy:proxy /data
COPY --from=build /proxy /usr/local/bin/proxy
USER 10001:10001
ENV DATA_DIR=/data
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/proxy"]
