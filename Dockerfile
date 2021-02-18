####################
## STEP 1: Build  ##
####################
# Image Base
FROM golang:1.14 AS builder

# Arguments
ARG VERSION="0.0"

# Labels
LABEL version=$VERSION
LABEL description="Keedio Cloudera Exporter Builder"
LABEL vendor="Keedio"

# Code copy
RUN mkdir /go/cloudera_exporter_code
WORKDIR /go/cloudera_exporter_code
COPY . .

# Environment variable to Static-link compilation
ENV CGO_ENABLED 0
# Work Go Path
ENV GOPATH /go
# GO OS system for compatibility compilation
ENV GOOS linux
# GO OS system architecture
ENV GOARCH amd64

# Run command
RUN make build





####################
## STEP 2: Run    ##
####################
FROM scratch

# Arguments
ARG VERSION="0.0"

# Labels
LABEL version=$VERSION
LABEL description="Keedio Cloudera Exporter Builder"
LABEL vendor="Keedio"

# Binary
COPY --from=builder /go/cloudera_exporter_code/cloudera_exporter /cloudera_exporter
# Config File
COPY --from=builder /go/cloudera_exporter_code/config.ini /config.ini

EXPOSE 9200

ENTRYPOINT ["/cloudera_exporter", "--config-file", "/config.ini"]
