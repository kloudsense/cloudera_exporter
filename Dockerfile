####################
## STEP 1: Build  ##
####################
# Image Base
FROM golang:1.12 AS builder

# Labels
LABEL maintainer="Keedio Sistemas <systems@keedio.com>"
LABEL version="1.3"
LABEL description="Keedio Cloudera Exporter Builder"
LABEL vendor="keedio"
LABEL image_name="cloudera_exporter_builder"

# System Configs
WORKDIR /go

# Code Folder
RUN mkdir cloudera_exporter_code

# Workdir with the cloudera_exporter's code
WORKDIR /go/cloudera_exporter_code

# Add cloudera_exporter's code
COPY ./collector ./collector
COPY ./config_parser ./config_parser
COPY ./json_parser ./json_parser
COPY ./logger ./logger
COPY ./cloudera_exporter.go ./cloudera_exporter.go
COPY ./config.ini ./config.ini
RUN go mod init keedio/cloudera_exporter

# Environment variable to Static-link compilation
ENV CGO_ENABLED 0
# Work Go Path
ENV GOPATH /go
# GO OS system for compatibility compilation
ENV GOOS linux
# GO OS system architecture
ENV GOARCH amd64

# Run command
RUN go build -o cloudera_exporter cloudera_exporter.go 





####################
## STEP 2: Run    ##
####################
FROM scratch

# Labels
LABEL maintainer="Keedio Sistemas <systems@keedio.com>"
LABEL version="1.3"
LABEL description="Keedio Cloudera Exporter"
LABEL vendor="keedio"
LABEL image_name="cloudera_exporter"

# Binary
COPY --from=builder /go/cloudera_exporter_code/cloudera_exporter /cloudera_exporter
# Config File
COPY --from=builder /go/cloudera_exporter_code/config.ini /config.ini

EXPOSE 9200

ENTRYPOINT ["/cloudera_exporter", "--config-file", "/config.ini"]
