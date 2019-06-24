# Copyright 2019 Keedio
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: Makefile.common
include Makefile.common

.PHONY: all
all: docker_build docker_run test 

.PHONY: docker_build
docker_build: build_in_docker

.PHONY: docker_run
docker_run: run_in_docker

.PHONY: docker
docker: docker_build docker_run

.PHONY: stop
stop: stop_docker

.PHONY: local_build
local_build: build_in_local

.PHONY: local_run
local_run: run_in_local

.PHONY: local
local: local_build local_run
	@echo "If Cloudera Exporter launch fails, check your config.ini or your /etc/hosts file"

.PHONY: test
test: test_exporter

.PHONY: clean
clean: clean_env

.PHONY: help
help: print_help
