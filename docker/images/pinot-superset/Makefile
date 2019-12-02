#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

image  := fx19880617/pinot-superset
stages := build dist final
shells := $(foreach stage,$(stages),shell@$(stage))

node_version     := latest
python_version   := 3.6
superset_version := 0.34.1

.PHONY: all clean push $(stages) $(shells)

all: latest

.docker:
	mkdir -p $@

.docker/$(superset_version)@dist:  .docker/$(superset_version)@build
.docker/$(superset_version)@final: .docker/$(superset_version)@dist
.docker/$(superset_version)@%:   | .docker
	docker build \
	--build-arg NODE_VERSION=$(node_version) \
	--build-arg PYTHON_VERSION=$(python_version) \
	--build-arg SUPERSET_VERSION=$(superset_version) \
	--iidfile $@ \
	--tag $(image):$(superset_version)-$* \
	--target $* .

.docker/latest .docker/$(superset_version): .docker/$(superset_version)@final
.docker/%:
	docker tag $(shell cat $<) $(image):$*
	cp $< $@

clean:
	-docker image rm -f $(shell awk {print} .docker/*)
	-rm -rf .docker

latest: .docker/latest .docker/$(superset_version)

push: .docker/latest .docker/$(superset_version)
	docker push $(image):$(superset_version)
	docker push $(image):latest

$(stages): %: .docker/$(superset_version)@%

$(shells): shell@%: .docker/$(superset_version)@%
	docker run --rm -it --entrypoint /bin/bash $(shell cat $<)
