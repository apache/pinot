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

FROM openjdk:8

LABEL MAINTAINER=dev@pinot.apache.org

ARG PRESTO_BRANCH=master
ARG PRESTO_GIT_URL="https://github.com/prestodb/presto.git"
RUN echo "Trying to build Presto image from [ ${PRESTO_GIT_URL} ] on branch [ ${PRESTO_BRANCH} ]"
ENV PRESTO_HOME=/home/presto
ENV PRESTO_BUILD_DIR=/home/presto/build

# extra dependency for running launcher
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential vim wget curl git \
    gcc libpq-dev python3-dev python3-pip python3-venv python3-wheel && \
    pip3 install --upgrade pip setuptools wheel && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN groupadd -g 999 presto && \
    useradd -r -u 999 -g presto --create-home --shell /bin/bash presto
USER presto

RUN git clone ${PRESTO_GIT_URL} ${PRESTO_BUILD_DIR} && \
    cd ${PRESTO_BUILD_DIR} && \
    git checkout ${PRESTO_BRANCH} && \
    echo "distributionUrl=https://repo1.maven.org/maven2/org/apache/maven/apache-maven/3.9.4/apache-maven-3.9.4-bin.zip" > .mvn/wrapper/maven-wrapper.properties && \
    ./mvnw clean install -DskipTests && \
    mkdir -p ${PRESTO_HOME}/data && \
    cp -r presto-server/target/presto-server-*/presto-server-*/* ${PRESTO_HOME} && \
    mkdir -p ${PRESTO_HOME}/plugin/pinot && cp -r presto-pinot/target/presto-pinot-*/* ${PRESTO_HOME}/plugin/pinot/ && \
    rm -rf ${PRESTO_BUILD_DIR} ${PRESTO_HOME}/.m2

COPY etc ${PRESTO_HOME}/etc

EXPOSE 8080

VOLUME ["${PRESTO_HOME}/etc", "${PRESTO_HOME}/data"]

WORKDIR ${PRESTO_HOME}

ENTRYPOINT ["./bin/launcher"]

CMD ["run"]
