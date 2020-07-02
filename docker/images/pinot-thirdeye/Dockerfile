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

FROM openjdk:8 AS thirdeye_build_env

LABEL MAINTAINER=dev@pinot.apache.org

ARG PINOT_BRANCH=master
ARG PINOT_GIT_URL="https://github.com/apache/incubator-pinot.git"
RUN echo "Trying to build Thirdeye from [ ${PINOT_GIT_URL} ] on branch [ ${PINOT_BRANCH} ]"

ENV TE_HOME=/opt/thirdeye
ENV TE_BUILD_DIR=/opt/thirdeye-build

# extra dependency for running launcher
RUN apt-get update \
    && curl -sL https://deb.nodesource.com/setup_10.x | bash - \
    && apt-get install -y --no-install-recommends vim wget curl git automake bison flex g++ libboost-all-dev libevent-dev libssl-dev libtool make pkg-config nodejs \
    && echo '{ "allow_root": true }' > /root/.bowerrc \
    && rm -rf /var/lib/apt/lists/*

# install maven
RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && wget https://www.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz -P /tmp \
  && tar -xzf /tmp/apache-maven-*.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven-*.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG /opt/.m2

RUN npm install -g phantomjs --unsafe-perm --ignore-scripts

RUN git clone ${PINOT_GIT_URL} ${TE_BUILD_DIR} \
    && cd ${TE_BUILD_DIR}/thirdeye  \
    && git checkout ${PINOT_BRANCH} \
    && cd thirdeye-frontend \
    && mvn clean install -X -DskipTests || exit 1 \
    && cd .. \
    && mvn clean install -X -DskipTests || exit 1 \
    && mkdir -p ${TE_HOME}/config/default \
    && mkdir -p ${TE_HOME}/bin \
    && cp -rp ${TE_BUILD_DIR}/thirdeye/thirdeye-pinot/config/* ${TE_HOME}/config/default/. \
    && rm ${TE_BUILD_DIR}/thirdeye/thirdeye-pinot/target/thirdeye-pinot-*-sources.jar \
    && cp ${TE_BUILD_DIR}/thirdeye/thirdeye-pinot/target/thirdeye-pinot-*.jar ${TE_HOME}/bin/thirdeye-pinot.jar \
    && rm -rf ${TE_BUILD_DIR}

FROM openjdk:8-jdk-slim

LABEL MAINTAINER=dev@pinot.apache.org

ENV TE_HOME=/opt/thirdeye

COPY --from=thirdeye_build_env ${TE_HOME} ${TE_HOME}
COPY bin ${TE_HOME}/bin
COPY config ${TE_HOME}/config

VOLUME ["${TE_HOME}/config"]
EXPOSE 1426 1427
WORKDIR ${TE_HOME}

ENTRYPOINT ["./bin/start-thirdeye.sh"]
