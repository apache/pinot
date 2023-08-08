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
ARG PINOT_BASE_IMAGE_TAG=11-amazoncorretto
FROM apachepinot/pinot-base-build:${PINOT_BASE_IMAGE_TAG} AS pinot_build_env

LABEL MAINTAINER=dev@pinot.apache.org

ARG PINOT_BRANCH=master
ARG JDK_VERSION=11
ARG PINOT_GIT_URL="https://github.com/apache/pinot.git"
ARG CI=true

RUN echo "Build Pinot based on image: apachepinot/pinot-base-build:${PINOT_BASE_IMAGE_TAG}"
RUN echo "Current build system CPU arch is [ $(uname -m) ]"

RUN echo "Trying to build Pinot from [ ${PINOT_GIT_URL} ] on branch [ ${PINOT_BRANCH} ] and CI [ ${CI} ]"
ENV PINOT_HOME=/opt/pinot
ENV PINOT_BUILD_DIR=/opt/pinot-build
ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG /opt/.m2

RUN git clone ${PINOT_GIT_URL} ${PINOT_BUILD_DIR} && \
    cd ${PINOT_BUILD_DIR} && \
    git checkout ${PINOT_BRANCH} && \
    mvn install package -DskipTests -Pbin-dist -Pbuild-shaded-jar -Djdk.version=${JDK_VERSION} -T1C && \
    rm -rf /root/.m2 && \
    mkdir -p ${PINOT_HOME}/configs && \
    mkdir -p ${PINOT_HOME}/data && \
    cp -r build/* ${PINOT_HOME}/. && \
    chmod +x ${PINOT_HOME}/bin/*.sh

FROM apachepinot/pinot-base-runtime:${PINOT_BASE_IMAGE_TAG}

LABEL MAINTAINER=dev@pinot.apache.org

ENV PINOT_HOME=/opt/pinot
ENV JAVA_OPTS="-Xms4G -Xmx4G -Dpinot.admin.system.exit=false"

VOLUME ["${PINOT_HOME}/configs", "${PINOT_HOME}/data"]

COPY --from=pinot_build_env ${PINOT_HOME} ${PINOT_HOME}
COPY bin ${PINOT_HOME}/bin
COPY etc ${PINOT_HOME}/etc
COPY examples ${PINOT_HOME}/examples

RUN wget -O ${PINOT_HOME}/etc/jmx_prometheus_javaagent/jmx_prometheus_javaagent-0.18.0.jar https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.18.0/jmx_prometheus_javaagent-0.18.0.jar && \
    ln -s ${PINOT_HOME}/etc/jmx_prometheus_javaagent/jmx_prometheus_javaagent-0.18.0.jar ${PINOT_HOME}/etc/jmx_prometheus_javaagent/jmx_prometheus_javaagent.jar

# expose ports for controller/broker/server/admin
EXPOSE 9000 8099 8098 8097 8096

WORKDIR ${PINOT_HOME}

ENTRYPOINT ["./bin/pinot-admin.sh"]

CMD ["-help"]
