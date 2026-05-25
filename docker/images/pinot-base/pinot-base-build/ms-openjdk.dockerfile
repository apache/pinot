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
ARG JAVA_VERSION=21
ARG JDK_IMAGE=mcr.microsoft.com/openjdk/jdk
FROM ${JDK_IMAGE}:${JAVA_VERSION}-ubuntu AS pinot_build_env

LABEL MAINTAINER=dev@pinot.apache.org

# Print platform info for debugging
RUN echo "Building for platform: $(uname -m)" && \
    echo "Architecture: $(dpkg --print-architecture)" && \
    cat /etc/os-release

# Upgrade all OS packages first to pick up security patches, then install build deps
RUN set -eux && apt-get update && \
  apt-get upgrade -y --no-install-recommends && \
  apt-get install -y --no-install-recommends \
    wget curl git \
    automake bison flex g++ \
    libtool make pkg-config && \
  rm -rf /var/lib/apt/lists/*

# Install libraries for Thrift build (headers-only boost, no Python bindings)
RUN apt-get update && \
  apt-get install -y --no-install-recommends libboost-dev libevent-dev libssl-dev && \
  rm -rf /var/lib/apt/lists/*

# install maven
ARG MAVEN_VERSION=3.9.14
RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && echo "Downloading Maven ${MAVEN_VERSION} for $(uname -m) architecture..." \
  && wget https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz -O /tmp/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && wget https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz.sha512 -O /tmp/apache-maven-${MAVEN_VERSION}-bin.tar.gz.sha512 \
  && echo "$(cat /tmp/apache-maven-${MAVEN_VERSION}-bin.tar.gz.sha512)  /tmp/apache-maven-${MAVEN_VERSION}-bin.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven-${MAVEN_VERSION}-bin.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven-${MAVEN_VERSION}-bin.tar.gz* \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn \
  && mvn --version | tee /tmp/maven-version \
  && grep -F "Apache Maven ${MAVEN_VERSION}" /tmp/maven-version \
  && rm -f /tmp/maven-version
ENV MAVEN_HOME=/usr/share/maven
ENV MAVEN_CONFIG=/opt/.m2

# install thrift — version matches libthrift in Pinot's pom.xml
RUN echo "Building Thrift for $(uname -m) architecture..." && \
  wget https://archive.apache.org/dist/thrift/0.22.0/thrift-0.22.0.tar.gz -O /tmp/thrift-0.22.0.tar.gz && \
  wget https://archive.apache.org/dist/thrift/0.22.0/thrift-0.22.0.tar.gz.sha512 -O /tmp/thrift-0.22.0.tar.gz.sha512 && \
  echo "$(cat /tmp/thrift-0.22.0.tar.gz.sha512)  /tmp/thrift-0.22.0.tar.gz" | sha512sum -c - && \
  tar xfz /tmp/thrift-0.22.0.tar.gz --directory /tmp && \
  cd /tmp/thrift-0.22.0 && \
  echo "Configuring Thrift..." && \
  ./configure --with-cpp=no --with-c_glib=no --with-java=yes --with-python=no --with-ruby=no --with-erlang=no --with-go=no --with-nodejs=no --with-php=no && \
  echo "Building Thrift..." && \
  make -j$(nproc) install && \
  echo "Thrift installation completed" && \
  rm -rf /tmp/thrift-0.22.0*

CMD ["bash"]
