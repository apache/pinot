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
ARG JAVA_VERSION=11
FROM debian:bookworm-slim

LABEL MAINTAINER=dev@pinot.apache.org

# Print platform info for debugging
RUN echo "Building for platform: $(uname -m)" && \
    echo "Architecture: $(dpkg --print-architecture)" && \
    cat /etc/os-release

# Upgrade all OS packages first to pick up security patches.
# Omit gpg binary: apt reads .asc (ASCII-armored) key files natively via
# signed-by= when placed in /etc/apt/keyrings/, so there is no need to
# install gpg/gpgconf/libreadline/libsqlite3 just for key dearmoring —
# avoiding the libsqlite3 CRITICAL CVE surface.
# curl is kept alongside wget because some build tooling (libssl configure
# tests, CI scripts) may invoke it; the libcurl CVE surface is acceptable
# in a build-only image that is not deployed to production.
RUN set -eux \
  && apt-get update \
  && apt-get upgrade -y --no-install-recommends \
  && apt-get install -y --no-install-recommends \
  ca-certificates java-common \
  wget curl git && \
  rm -rf /var/lib/apt/lists/*

# Install Amazon Corretto.
# /etc/apt/keyrings/ is the correct path for user-managed keys referenced via
# signed-by=; apt natively handles ASCII-armored .asc files at that path.
ARG JAVA_VERSION=11
RUN set -eux \
  && mkdir -p /etc/apt/keyrings \
  && wget -qO /etc/apt/keyrings/corretto.asc https://apt.corretto.aws/corretto.key \
  && echo "deb [signed-by=/etc/apt/keyrings/corretto.asc] https://apt.corretto.aws stable main" > /etc/apt/sources.list.d/corretto.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends java-${JAVA_VERSION}-amazon-corretto-jdk \
  && rm -rf /var/lib/apt/lists/*

# Install build dependencies separately
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
    automake bison flex g++ libtool make pkg-config && \
  rm -rf /var/lib/apt/lists/*

# Install libraries for Thrift build (headers-only boost, no Python bindings)
RUN apt-get update && \
  apt-get install -y --no-install-recommends libboost-dev libevent-dev libssl-dev && \
  rm -rf /var/lib/apt/lists/*

ENV LANG=C.UTF-8
ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-amazon-corretto

# install maven
RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && echo "Downloading Maven for $(uname -m) architecture..." \
  && wget https://dlcdn.apache.org/maven/maven-3/3.9.14/binaries/apache-maven-3.9.14-bin.tar.gz -P /tmp \
  && tar -xzf /tmp/apache-maven-*.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven-*.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn \
  && mvn --version
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
