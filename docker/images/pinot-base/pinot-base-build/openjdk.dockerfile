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
ARG JDK_IMAGE=openjdk
# At 2023-06-14, slim is the only openjdk flavour without medium, high or critical vulns
FROM ${JDK_IMAGE}:${JAVA_VERSION}-jdk-slim

LABEL MAINTAINER=dev@pinot.apache.org

ENV MAVEN_HOME=/usr/share/maven
ENV MAVEN_CONFIG=/root/.m2

# Print platform info for debugging
RUN echo "Building for platform: $(uname -m)" && \
    echo "Architecture: $(dpkg --print-architecture)" && \
    cat /etc/os-release

# extra dependency for running launcher
# Split package installation to better identify issues
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
    vim wget curl git \
    automake bison flex g++ \
    libtool make pkg-config && \
  rm -rf /var/lib/apt/lists/*

# Install boost separately as it can be problematic on ARM64
RUN apt-get update && \
  apt-get install -y --no-install-recommends \
    libboost-all-dev libevent-dev libssl-dev && \
  rm -rf /var/lib/apt/lists/*

# install maven
RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && echo "Downloading Maven for $(uname -m) architecture..." \
  && wget https://dlcdn.apache.org/maven/maven-3/3.9.10/binaries/apache-maven-3.9.10-bin.tar.gz -P /tmp \
  && tar -xzf /tmp/apache-maven-*.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven-*.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn \
  && mvn --version

# install thrift with better error handling
RUN echo "Building Thrift for $(uname -m) architecture..." && \
  wget https://archive.apache.org/dist/thrift/0.12.0/thrift-0.12.0.tar.gz -O /tmp/thrift-0.12.0.tar.gz && \
  tar xfz /tmp/thrift-0.12.0.tar.gz --directory /tmp && \
  cd /tmp/thrift-0.12.0 && \
  echo "Configuring Thrift..." && \
  ./configure --with-cpp=no --with-c_glib=no --with-java=yes --with-python=no --with-ruby=no --with-erlang=no --with-go=no --with-nodejs=no --with-php=no && \
  echo "Building Thrift..." && \
  make -j$(nproc) install && \
  echo "Thrift installation completed" && \
  rm -rf /tmp/thrift-0.12.0*

CMD ["bash"]
