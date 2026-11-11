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
# Uses debian:bookworm-slim as the base instead of the full amazoncorretto
# AL2023 image to avoid system packages (python3, etc.) that are not needed
# at runtime and consistently accumulate CVEs. wget is used instead of curl
# to avoid pulling in libcurl's LDAP/HTTP2 dependencies.
ARG JAVA_VERSION=21
FROM debian:bookworm-slim

LABEL MAINTAINER=dev@pinot.apache.org

# Upgrade OS packages and install prerequisites for Corretto apt repo.
# Use wget (not curl) to avoid libcurl → libldap/libnghttp2 CVE surface.
# Omit gpg binary: apt reads .asc (ASCII-armored) key files natively via
# signed-by= when placed in /etc/apt/keyrings/, so there is no need to
# install gpg/gpgconf/libreadline/libsqlite3 just for key dearmoring —
# avoiding the libsqlite3 CRITICAL CVE surface.
RUN set -eux \
  && apt-get update \
  && apt-get upgrade -y --no-install-recommends \
  && apt-get install -y --no-install-recommends \
    wget ca-certificates java-common \
  && rm -rf /var/lib/apt/lists/*

# Install Amazon Corretto JDK.
# /etc/apt/keyrings/ is the correct path for user-managed keys referenced via
# signed-by=; apt natively handles ASCII-armored .asc files at that path.
ARG JAVA_VERSION=21
RUN set -eux \
  && mkdir -p /etc/apt/keyrings \
  && wget -qO /etc/apt/keyrings/corretto.asc https://apt.corretto.aws/corretto.key \
  && echo "deb [signed-by=/etc/apt/keyrings/corretto.asc] https://apt.corretto.aws stable main" > /etc/apt/sources.list.d/corretto.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends java-${JAVA_VERSION}-amazon-corretto-jdk \
  && rm -rf /var/lib/apt/lists/*

ENV LANG=C.UTF-8
ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-amazon-corretto
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN apt-get update && \
  apt-get install -y --no-install-recommends \
    procps wget zstd libtasn1-6 && \
  rm -rf /var/lib/apt/lists/*

RUN case `uname -m` in \
  x86_64) arch=x64; ;; \
  aarch64) arch=arm64; ;; \
  *) echo "platform=$(uname -m) un-supported, exit ..."; exit 1; ;; \
  esac \
  && mkdir -p /usr/local/lib/async-profiler \
  && wget -qO- https://github.com/async-profiler/async-profiler/releases/download/v4.3/async-profiler-4.3-linux-${arch}.tar.gz | tar -xz --strip-components 1 -C /usr/local/lib/async-profiler \
  && ln -s /usr/local/lib/async-profiler/bin/asprof /usr/local/bin/async-profiler

CMD ["bash"]
