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
ARG JDK_IMAGE=amazoncorretto

FROM ${JDK_IMAGE}:${JAVA_VERSION}-al2-jdk

LABEL MAINTAINER=dev@pinot.apache.org

RUN yum update -y && \
  yum groupinstall 'Development Tools' -y && \
  yum install -y procps vim less wget curl git python sysstat perf libtasn1 && \
  yum clean all

RUN case `uname -m` in \
  x86_64) arch=x64; ;; \
  aarch64) arch=arm64; ;; \
  *) echo "platform=$(uname -m) un-supported, exit ..."; exit 1; ;; \
  esac \
  && mkdir -p /usr/local/lib/async-profiler \
  && curl -L https://github.com/jvm-profiling-tools/async-profiler/releases/download/v2.9/async-profiler-2.9-linux-${arch}.tar.gz | tar -xz --strip-components 1 -C /usr/local/lib/async-profiler \
  && ln -s /usr/local/lib/async-profiler/profiler.sh /usr/local/bin/async-profiler

CMD ["bash"]
