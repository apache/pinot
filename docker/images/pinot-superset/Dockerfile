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

ARG SUPERSET_IMAGE_TAG=latest
FROM apache/superset:${SUPERSET_IMAGE_TAG}

# Switching to root to install the required packages
USER root

# Install pinotdb driver to connect to Pinot
COPY requirements-db.txt requirements-db.txt

RUN pip install --no-cache -r requirements-db.txt

COPY examples /etc/examples/pinot

# Switching back to using the `superset` user
USER superset
