<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Pinot connectors module

The pinot-connectors module is the place to write any pinot connectors to streams. For exampple, the pinot-connector-kafka-0.9 sub module contains the stream implementation for kafka-0.9. Dependencies to be shared across all sub modules are to be added in the pinot-connectors/pom.xml, and dependencies specific to the specific connector should go in its own pom file
