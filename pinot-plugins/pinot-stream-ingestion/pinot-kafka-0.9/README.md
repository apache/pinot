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
# Pinot connector for kafka 0.9.x

This is an implementation of the kafka stream for kafka versions 0.9.x. The version used in this implementation is kafka 0.9.0.1. This module compiles with version 0.9.0.0 as well, however we have not tested if it runs with the older versions.
A stream plugin for another version of kafka, or another stream, can be added in a similar fashion. Refer to documentation on (Pluggable Streams)[https://docs.pinot.apache.org/developers-and-contributors/extending-pinot/pluggable-streams] for the specfic interfaces to implement.
