/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.api.resources;

public class InstanceInfo {
  private final String _instanceName;
  private final String _host;
  private final Integer _port;
  private final Integer _grpcPort;

  public InstanceInfo(String instanceName, String host, Integer port, Integer grpcPort) {
    _instanceName = instanceName;
    _host = host;
    _port = port;
    _grpcPort = grpcPort;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public String getHost() {
    return _host;
  }

  public Integer getPort() {
    return _port;
  }

  public Integer getGrpcPort() {
    return _grpcPort;
  }
}
