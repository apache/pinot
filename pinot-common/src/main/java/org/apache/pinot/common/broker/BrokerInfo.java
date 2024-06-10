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
package org.apache.pinot.common.broker;

import java.util.Objects;


public class BrokerInfo {
  private final String _instanceId;
  private final String _hostname;
  private final int _port;
  private final int _tlsPort;

  public BrokerInfo(String instanceId, String hostname, int port, int tlsPort) {
    _instanceId = instanceId;
    _hostname = hostname;
    _port = port;
    _tlsPort = tlsPort;
  }

  public BrokerInfo(String hostname, int port) {
    _instanceId = hostname + ":" + port;
    _hostname = hostname;
    _port = port;
    _tlsPort = -1;
  }

  public String getInstanceId() {
    return _instanceId;
  }

  public String getHostname() {
    return _hostname;
  }

  public int getPort() {
    return _port;
  }

  public int getTlsPort() {
    return _tlsPort;
  }

  public String getHostPort(boolean preferTlsPort) {
    return String.format("%s:%s", _hostname, preferTlsPort && _tlsPort > 0 ? _tlsPort : _port);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BrokerInfo that = (BrokerInfo) o;
    return Objects.equals(getInstanceId(), that.getInstanceId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getInstanceId());
  }
}
