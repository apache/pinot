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
package org.apache.pinot.core.transport;

import com.google.common.annotations.VisibleForTesting;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.spi.config.table.TableType;


public class ServerInstance {
  private static final int SERVER_INSTANCE_PREFIX_LENGTH = Helix.PREFIX_OF_SERVER_INSTANCE.length();
  private static final String HOSTNAME_PORT_DELIMITER = "_";

  private final String _hostname;
  private final int _port;

  /**
   * By default (auto joined instances), server instance name is of format: {@code Server_<hostname>_<port>}, e.g.
   * {@code Server_localhost_12345}, hostname is of format: {@code Server_<hostname>}, e.g. {@code Server_localhost}.
   */
  public ServerInstance(InstanceConfig instanceConfig) {
    String hostname = instanceConfig.getHostName();
    if (hostname != null) {
      if (hostname.startsWith(Helix.PREFIX_OF_SERVER_INSTANCE)) {
        _hostname = hostname.substring(SERVER_INSTANCE_PREFIX_LENGTH);
      } else {
        _hostname = hostname;
      }
      _port = Integer.parseInt(instanceConfig.getPort());
    } else {
      // Hostname might be null in some tests (InstanceConfig created by calling the constructor instead of fetching
      // from ZK), directly parse the instance name
      String instanceName = instanceConfig.getInstanceName();
      String[] hostnameAndPort = instanceName.split(Helix.PREFIX_OF_SERVER_INSTANCE)[1].split(HOSTNAME_PORT_DELIMITER);
      _hostname = hostnameAndPort[0];
      _port = Integer.parseInt(hostnameAndPort[1]);
    }
  }

  @VisibleForTesting
  ServerInstance(String hostname, int port) {
    _hostname = hostname;
    _port = port;
  }

  public String getHostname() {
    return _hostname;
  }

  public int getPort() {
    return _port;
  }

  public ServerRoutingInstance toServerRoutingInstance(TableType tableType) {
    return new ServerRoutingInstance(_hostname, _port, tableType);
  }

  @Override
  public int hashCode() {
    return 31 * _hostname.hashCode() + _port;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof ServerInstance) {
      ServerInstance that = (ServerInstance) obj;
      return _hostname.equals(that._hostname) && _port == that._port;
    }
    return false;
  }

  /**
   * Use default format {@code Server_<hostname>_<port>} for backward-compatibility.
   */
  @Override
  public String toString() {
    return Helix.PREFIX_OF_SERVER_INSTANCE + _hostname + HOSTNAME_PORT_DELIMITER + _port;
  }
}
