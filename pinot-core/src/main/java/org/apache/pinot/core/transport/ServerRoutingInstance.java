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

import com.google.common.net.InternetDomainName;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.utils.CommonConstants.Helix;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;


/**
 * The {@code ServerRoutingInstance} class represents the routing target instance which contains the information of
 * hostname, port, and table type it serves.
 * <p>Different table types on same host and port are counted as different instances. Therefore, one single Pinot Server
 * might be treated as two different routing target instances based on the types of table it serves.
 */
@ThreadSafe
public class ServerRoutingInstance {
  private static final String HOSTNAME_PORT_DELIMITER = "_";
  private static final String SHORT_OFFLINE_SUFFIX = "_O";
  private static final String SHORT_REALTIME_SUFFIX = "_R";
  private static final Map<String, String> SHORT_HOSTNAME_MAP = new ConcurrentHashMap<>();

  private final String _hostname;
  private final int _port;
  private final TableType _tableType;

  /**
   * NOTE: server instance name is of format: {@code Server_<hostname>_<port>}, e.g. {@code Server_localhost_12345}.
   */
  public ServerRoutingInstance(String instanceName, TableType tableType) {
    String[] hostnameAndPort = instanceName.split(Helix.PREFIX_OF_SERVER_INSTANCE)[1].split(HOSTNAME_PORT_DELIMITER);
    _hostname = hostnameAndPort[0];
    _port = Integer.parseInt(hostnameAndPort[1]);
    _tableType = tableType;
  }

  public ServerRoutingInstance(String hostname, int port, TableType tableType) {
    _hostname = hostname;
    _port = port;
    _tableType = tableType;
  }

  public String getHostname() {
    return _hostname;
  }

  public int getPort() {
    return _port;
  }

  public TableType getTableType() {
    return _tableType;
  }

  public String getShortName() {
    String shortHostname = SHORT_HOSTNAME_MAP.computeIfAbsent(_hostname, hostname -> {
      try {
        InternetDomainName domainName = InternetDomainName.from(hostname);
        return domainName.parts().get(0);
      } catch (Exception e) {
        return hostname;
      }
    });
    return shortHostname + (_tableType == TableType.OFFLINE ? SHORT_OFFLINE_SUFFIX : SHORT_REALTIME_SUFFIX);
  }

  @Override
  public int hashCode() {
    return 31 * 31 * _hostname.hashCode() + 31 * _port + _tableType.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof ServerRoutingInstance) {
      ServerRoutingInstance that = (ServerRoutingInstance) obj;
      return _hostname.equals(that._hostname) && _port == that._port && _tableType == that._tableType;
    }
    return false;
  }

  @Override
  public String toString() {
    return getShortName();
  }
}
