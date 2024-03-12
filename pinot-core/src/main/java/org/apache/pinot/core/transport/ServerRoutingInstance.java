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
import com.google.common.net.InternetDomainName;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.CommonConstants.Helix;


/**
 * The {@code ServerRoutingInstance} class represents the routing target instance which contains the information of
 * hostname, port, and table type it serves.
 * <p>Different table types on same host and port are counted as different instances. Therefore, one single Pinot Server
 * might be treated as two different routing target instances based on the types of table it serves.
 */
@ThreadSafe
public final class ServerRoutingInstance {
  private static final String SHORT_OFFLINE_SUFFIX = "_O";
  private static final String SHORT_REALTIME_SUFFIX = "_R";
  private static final Map<String, String> SHORT_HOSTNAME_MAP = new ConcurrentHashMap<>();

  private final String _instanceId;
  private final String _hostname;
  private final int _port;
  private final TableType _tableType;
  private final boolean _tlsEnabled;

  public ServerRoutingInstance(String instanceId, String hostname, int port, TableType tableType, boolean tlsEnabled) {
    _instanceId = instanceId;
    _hostname = hostname;
    _port = port;
    _tableType = tableType;
    _tlsEnabled = tlsEnabled;
  }

  public ServerRoutingInstance(String instanceId, String hostname, int port, TableType tableType) {
    this(instanceId, hostname, port, tableType, false);
  }

  @VisibleForTesting
  public ServerRoutingInstance(String hostname, int port, TableType tableType) {
    this(Helix.PREFIX_OF_SERVER_INSTANCE + hostname + "_" + port, hostname, port, tableType);
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

  public boolean isTlsEnabled() {
    return _tlsEnabled;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ServerRoutingInstance)) {
      return false;
    }
    ServerRoutingInstance that = (ServerRoutingInstance) o;
    // NOTE: Only check hostname, port and tableType for performance concern because they can identify a routing
    //       instance within the same query
    return _hostname.equals(that._hostname) && _port == that._port && _tableType == that._tableType;
  }

  @Override
  public int hashCode() {
    // NOTE: Only check hostname, port and tableType for performance concern because they can identify a routing
    //       instance within the same query
    return 31 * 31 * _hostname.hashCode() + 31 * Integer.hashCode(_port) + _tableType.hashCode();
  }

  @Override
  public String toString() {
    return getShortName();
  }
}
