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
import org.apache.pinot.spi.utils.CommonConstants.Helix;


/**
 * The {@code ServerRoutingInstance} class represents the routing target instance which contains the information of
 * hostname and port.
 */
@ThreadSafe
public final class ServerRoutingInstance {
  private static final Map<String, String> SHORT_HOSTNAME_MAP = new ConcurrentHashMap<>();

  private final String _instanceId;
  private final String _hostname;
  private final int _port;
  private final boolean _tlsEnabled;

  public ServerRoutingInstance(String instanceId, String hostname, int port, boolean tlsEnabled) {
    _instanceId = instanceId;
    _hostname = hostname;
    _port = port;
    _tlsEnabled = tlsEnabled;
  }

  public ServerRoutingInstance(String instanceId, String hostname, int port) {
    this(instanceId, hostname, port, false);
  }

  @VisibleForTesting
  public ServerRoutingInstance(String hostname, int port) {
    this(Helix.PREFIX_OF_SERVER_INSTANCE + hostname + "_" + port, hostname, port);
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

  public static String toShortName(String hostname) {
    try {
      InternetDomainName domainName = InternetDomainName.from(hostname);
      return domainName.parts().get(0);
    } catch (Exception e) {
      return hostname;
    }
  }

  public String getShortName() {
    return SHORT_HOSTNAME_MAP.computeIfAbsent(_hostname, ServerRoutingInstance::toShortName);
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
    // NOTE: Only check hostname and port for performance concern because they can identify a routing
    //       instance within the same query
    return _hostname.equals(that._hostname) && _port == that._port;
  }

  @Override
  public int hashCode() {
    // NOTE: Only check hostname and port for performance concern because they can identify a routing
    //       instance within the same query
    return 31 * _hostname.hashCode() + Integer.hashCode(_port);
  }

  @Override
  public String toString() {
    return getShortName();
  }
}
