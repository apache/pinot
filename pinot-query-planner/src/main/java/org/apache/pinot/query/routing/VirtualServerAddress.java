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

package org.apache.pinot.query.routing;

import java.util.Objects;


/**
 * Represents the address of a {@link QueryServerInstance} containing both the ID of the specific worker and the
 * physical host/port info from {@link QueryServerInstance}.
 */
public class VirtualServerAddress {

  private final String _hostname;
  private final int _port;
  private final int _workerId;

  public VirtualServerAddress(String hostname, int port, int workerId) {
    _hostname = hostname;
    _port = port;
    _workerId = workerId;
  }

  public VirtualServerAddress(QueryServerInstance server, int workerId) {
    this(server.getHostname(), server.getQueryMailboxPort(), workerId);
  }

  /**
   * Parses the standard hostname:port pattern into
   * a {@code ServerAddress}
   *
   * @param address the serialized string
   * @return the deserialized form
   */
  public static VirtualServerAddress parse(String address) {
    String[] split = address.split("@");
    String[] hostSplit = split[1].split(":");
    return new VirtualServerAddress(hostSplit[0], Integer.parseInt(hostSplit[1]), Integer.parseInt(split[0]));
  }

  /**
   * @return the server's hostname
   */
  public String hostname() {
    return _hostname;
  }

  /**
   * @return the server's port
   */
  public int port() {
    return _port;
  }

  public int workerId() {
    return _workerId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VirtualServerAddress that = (VirtualServerAddress) o;
    return _port == that._port
        && _workerId == that._workerId
        && Objects.equals(_hostname, that._hostname);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_hostname, _port, _workerId);
  }

  @Override
  public String toString() {
    return _workerId + "@" + _hostname + ":" + _port;
  }
}
