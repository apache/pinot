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

package org.apache.pinot.query.mailbox;

import java.util.Objects;


public class ServerAddress {

  private final String _hostname;
  private final int _port;

  public ServerAddress(String hostname, int port) {
    _hostname = hostname;
    _port = port;
  }

  /**
   * Parses the standard hostname:port pattern into
   * a {@code ServerAddress}
   *
   * @param address the serialized string
   * @return the deserialized form
   */
  public static ServerAddress parse(String address) {
    String[] split = address.split(":");
    return new ServerAddress(split[0], Integer.parseInt(split[1]));
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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ServerAddress that = (ServerAddress) o;
    return _port == that._port && Objects.equals(_hostname, that._hostname);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_hostname, _port);
  }

  @Override
  public String toString() {
    return _hostname + ":" + _port;
  }
}
