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
package org.apache.pinot.common.response;

import com.google.common.net.InternetDomainName;
import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.common.utils.EqualityUtils;


/**
 * Service abstraction.
 * A service is identified by its host, port and sequence id.
 */
public class ServerInstance {
  public static final String NAME_PORT_DELIMITER = ":";
  public static final String NAME_PORT_DELIMITER_FOR_INSTANCE_NAME = "_";

  private static class HostNamePair {
    String _hostName;
    String _shortHostName;

    HostNamePair(String hostName, String shortHostName) {
      _hostName = hostName;
      _shortHostName = shortHostName;
    }
  }

  private static final ConcurrentHashMap<String, HostNamePair> HOST_NAME_MAP = new ConcurrentHashMap<>();

  /** Host-name where the service is running **/
  private final String _hostName;

  private final String _shortHostName;

  /** Service Port **/
  private final int _port;

  private final int _seq;

  /**
   * Use this constructor if the name and port are embedded as string with ":" as delimiter
   *
   * Note: This constructor is used primarily for testing.
   */
  public ServerInstance(String namePortPair) {
    this(namePortPair.split(NAME_PORT_DELIMITER)[0], Integer.parseInt(namePortPair.split(NAME_PORT_DELIMITER)[1]));
  }

  public ServerInstance(String name, int port) {
    this(name, port, 0);
  }

  public ServerInstance(String name, int port, int seq) {
    HostNamePair hostNamePair = HOST_NAME_MAP.computeIfAbsent(name, key -> {
      String hostName = getHostName(name);
      String shortHostName = getShortHostName(hostName);
      return new HostNamePair(hostName, shortHostName);
    });
    _hostName = hostNamePair._hostName;
    _shortHostName = hostNamePair._shortHostName;
    _port = port;
    _seq = seq;
  }

  private ServerInstance(String hostName, String shortHostName, int port, int seq) {
    _hostName = hostName;
    _shortHostName = shortHostName;
    _port = port;
    _seq = seq;
  }

  /**
   * Server instance name is formatted as {@code Server_<host>_<port>}
   */
  @Deprecated
  public static ServerInstance forInstanceName(String instanceName) {
    String[] parts = instanceName.split(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)[1]
        .split(NAME_PORT_DELIMITER_FOR_INSTANCE_NAME);
    return new ServerInstance(parts[0], Integer.parseInt(parts[1]));
  }

  /**
   * Name can either be a machine name, or a textual representation of its IP address.
   */
  private static String getHostName(String name) {
    try {
      return InetAddress.getByName(name).getHostName();
    } catch (Exception e) {
      return name;
    }
  }

  /**
   * As per <a href="https://tools.ietf.org/html/rfc952">RFC-952</a> domain names should begin with a letter.
   * That said, <a href="https://tools.ietf.org/html/rfc1123#page-13">RFC-1123</a> updated it say that it may also begin
   * with a digit. Indeed, <a href="http://9292.nl/">this</a> is a valid domain name. Only the top-level domain (i.e. the
   * last portion) has to be non-numeric. More clarification on this matter is in
   * <a href="https://tools.ietf.org/html/rfc3696#section-2">RFC-3696</a>
   *
   * A potentially faster solution is
   *
   * if (first char is a digit) {
   *   it is probably ipv4;
   *   return name;
   * } else {
   *   it could be ipv6 (in which case no dots), or a hostname
   *   return substring before the first dot.
   * }
   *
   * It will fail if there are host names starting with a digit, but will work right otherwise.
   */
  private static String getShortHostName(String hostName) {
    try {
      return InternetDomainName.from(hostName).parts().get(0);
    } catch (Exception e) {
      return hostName;
    }
  }

  public String getHostname() {
    return _hostName;
  }

  public String getShortHostName() {
    return _shortHostName;
  }

  public int getPort() {
    return _port;
  }

  public ServerInstance withSeq(int seq) {
    return new ServerInstance(_hostName, _shortHostName, _port, seq);
  }

  @Override
  public int hashCode() {
    return EqualityUtils.hashCodeOf(EqualityUtils.hashCodeOf(_hostName.hashCode(), _port), _seq);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof ServerInstance) {
      ServerInstance that = (ServerInstance) obj;
      return _hostName.equals(that._hostName) && _port == that._port && _seq == that._seq;
    }
    return false;
  }

  @Override
  public String toString() {
    return _hostName + "_" + _port;
  }
}
