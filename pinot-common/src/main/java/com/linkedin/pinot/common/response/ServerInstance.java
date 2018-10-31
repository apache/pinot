/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.response;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.net.InternetDomainName;
import com.linkedin.pinot.common.utils.CommonConstants;


/**
 * Service abstraction.
 * A service is identified by its hostname and port.
 * Internally, an ip address is also resolved.
 *
 * Nuances:
 * -------
 * A hostname "localhost" will not be resolved to the
 * local hostname and will retain the hostname as "localhost".
 * If the name passed is "127.0.0.1", then it is resolved to the
 * local hostname.
 */
public class ServerInstance implements Comparable<ServerInstance> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(ServerInstance.class);

  public static final String NAME_PORT_DELIMITER = ":";
  public static final String NAME_PORT_DELIMITER_FOR_INSTANCE_NAME = "_";

  /** Host-name where the service is running **/
  private final String _hostname;

  /** Service Port **/
  private final int _port;

  /** IP Address. Not used in equals/hash-code generation **/
  private final InetAddress _ipAddress;

  private final int _seq;

  private final String _shortHostName;

  private static final ConcurrentHashMap<String, Triple<String, String, InetAddress>> nameToInstanceInfo =
      new ConcurrentHashMap<>();

  /**
   * Use this constructor if the name and port are embedded as string with ":" as delimiter
   *
   * @param namePortStr Name and Port settings
   */
  public ServerInstance(String namePortStr) {
    this(namePortStr.split(NAME_PORT_DELIMITER)[0], Integer.parseInt(namePortStr.split(NAME_PORT_DELIMITER)[1]));
  }

  public ServerInstance(String name, int port) {
    this(name, port, 0);
  }

  public ServerInstance(String name, int port, int seq) {
    InetAddress ipAddr = null;

    try {
      ipAddr = InetAddress.getByName(name);
    } catch (UnknownHostException e) {
      LOGGER.error("Unable to fetch IpAddresses for host:" + name, e);
      ipAddr = null;
    }

    _ipAddress = ipAddr;
    _hostname = _ipAddress != null ? _ipAddress.getHostName() : name;
    _port = port;
    _seq = seq;
    _shortHostName = makeShortHostName(_hostname);
  }

  private ServerInstance(String hostname, String shortHostName, InetAddress ipAddress, int port, int seq) {
    _hostname = hostname;
    _shortHostName = shortHostName;
    _ipAddress = ipAddress;
    _port = port;
    _seq = seq;
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
  private String makeShortHostName(final String name) {
    try {
      InternetDomainName domainName = InternetDomainName.from(name);
      return domainName.parts().get(0);
    } catch (Exception e) {
      return name;
    }
  }

  public String getShortHostName() {
    return _shortHostName;
  }

  public String getHostname() {
    return _hostname;
  }

  public int getPort() {
    return _port;
  }

  public InetAddress getIpAddress() {
    return _ipAddress;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + (_hostname == null ? 0 : _hostname.hashCode());
    result = (prime * result) + _port;
    result = (prime * result) + _seq;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ServerInstance other = (ServerInstance) obj;
    if (_hostname == null) {
      if (other._hostname != null) {
        return false;
      }
    } else if (!_hostname.equals(other._hostname)) {
      return false;
    }
    if (_port != other._port) {
      return false;
    }
    if (_seq != other._seq) {
      return false;
    }
    return true;
  }

  public ServerInstance withSeq(int seq) {
    return new ServerInstance(_hostname, _shortHostName, _ipAddress, _port, seq);
  }

  @Override
  public String toString() {
    return _hostname + "_" + _port;
  }

  @Override
  public int compareTo(ServerInstance o) {
    return this.toString().compareTo(o.toString());
  }

  public static ServerInstance forHostPort(String name, int port) {
    if (nameToInstanceInfo.containsKey(name)) {
      Triple<String, String, InetAddress> instanceInfo = nameToInstanceInfo.get(name);
      return new ServerInstance(instanceInfo.getLeft(), instanceInfo.getMiddle(), instanceInfo.getRight(), port, 0);
    } else {
      ServerInstance newInstance = new ServerInstance(name, port);

      nameToInstanceInfo.putIfAbsent(name,
          Triple.of(newInstance.getHostname(), newInstance.getShortHostName(), newInstance.getIpAddress()));

      return newInstance;
    }
  }

  public static ServerInstance forInstanceName(String instanceName) {
    String namePortStr = instanceName.split(CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE)[1];
    String hostName = namePortStr.split(NAME_PORT_DELIMITER_FOR_INSTANCE_NAME)[0];
    int port;
    try {
      port = Integer.parseInt(namePortStr.split(NAME_PORT_DELIMITER_FOR_INSTANCE_NAME)[1]);
    } catch (Exception e) {
      port = CommonConstants.Helix.DEFAULT_SERVER_NETTY_PORT;
    }
    return forHostPort(hostName, port);
  }
}
