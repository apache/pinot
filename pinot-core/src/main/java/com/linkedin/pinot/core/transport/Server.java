/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.transport;

import com.google.common.net.InternetDomainName;
import javax.annotation.concurrent.ThreadSafe;

import static com.linkedin.pinot.common.utils.CommonConstants.Helix.*;


/**
 * The {@code Server} class represents the routing target instance which contains the information of host name, port,
 * and table type it serves.
 * <p>Different table types on same host and port are counted as different instances. Therefore, one single Pinot Server
 * might be treated as two different routing target instances based on the types of table it serves.
 */
@ThreadSafe
public class Server {
  private static final String NAME_PORT_DELIMITER = "_";
  private static final String SHORT_OFFLINE_SUFFIX = "_O";
  private static final String SHORT_REALTIME_SUFFIX = "_R";

  private final String _hostName;
  private final int _port;
  private final TableType _tableType;

  private transient String _shortName;

  /**
   * NOTE: server instance name is of format: {@code Server_<hostName>_<port>}, e.g. {@code Server_localhost_12345}.
   */
  public Server(String instanceName, TableType tableType) {
    String[] hostNameAndPort = instanceName.split(PREFIX_OF_SERVER_INSTANCE)[1].split(NAME_PORT_DELIMITER);
    _hostName = hostNameAndPort[0];
    _port = Integer.parseInt(hostNameAndPort[1]);
    _tableType = tableType;
  }

  public String getHostName() {
    return _hostName;
  }

  public int getPort() {
    return _port;
  }

  public TableType getTableType() {
    return _tableType;
  }

  public synchronized String getShortName() {
    if (_shortName == null) {
      String shortHostName;
      try {
        InternetDomainName domainName = InternetDomainName.from(_hostName);
        shortHostName = domainName.parts().get(0);
      } catch (Exception e) {
        shortHostName = _hostName;
      }
      if (_tableType == TableType.OFFLINE) {
        _shortName = shortHostName + SHORT_OFFLINE_SUFFIX;
      } else {
        _shortName = shortHostName + SHORT_REALTIME_SUFFIX;
      }
    }
    return _shortName;
  }

  @Override
  public int hashCode() {
    return 31 * 31 * _hostName.hashCode() + 31 * _port + _tableType.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof Server) {
      Server that = (Server) obj;
      return _hostName.equals(that._hostName) && _port == that._port && _tableType == that._tableType;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s:%s_%s", _hostName, _port, _tableType);
  }
}
