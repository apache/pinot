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
import org.apache.pinot.core.transport.ServerInstance;


/**
 * {@code QueryServerInstance} is representation used during query dispatch to indicate the
 * physical location of a query server.
 *
 * <p>Note that {@code QueryServerInstance} should only be used during dispatch.</p>
 */
public class QueryServerInstance {
  private final String _hostname;
  private final int _queryServicePort;
  private final int _queryMailboxPort;

  public QueryServerInstance(ServerInstance server) {
    this(server.getHostname(), server.getQueryServicePort(), server.getQueryMailboxPort());
  }

  public QueryServerInstance(String hostName, int servicePort, int mailboxPort) {
    _hostname = hostName;
    _queryServicePort = servicePort;
    _queryMailboxPort = mailboxPort;
  }

  public String getHostname() {
    return _hostname;
  }

  public int getQueryMailboxPort() {
    return _queryMailboxPort;
  }

  public int getQueryServicePort() {
    return _queryServicePort;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryServerInstance that = (QueryServerInstance) o;
    return _hostname.equals(that._hostname) && _queryServicePort == that._queryServicePort
        && _queryMailboxPort == that._queryMailboxPort;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_hostname, _queryServicePort, _queryMailboxPort);
  }

  @Override
  public String toString() {
    return _hostname + "@{" + _queryServicePort + "," + _queryMailboxPort + "}";
  }
}
