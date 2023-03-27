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

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.pinot.core.transport.ServerInstance;


/**
 * {@code VirtualServer} is a {@link ServerInstance} associated with a
 * unique virtualization identifier which allows the multistage query
 * engine to collocate multiple virtual servers on a single physical
 * instance, enabling higher levels of parallelism and partitioning
 * the query input.
 */
public class VirtualServer {

  private final ServerInstance _server;
  private final Set<Integer> _partitionIds;

  public VirtualServer(ServerInstance server, Collection<Integer> partitionIds) {
    _server = server;
    _partitionIds = new HashSet<>(partitionIds);
  }

  public ServerInstance getServer() {
    return _server;
  }

  public Set<Integer> getPartitionIds() {
    return _partitionIds;
  }

  public String getHostname() {
    return _server.getHostname();
  }

  public int getPort() {
    return _server.getPort();
  }

  public int getQueryMailboxPort() {
    return _server.getQueryMailboxPort();
  }

  public int getQueryServicePort() {
    return _server.getQueryServicePort();
  }

  public int getGrpcPort() {
    return _server.getGrpcPort();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VirtualServer that = (VirtualServer) o;
    return _partitionIds.equals(that._partitionIds) && Objects.equals(_server, that._server);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_server, _partitionIds);
  }

  @Override
  public String toString() {
    return _partitionIds + "@" + _server.getInstanceId();
  }
}
