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

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.pinot.core.transport.ServerInstance;


/// `QueryServerInstance` is representation used during query dispatch to indicate the
/// physical location of a query server.
///
/// Note that `QueryServerInstance` should only be used during dispatch.
public class QueryServerInstance {
  private final String _instanceId;
  private final String _hostname;
  private final int _queryServicePort;
  private final int _queryMailboxPort;
  private final long _instanceIdHash;

  public QueryServerInstance(ServerInstance server) {
    this(server.getInstanceId(), server.getHostname(), server.getQueryServicePort(), server.getQueryMailboxPort());
  }

  public QueryServerInstance(String instanceId, String hostName, int servicePort, int mailboxPort) {
    _instanceId = instanceId;
    _hostname = hostName;
    _queryServicePort = servicePort;
    _queryMailboxPort = mailboxPort;
    _instanceIdHash = hashInstanceId(instanceId);
  }

  /// Hashes an instance id into the value that mailbox plans and streams carry to say which instance they are meant
  /// for. Plans carry the hash rather than the id because a plan repeats the receivers of a stage for every sender
  /// worker, and an id is about as long as a hostname.
  ///
  /// Brokers and servers of different versions compare hashes, so the function must never change: it is FarmHash
  /// Fingerprint64, which Guava guarantees to be stable. Never returns 0, which stands for an unknown instance.
  public static long hashInstanceId(String instanceId) {
    long hash = Hashing.farmHashFingerprint64().hashString(instanceId, StandardCharsets.UTF_8).asLong();
    return hash != 0 ? hash : 1;
  }

  public String getInstanceId() {
    return _instanceId;
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

  /// Returns the hash of the instance id. See [#hashInstanceId(String)].
  public long getInstanceIdHash() {
    return _instanceIdHash;
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
