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
package org.apache.pinot.coordination.ratis;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetInstallationWork;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRevision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetStreamTip;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipQuery;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Query;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.StreamTipResult;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineInstallationProtocol.InstallationSnapshot;
import org.apache.pinot.coordination.ratis.metadata.OfflineRoutingProtocol.RoutingSnapshot;
import org.apache.pinot.coordination.ratis.metadata.TaskProtocol.WorkSnapshot;
import org.apache.pinot.coordination.ratis.transport.InstallationAccess;
import org.apache.pinot.coordination.ratis.transport.MembershipAccess;
import org.apache.pinot.coordination.ratis.transport.MetadataAccess;
import org.apache.pinot.coordination.ratis.transport.RoutingAccess;
import org.apache.pinot.coordination.ratis.transport.TaskAccess;


/// Controller-side metadata access for an authenticated service. Reads establish a consensus barrier before a local
/// coherent capture; no full catalog image crosses the Raft client transport. The enclosing service authorizes actors.
/// This class is thread-safe and owns its clients, but does not own the voter.
public final class RatisMetadataAccess
    implements MetadataAccess, MembershipAccess, InstallationAccess, TaskAccess, RoutingAccess, AutoCloseable {
  private final MetadataStateMachine _stateMachine;
  private final RatisMetadataStore.Client _client;
  private final RatisMetadataStore.Client _membershipClient;
  private final UUID _historyId;
  private final long _requestTimeoutMillis;

  RatisMetadataAccess(MetadataStateMachine stateMachine, RatisMetadataStore.Client client,
      RatisMetadataStore.Client membershipClient, UUID historyId, long requestTimeoutMillis) {
    _stateMachine = stateMachine;
    _client = client;
    _membershipClient = membershipClient;
    _historyId = historyId;
    _requestTimeoutMillis = requestTimeoutMillis;
  }

  @Override
  public Result execute(Command command, long timeoutMillis)
      throws IOException {
    if (command instanceof MembershipCommand) {
      throw new IOException("Membership commands require the typed membership access method");
    }
    return MetadataProtocol.decodeResult(_client.execute(MetadataProtocol.encode(command), timeoutMillis));
  }

  @Override
  public Result query(Query query, long timeoutMillis)
      throws IOException {
    if (query instanceof MembershipQuery || query instanceof GetInstallationWork || query instanceof GetStreamTip) {
      throw new IOException("Membership, installation work and stream queries require their typed access method");
    }
    return MetadataProtocol.decodeResult(_client.query(MetadataProtocol.encode(query), timeoutMillis));
  }

  @Override
  public StreamTipResult query(GetStreamTip query, long timeoutMillis)
      throws IOException {
    return MetadataProtocol.decodeStreamTipResult(_client.query(MetadataProtocol.encode(query), timeoutMillis));
  }

  @Override
  public MembershipProtocol.Response execute(MembershipCommand command, long timeoutMillis)
      throws IOException {
    return MembershipProtocol.decodeResponse(
        _membershipClient.execute(MetadataProtocol.encode(command), timeoutMillis));
  }

  @Override
  public OfflineInstallationProtocol.WorkResult query(GetInstallationWork query, long timeoutMillis)
      throws IOException {
    return OfflineInstallationProtocol.decodeWorkResult(
        _client.query(MetadataProtocol.encode(query), timeoutMillis));
  }

  @Override
  public MembershipProtocol.Response query(MembershipQuery query, long timeoutMillis)
      throws IOException {
    return MembershipProtocol.decodeResponse(_membershipClient.query(MetadataProtocol.encode(query), timeoutMillis));
  }

  @Override
  public CatalogImage image(FeedScope scope, int maxRecords, int maxBytes, long timeoutMillis)
      throws IOException {
    long deadline = deadline(timeoutMillis);
    long index = readBarrier(deadline);
    return _stateMachine.captureImage(scope, maxRecords, maxBytes, index, deadline);
  }

  @Override
  public InstallationSnapshot installationSnapshot(ParticipantToken server, int maxRecords, int maxBytes,
      long timeoutMillis)
      throws IOException {
    long deadline = deadline(timeoutMillis);
    long index = readBarrier(deadline);
    return _stateMachine.installationSnapshot(server, maxRecords, maxBytes, index, deadline);
  }

  @Override
  public RoutingSnapshot routingSnapshot(ParticipantToken broker, String tableName, int maxRecords, int maxBytes,
      long timeoutMillis) throws IOException {
    long deadline = deadline(timeoutMillis);
    long index = readBarrier(deadline);
    return _stateMachine.routingSnapshot(broker, tableName, maxRecords, maxBytes, index, deadline);
  }

  @Override
  public WorkSnapshot taskSnapshot(ParticipantToken minion, int maxRecords, int maxBytes, long timeoutMillis)
      throws IOException {
    long deadline = deadline(timeoutMillis);
    long index = readBarrier(deadline);
    return _stateMachine.taskSnapshot(minion, maxRecords, maxBytes, index, deadline);
  }

  @Override
  public ChangePage changes(FeedScope scope, FeedCursor cursor, int maxTransactions, int maxRecords, int maxBytes,
      long timeoutMillis)
      throws IOException {
    long deadline = deadline(timeoutMillis);
    long index = readBarrier(deadline);
    return _stateMachine.changes(scope, cursor, maxTransactions, maxRecords, maxBytes, index, deadline);
  }

  private long readBarrier(long deadline)
      throws IOException {
    long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
    Result result = query(new GetRevision(_historyId), remaining);
    if (result.status() != Status.OK || result.revision() == null
        || !_historyId.equals(result.revision().historyId()) || result.revision().index() < 0) {
      throw new IOException("Could not establish metadata read barrier: " + result.status());
    }
    return result.revision().index();
  }

  private long deadline(long timeoutMillis)
      throws IOException {
    if (timeoutMillis <= 0) {
      throw new IOException("Metadata request deadline has expired");
    }
    return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(Math.min(timeoutMillis, _requestTimeoutMillis));
  }

  @Override
  public void close()
      throws IOException {
    try {
      _membershipClient.close();
    } finally {
      _client.close();
    }
  }
}
