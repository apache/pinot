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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MetadataCatalog;
import org.apache.pinot.coordination.ratis.metadata.MetadataCatalog.SnapshotData;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;


/// Serial deterministic catalog application with coherent read/snapshot capture. Disk serialization runs outside the
/// catalog monitor; a separate lock excludes snapshot writes from installation and lifecycle changes.
final class MetadataStateMachine extends BaseStateMachine {
  private final MetadataCatalog _catalog;
  private final MetadataSnapshotStorage _storage;
  private final Object _snapshotLock = new Object();

  MetadataStateMachine(UUID clusterId, UUID historyId) {
    _catalog = new MetadataCatalog(clusterId, historyId);
    _storage = new MetadataSnapshotStorage(clusterId, historyId);
  }

  @Override
  public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage)
      throws IOException {
    super.initialize(server, groupId, storage);
    getLifeCycle().transition(LifeCycle.State.STARTING);
    _storage.init(storage);
    restore();
    getLifeCycle().transition(LifeCycle.State.RUNNING);
  }

  private synchronized void restore()
      throws IOException {
    MetadataSnapshotStorage.SnapshotImage snapshot = _storage.load();
    if (snapshot != null) {
      JsonNode appliedIndex = snapshot._state.path("lastAppliedIndex");
      if (!appliedIndex.isIntegralNumber() || !appliedIndex.canConvertToLong() || appliedIndex.longValue() < 0
          || appliedIndex.longValue() > snapshot._info.getIndex()) {
        throw new IOException("Catalog position exceeds the installed Raft snapshot position");
      }
      _catalog.restoreSnapshot(snapshot._state);
      setLastAppliedTermIndex(snapshot._info.getTermIndex());
      _storage.publishLoaded(snapshot);
    }
  }

  @Override
  public void pause() {
    synchronized (_snapshotLock) {
      synchronized (this) {
        if (getLifeCycleState() == LifeCycle.State.PAUSED) {
          return;
        }
        getLifeCycle().transition(LifeCycle.State.PAUSING);
        try {
          _storage.beginInstall(getLastAppliedTermIndex().getIndex());
        } catch (IOException e) {
          throw new UncheckedIOException("Cannot fence interrupted snapshot installation", e);
        }
        getLifeCycle().transition(LifeCycle.State.PAUSED);
      }
    }
  }

  @Override
  public void reinitialize()
      throws IOException {
    synchronized (_snapshotLock) {
      getLifeCycle().transition(LifeCycle.State.STARTING);
      restore();
      getLifeCycle().transition(LifeCycle.State.RUNNING);
      synchronized (this) {
        notifyAll();
      }
    }
  }

  @Override
  public TransactionContext startTransaction(RaftClientRequest request)
      throws IOException {
    MetadataProtocol.decodeCommand(request.getMessage().getContent().toByteArray());
    return super.startTransaction(request);
  }

  @Override
  public synchronized CompletableFuture<Message> applyTransaction(TransactionContext transaction) {
    LogEntryProto entry = transaction.getLogEntry();
    try {
      // Ratis can pause installation after its updater has fetched one older committed entry. That single updater
      // also performs reinitialize(), so the old entry finishes before the snapshot replaces the catalog.
      if (getLifeCycleState() != LifeCycle.State.RUNNING && getLifeCycleState() != LifeCycle.State.PAUSED) {
        throw new IllegalStateException("Cannot apply metadata in " + getLifeCycleState());
      }
      byte[] response = _catalog.execute(entry.getStateMachineLogEntry().getLogData().toByteArray(), entry.getTerm(),
          entry.getIndex());
      updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
      notifyAll();
      return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(response)));
    } catch (IOException e) {
      // Malformed committed state is a fatal replay error, not a recoverable application conflict. The catalog
      // represents expected conflicts in its result instead of throwing; never skip a broken committed command.
      throw new UncheckedIOException("Cannot apply committed metadata at index " + entry.getIndex(), e);
    }
  }

  @Override
  public synchronized void notifyTermIndexUpdated(long term, long index) {
    super.notifyTermIndexUpdated(term, index);
    notifyAll();
  }

  /// Committed local clock state for the leader driver. The driver independently checks Ratis leadership and term;
  /// this accessor is not an externally authorized or linearizable read.
  synchronized ControlState membershipControl() {
    requireRunning();
    return _catalog.membershipControl();
  }

  /// Captures on this voter only after it reaches a leader-confirmed revision. Capture and the readiness check share
  /// the apply monitor, so installation cannot replace the catalog between the check and capture.
  synchronized CatalogImage captureImage(FeedScope scope, int maxRecords, int maxBytes, long minimumIndex,
      long deadlineNanos)
      throws IOException {
    awaitApplied(minimumIndex, deadlineNanos);
    return _catalog.captureImage(scope, maxRecords, maxBytes);
  }

  synchronized ChangePage changes(FeedScope scope, FeedCursor cursor, int maxTransactions, int maxRecords,
      int maxBytes, long minimumIndex, long deadlineNanos)
      throws IOException {
    awaitApplied(minimumIndex, deadlineNanos);
    return _catalog.changes(scope, cursor, maxTransactions, maxRecords, maxBytes);
  }

  private void awaitApplied(long minimumIndex, long deadlineNanos)
      throws IOException {
    while (true) {
      LifeCycle.State state = getLifeCycleState();
      if (state == LifeCycle.State.CLOSING || state == LifeCycle.State.CLOSED || state == LifeCycle.State.EXCEPTION) {
        throw new IOException("Metadata voter is unavailable: " + state);
      }
      long remaining = deadlineNanos - System.nanoTime();
      if (remaining <= 0) {
        throw new IOException("Timed out waiting for the local metadata voter to reach revision " + minimumIndex);
      }
      if (state == LifeCycle.State.RUNNING && getLastAppliedTermIndex().getIndex() >= minimumIndex) {
        return;
      }
      try {
        TimeUnit.NANOSECONDS.timedWait(this, remaining);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted waiting for the local metadata voter", e);
      }
    }
  }

  @Override
  public CompletableFuture<Message> query(Message request) {
    // Do not expose a bypass through native read-only or stale-read RPCs. Metadata clients submit ReadBarrier
    // commands; local image capture separately waits for the resulting committed revision.
    return CompletableFuture.failedFuture(
        new StateMachineException("Metadata reads require a committed ReadBarrier command", false));
  }

  @Override
  public long takeSnapshot()
      throws IOException {
    synchronized (_snapshotLock) {
      SnapshotData state;
      TermIndex position;
      synchronized (this) {
        // Installation may pause the state machine after Ratis decides a snapshot is due. The updater must be free
        // to continue to reinitialize(), so decline this snapshot without blocking or failing the voter.
        if (getLifeCycleState() != LifeCycle.State.RUNNING) {
          return -1;
        }
        position = getLastAppliedTermIndex();
        if (position.getIndex() < 0) {
          return -1;
        }
        state = _catalog.captureSnapshotData();
      }
      return _storage.write(state.toJson(), position);
    }
  }

  @Override
  public StateMachineStorage getStateMachineStorage() {
    return _storage;
  }

  private void requireRunning() {
    if (getLifeCycleState() != LifeCycle.State.RUNNING) {
      throw new IllegalStateException("Metadata state machine is not running: " + getLifeCycleState());
    }
  }

  @Override
  public void close() {
    synchronized (_snapshotLock) {
      synchronized (this) {
        getLifeCycle().checkStateAndClose();
        notifyAll();
      }
    }
  }
}
