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
package org.apache.pinot.server.starter;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Authority;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Installation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ReadGuard;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Request;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Result;
import org.apache.pinot.common.config.provider.RecoverableClusterConfigCache;
import org.apache.pinot.common.coordination.NativeOfflineSegmentWorker;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.apache.pinot.segment.local.utils.SegmentOperationsThrottlerSet;
import org.apache.pinot.segment.local.utils.ServerReloadJobStatusCache;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;


/// Owns a native SERVER registration, independent heartbeat lane, scoped metadata image and bounded installation
/// workers. Durable operation discovery makes lost local wake-ups harmless. This runtime deliberately grants no
/// query readiness: the normal query paths still require serving-permit and per-content admission integration.
public final class NativeServerParticipant implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeServerParticipant.class);

  /// Channels are owned by the participant. Membership and work must use separate channel/executor admission.
  public interface Rpc extends InstallationOutbox.Rpc, AutoCloseable {
    MetadataRpc.MembershipResponse membershipRead(MetadataRpc.MembershipReadRequest request, long timeoutMillis)
        throws Exception;

    MetadataRpc.MembershipResponse membershipExecute(MetadataRpc.MembershipCommandRequest request, long timeoutMillis)
        throws Exception;

    MetadataRpc.InstallationSnapshot snapshot(MetadataRpc.InstallationSnapshotRequest request, long timeoutMillis)
        throws Exception;

    MetadataRpc.InstallationWorkResult lookup(MetadataRpc.InstallationLookupRequest request, long timeoutMillis)
        throws Exception;

    @Override
    void close();
  }

  public record Settings(UUID historyId, String participantId, Map<String, String> endpoints, long rpcTimeoutMillis,
                         long heartbeatMillis, long snapshotMillis, int actionThreads, int queuedActions) {
    public Settings {
      requireNonNull(historyId, "historyId");
      checkArgument(participantId != null && !participantId.isBlank() && participantId.length() <= 128,
          "A stable SERVER participant ID is required");
      endpoints = Map.copyOf(endpoints);
      checkArgument(rpcTimeoutMillis > 0 && rpcTimeoutMillis <= 30_000 && heartbeatMillis > 0
          && snapshotMillis > 0 && actionThreads > 0 && actionThreads <= 16 && queuedActions > 0
          && queuedActions <= 4096, "Invalid bounded native participant configuration");
    }
  }

  private final Settings _settings;
  private final Rpc _membership;
  private final Rpc _work;
  private final UUID _incarnation = UUID.randomUUID();
  private final NativeInstanceDataManager _manager = new NativeInstanceDataManager();
  private final RecoverableClusterConfigCache _clusterConfig = new RecoverableClusterConfigCache();
  private final ScheduledExecutorService _heartbeats = Executors.newSingleThreadScheduledExecutor(
      runnable -> new Thread(runnable, "native-server-membership"));
  private final ScheduledExecutorService _snapshots = Executors.newSingleThreadScheduledExecutor(
      runnable -> new Thread(runnable, "native-server-metadata"));
  private final ThreadPoolExecutor _actions;
  private final Set<UUID> _queued = ConcurrentHashMap.newKeySet();
  private final AtomicBoolean _closed = new AtomicBoolean();
  private final ReentrantReadWriteLock _admission = new ReentrantReadWriteLock();
  @Nullable
  private volatile MetadataRpc.ParticipantToken _participant;
  @Nullable
  private volatile NativeServerMetadata _metadata;
  @Nullable
  private volatile NativeOfflineSegmentWorker _worker;
  @Nullable
  private volatile InstallationOutbox _outbox;
  @Nullable
  private volatile String _failure;
  private volatile boolean _hydrated;
  private volatile boolean _membershipHealthy;
  private volatile boolean _fenced;
  private boolean _started;
  private long _nextRenewal = 1;
  @Nullable
  private MetadataRpc.MembershipCommandRequest _pendingRenewal;

  public NativeServerParticipant(Settings settings, Rpc membership, Rpc work) {
    _settings = requireNonNull(settings, "settings");
    _membership = requireNonNull(membership, "membership");
    _work = requireNonNull(work, "work");
    checkArgument(membership != work, "Membership must have independent channel admission");
    _actions = new ThreadPoolExecutor(settings.actionThreads(), settings.actionThreads(), 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(settings.queuedActions()), runnable -> new Thread(runnable, "native-server-action"));
  }

  /// Acquires directory ownership before registering. A failed startup closes all owned resources; a later process
  /// obtains a new incarnation and waits for an unknown old registration to expire instead of replacing live authority.
  public synchronized void start(PinotConfiguration instanceConfig, ServerMetrics metrics,
      @Nullable SegmentOperationsThrottlerSet throttlers, ServerReloadJobStatusCache jobs) throws Exception {
    checkState(!_started && !_closed.get(), "Native participant is already started or closed");
    _started = true;
    try {
      _manager.initWithMetadata(instanceConfig, table -> metadata().provider(table), metrics, throttlers, jobs);
      require(_settings.participantId().equals(_manager.getInstanceId()), "Configured instance ID differs");
      register();
      _metadata = new NativeServerMetadata(participant(), _clusterConfig);
      hydrate();
      MetadataRpc.MembershipCommandRequest ready = command().setReady(MetadataRpc.ParticipantReadiness.newBuilder()
          .setSession(session()).setAppliedRevision(metadata().revision()).setReady(true)).build();
      MetadataRpc.MembershipResponse response = _membership.membershipExecute(ready, _settings.rpcTimeoutMillis());
      validateMembership(response);
      require(response.getStatus() == ResultStatus.OK && response.hasParticipant()
          && response.getParticipant().getParticipant().equals(participant()) && response.getParticipant().getReady(),
          "Native readiness was not committed: " + response.getStatus());
      _manager.start();
      InstallationOutbox outbox = _manager.openInstallationOutbox(participant(), _work, _settings.rpcTimeoutMillis());
      _outbox = outbox;
      _worker = new NativeOfflineSegmentWorker(participant(), this::lookup, outbox::publish);
      membership(true);
      _heartbeats.scheduleWithFixedDelay(this::heartbeat, 0, _settings.heartbeatMillis(), TimeUnit.MILLISECONDS);
      _snapshots.scheduleWithFixedDelay(this::reconcile, 0, _settings.snapshotMillis(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      try {
        close();
      } catch (RuntimeException cleanup) {
        e.addSuppressed(cleanup);
      }
      throw e;
    }
  }

  private void register() throws Exception {
    MetadataRpc.MembershipResponse current = _membership.membershipRead(MetadataRpc.MembershipReadRequest.newBuilder()
        .setHistoryId(_settings.historyId().toString()).build(), _settings.rpcTimeoutMillis());
    validateMembership(current);
    require(current.getStatus() == ResultStatus.OK || current.getStatus() == ResultStatus.NOT_FOUND,
        "Cannot inspect native registration: " + current.getStatus());
    long epoch = 0;
    if (current.hasParticipant()) {
      MetadataRpc.ParticipantToken previous = current.getParticipant().getParticipant();
      validateParticipant(previous);
      epoch = previous.getRegistrationEpoch();
    }
    MetadataRpc.MembershipCommandRequest registration = command().setRegister(MetadataRpc.ParticipantRegistration
        .newBuilder().setIncarnation(_incarnation.toString()).setExpectedEpoch(epoch)
        .putAllEndpoints(_settings.endpoints())).build();
    // Endpoint failover and any caller retry must keep this exact incarnation/expected epoch/endpoints tuple.
    MetadataRpc.MembershipResponse registered = _membership.membershipExecute(registration,
        _settings.rpcTimeoutMillis());
    validateMembership(registered);
    require(registered.getStatus() == ResultStatus.OK && registered.hasParticipant(),
        "Cannot acquire native registration: " + registered.getStatus());
    MetadataRpc.ParticipantToken token = registered.getParticipant().getParticipant();
    validateParticipant(token);
    require(token.getIncarnation().equals(_incarnation.toString()) && token.getRegistrationEpoch() == epoch + 1,
        "Registration response does not match this process");
    // Remember confirmed authority before later startup checks so cleanup can retire this exact registration.
    _participant = token;
    require(registered.getParticipant().getCapabilitiesCount() == 0,
        "Native installation runtime must not be granted query-serving capabilities");
  }

  private void hydrate() throws Exception {
    MetadataRpc.InstallationSnapshot snapshot = _work.snapshot(MetadataRpc.InstallationSnapshotRequest.newBuilder()
        .setHistoryId(_settings.historyId().toString()).setSession(session()).setMaxRecords(4096)
        .setMaxBytes(2 * 1024 * 1024).build(), _settings.rpcTimeoutMillis());
    require(!_closed.get() && !_fenced, "Native participant is closed or fenced");
    if (snapshot.getStatus() == ResultStatus.FENCED || snapshot.getStatus() == ResultStatus.HISTORY_MISMATCH) {
      fence("Server metadata registration was fenced");
    }
    hydration(false);
    metadata().apply(snapshot);
    hydration(true);
  }

  private void reconcile() {
    if (_closed.get() || _fenced) {
      return;
    }
    try {
      hydrate();
    } catch (Exception e) {
      hydration(false);
      failed("metadata hydration", e);
      return;
    }
    if (_membershipHealthy) {
      try {
        // A completed/cancelled operation can disappear from discovery after a lost report or ack response.
        // Recover its persisted stream independently before dispatching more work. Busy recovery retries next pass.
        requireNonNull(_outbox).recover();
        for (UUID operation : metadata().operations()) {
          wake(operation);
        }
      } catch (Exception e) {
        failed("installation outcome recovery", e);
      }
    }
  }

  private void heartbeat() {
    if (_closed.get() || _fenced) {
      return;
    }
    try {
      if (_pendingRenewal == null) {
        _pendingRenewal = command().setRenew(MetadataRpc.ParticipantRenewal.newBuilder().setSession(session())
            .setSequence(_nextRenewal).setAppliedRevision(metadata().revision())).build();
      }
      MetadataRpc.MembershipResponse response = _membership.membershipExecute(_pendingRenewal,
          _settings.rpcTimeoutMillis());
      validateMembership(response);
      if (response.getStatus() == ResultStatus.FENCED || response.getStatus() == ResultStatus.HISTORY_MISMATCH) {
        fence("Native registration expired or history changed");
        return;
      }
      if (response.getStatus() == ResultStatus.NOT_READY) {
        membership(false);
        return;
      }
      require(response.getStatus() == ResultStatus.OK && response.getRenewalsCount() == 1,
          "Renewal response is incomplete: " + response.getStatus());
      MetadataRpc.RenewalResult result = response.getRenewals(0);
      require(result.getParticipant().equals(participant()) && result.getSequence() == _nextRenewal,
          "Renewal response identity differs");
      if (result.getStatus() == ResultStatus.FENCED || result.getStatus() == ResultStatus.HISTORY_MISMATCH) {
        fence("Native registration expired or was replaced");
        return;
      }
      require(result.getStatus() == ResultStatus.OK || result.getStatus() == ResultStatus.NOT_READY,
          "Unexpected durable renewal outcome: " + result.getStatus());
      require(_nextRenewal < Long.MAX_VALUE, "Renewal sequence exhausted");
      _nextRenewal++;
      _pendingRenewal = null;
      membership(result.getStatus() == ResultStatus.OK);
      // Serving permits are deliberately unused while query/content admission is not integrated.
    } catch (Exception e) {
      membership(false);
      failed("membership renewal", e);
    }
  }

  /// A wake-up is only a bounded hint. Lost or rejected hints are recovered by the next committed metadata snapshot.
  public boolean wake(UUID operation) {
    requireNonNull(operation, "operation");
    if (!isReadyForInstallations() || !_queued.add(operation)) {
      return false;
    }
    try {
      _actions.execute(() -> {
        try {
          if (isReadyForInstallations()) {
            requireNonNull(_worker).execute(operation, this::executor);
          }
        } catch (Exception e) {
          failed("installation reconciliation", e);
        } finally {
          _queued.remove(operation);
        }
      });
      return true;
    } catch (RejectedExecutionException e) {
      _queued.remove(operation);
      return false;
    }
  }

  private NativeOfflineSegmentAction.Executor executor(String table) {
    NativeOfflineSegmentAction.Executor delegate = _manager.executor(table);
    return new NativeOfflineSegmentAction.Executor() {
      @Override
      public Result execute(Request request, Authority authority) throws Exception {
        return delegate.execute(request, new Authority() {
          @Override
          public void refresh() throws Exception {
            authority.refresh();
          }

          @Override
          public ReadGuard acquire(Identity expected) throws Exception {
            _admission.readLock().lock();
            ReadGuard work;
            try {
              work = authority.acquire(expected);
            } catch (Exception e) {
              _admission.readLock().unlock();
              throw e;
            }
            return new ReadGuard() {
              @Override
              public boolean isCurrent() {
                return isReadyForInstallations() && metadata().isCurrent(expected) && work.isCurrent();
              }

              @Override
              public void close() {
                try {
                  work.close();
                } finally {
                  _admission.readLock().unlock();
                }
              }
            };
          }
        });
      }

      @Nullable
      @Override
      public Installation installed(String segmentName) throws IOException {
        return delegate.installed(segmentName);
      }
    };
  }

  private void hydration(boolean ready) {
    _admission.writeLock().lock();
    try {
      _hydrated = ready && !_closed.get() && !_fenced;
    } finally {
      _admission.writeLock().unlock();
    }
  }

  private void membership(boolean healthy) {
    _admission.writeLock().lock();
    try {
      _membershipHealthy = healthy && !_closed.get() && !_fenced;
    } finally {
      _admission.writeLock().unlock();
    }
  }

  private MetadataRpc.InstallationWorkResult lookup(UUID operation) throws Exception {
    require(isReadyForInstallations(), "Native installation admission is closed");
    MetadataRpc.InstallationWorkResult result = _work.lookup(MetadataRpc.InstallationLookupRequest.newBuilder()
        .setHistoryId(_settings.historyId().toString()).setSession(session()).setOperationId(operation.toString())
        .build(), _settings.rpcTimeoutMillis());
    require(isReadyForInstallations(), "Native installation admission changed during lookup");
    return result;
  }

  public boolean isReadyForInstallations() {
    return !_closed.get() && !_fenced && _hydrated && _membershipHealthy && _worker != null;
  }

  public NativeInstanceDataManager dataManager() {
    return _manager;
  }

  public RecoverableClusterConfigCache clusterConfig() {
    return _clusterConfig;
  }

  @Nullable
  public String failure() {
    return _failure;
  }

  public MetadataRpc.ParticipantToken participant() {
    return requireNonNull(_participant, "Native participant is not registered");
  }

  private NativeServerMetadata metadata() {
    return requireNonNull(_metadata, "Native server metadata is not initialized");
  }

  private MetadataRpc.ParticipantSession session() {
    MetadataRpc.ParticipantToken token = participant();
    return MetadataRpc.ParticipantSession.newBuilder().setIncarnation(token.getIncarnation())
        .setRegistrationEpoch(token.getRegistrationEpoch()).build();
  }

  private MetadataRpc.MembershipCommandRequest.Builder command() {
    return MetadataRpc.MembershipCommandRequest.newBuilder().setHistoryId(_settings.historyId().toString());
  }

  private void validateMembership(MetadataRpc.MembershipResponse response) throws IOException {
    NativeOfflineSegmentWorker.validateMessage(response);
    require(response.hasRevision() && response.getRevision().getHistoryId().equals(_settings.historyId().toString())
        && response.getRevision().getIndex() >= 0, "Membership response history differs");
  }

  private void validateParticipant(MetadataRpc.ParticipantToken token) throws IOException {
    require(token.getHistoryId().equals(_settings.historyId().toString())
        && token.getParticipantId().equals(_settings.participantId())
        && token.getRole() == MetadataRpc.ParticipantRole.SERVER
        && token.getRegistrationEpoch() > 0, "Membership participant identity differs");
  }

  private void fence(String detail) {
    _admission.writeLock().lock();
    try {
      _fenced = true;
      _hydrated = false;
      _membershipHealthy = false;
      _failure = detail;
    } finally {
      _admission.writeLock().unlock();
    }
    metadata().invalidate();
  }

  private void failed(String operation, Exception e) {
    String failure = operation + ": " + e.getClass().getSimpleName() + ": " + e.getMessage();
    if (!failure.equals(_failure)) {
      _failure = failure;
      LOGGER.warn("Native server {} failed", operation, e);
    }
  }

  @Override
  public synchronized void close() {
    _admission.writeLock().lock();
    try {
      if (!_closed.compareAndSet(false, true)) {
        return;
      }
      _hydrated = false;
      _membershipHealthy = false;
    } finally {
      _admission.writeLock().unlock();
    }
    _heartbeats.shutdownNow();
    _snapshots.shutdownNow();
    _actions.shutdownNow();
    if (_participant != null) {
      try {
        _membership.membershipExecute(command().setDrain(session()).build(), _settings.rpcTimeoutMillis());
      } catch (Exception e) {
        failed("drain", e);
      }
    }
    try {
      _manager.shutDown();
      if (_participant != null) {
        try {
          _membership.membershipExecute(command().setUnregister(session()).build(), _settings.rpcTimeoutMillis());
        } catch (Exception e) {
          failed("unregister", e);
        }
      }
    } finally {
      try {
        _work.close();
      } finally {
        _membership.close();
      }
    }
  }

  private static void require(boolean condition, String detail) throws IOException {
    if (!condition) {
      throw new IOException(detail);
    }
  }
}
