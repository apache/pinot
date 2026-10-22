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
import java.util.List;
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
import org.apache.pinot.common.coordination.QueryDeadlineGuard;
import org.apache.pinot.common.coordination.ServingPermitGuard;
import org.apache.pinot.common.coordination.ServingPermitGuard.MembershipView;
import org.apache.pinot.common.coordination.ServingPermitGuard.MonotonicClock;
import org.apache.pinot.common.coordination.ServingPermitGuard.QualifiedLifetimePolicy;
import org.apache.pinot.common.coordination.ServingPermitGuard.QueryTicket;
import org.apache.pinot.common.coordination.ServingPermitGuard.ReadOnlyCapability;
import org.apache.pinot.common.coordination.ServingPermitGuard.ServingDeniedException;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.request.context.NativeQueryContext;
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
/// workers. Durable operation discovery makes lost local wake-ups harmless. Explicit clock qualification enables
/// local serving-permit and absolute-deadline checks, but does not start query listeners: transport authentication,
/// broker deadline propagation and delivery integration remain separate activation requirements.
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

  /// Explicit deployment-qualified serving inputs. There are no clock or lifetime defaults. Supplying these inputs
  /// opts into SERVER_QUERY grants; it does not qualify the platform or enable any network query resource.
  public record ServingSettings(MonotonicClock clock, QualifiedLifetimePolicy lifetimePolicy,
                                QueryDeadlineGuard queryDeadlines) {
    public ServingSettings {
      requireNonNull(clock, "clock");
      requireNonNull(lifetimePolicy, "lifetimePolicy");
      requireNonNull(queryDeadlines, "queryDeadlines");
    }
  }

  /// One local query admission, bound to its immutable attempt identity and original finite serving ticket.
  /// Callers must still authenticate the broker and pin exact segment handles. The original end-to-end deadline
  /// is retained by the ticket and cannot be extended by a renewed serving permit.
  public static final class QueryAdmission {
    private final NativeQueryContext _context;
    private final QueryTicket _ticket;

    private QueryAdmission(NativeQueryContext context, QueryTicket ticket) {
      _context = context;
      _ticket = ticket;
    }

    public NativeQueryContext context() {
      return _context;
    }
  }

  private final Settings _settings;
  private final Rpc _membership;
  private final Rpc _work;
  @Nullable
  private final ServingSettings _servingSettings;
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
  private volatile ServingPermitGuard _serving;
  @Nullable
  private volatile String _failure;
  private volatile boolean _hydrated;
  private volatile boolean _membershipHealthy;
  private volatile boolean _fenced;
  private volatile long _minimumServingRevision;
  private boolean _started;
  private long _nextRenewal = 1;
  @Nullable
  private MetadataRpc.MembershipCommandRequest _pendingRenewal;

  public NativeServerParticipant(Settings settings, Rpc membership, Rpc work) {
    this(settings, membership, work, null);
  }

  public NativeServerParticipant(Settings settings, Rpc membership, Rpc work,
      @Nullable ServingSettings servingSettings) {
    _settings = requireNonNull(settings, "settings");
    _membership = requireNonNull(membership, "membership");
    _work = requireNonNull(work, "work");
    _servingSettings = servingSettings;
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
      _metadata = new NativeServerMetadata(participant(), _clusterConfig, allowedCapabilities());
      if (_servingSettings != null) {
        _serving = new ServingPermitGuard(participant(), allowedCapabilities(), _servingSettings.clock(),
            _servingSettings.lifetimePolicy(), this::servingView);
      }
      hydrate();
      MetadataRpc.MembershipCommandRequest ready = command().setReady(MetadataRpc.ParticipantReadiness.newBuilder()
          .setSession(session()).setAppliedRevision(metadata().revision()).setReady(true)).build();
      MetadataRpc.MembershipResponse response = _membership.membershipExecute(ready, _settings.rpcTimeoutMillis());
      validateMembership(response);
      require(response.getStatus() == ResultStatus.OK && response.hasParticipant()
          && response.getParticipant().getParticipant().equals(participant()) && response.getParticipant().getReady(),
          "Native readiness was not committed: " + response.getStatus());
      // Publish readiness only from a coherent committed image, never by patching the bootstrap projection.
      hydrate();
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
    require(registered.getParticipant().getCapabilitiesCount() <= allowedCapabilities().size()
        && registered.getParticipant().getCapabilitiesList().stream()
        .allMatch(capability -> capability == MetadataRpc.ServingCapability.SERVER_QUERY
            && _servingSettings != null), "Native runtime received unconfigured query-serving capabilities");
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
      if (_serving != null) {
        // Retransmissions retain this logical sequence's first-send time, including after an unknown outcome.
        _serving.firstSend(_nextRenewal);
      }
      MetadataRpc.MembershipResponse response = _membership.membershipExecute(_pendingRenewal,
          _settings.rpcTimeoutMillis());
      validateMembership(response);
      if (_serving != null) {
        // Even a replayed old grant can carry newer control/policy state in its response. A known no-grant outcome
        // may reflect drain or readiness changes. Require a coherent image through all newly observed information.
        _admission.writeLock().lock();
        try {
          _minimumServingRevision = Math.max(_minimumServingRevision, response.getRevision().getIndex());
        } finally {
          _admission.writeLock().unlock();
        }
      }
      if (response.getStatus() == ResultStatus.FENCED || response.getStatus() == ResultStatus.HISTORY_MISMATCH) {
        fence("Native registration expired or history changed");
        return;
      }
      if (response.getStatus() == ResultStatus.NOT_READY) {
        membership(false);
        return;
      }
      if (response.getStatus() != ResultStatus.OK || response.getRenewalsCount() != 1) {
        fence("Native renewal was rejected or incomplete: " + response.getStatus());
        return;
      }
      MetadataRpc.RenewalResult result = response.getRenewals(0);
      if (!result.getParticipant().equals(participant()) || result.getSequence() != _nextRenewal) {
        fence("Native renewal response identity differs");
        return;
      }
      if (result.getStatus() == ResultStatus.FENCED || result.getStatus() == ResultStatus.HISTORY_MISMATCH) {
        fence("Native registration expired or was replaced");
        return;
      }
      if (result.getStatus() != ResultStatus.OK && result.getStatus() != ResultStatus.NOT_READY
          || result.getStatus() != ResultStatus.OK && result.hasPermit()) {
        // A definite semantic rejection (including conflict after ACL narrowing) cannot become a grant on retry.
        fence("Native renewal has an unusable durable outcome: " + result.getStatus());
        return;
      }
      if (_serving != null && result.hasPermit()) {
        _serving.accept(result.getPermit());
      }
      if (_nextRenewal == Long.MAX_VALUE) {
        fence("Native renewal sequence exhausted");
        return;
      }
      _nextRenewal++;
      _pendingRenewal = null;
      membership(result.getStatus() == ResultStatus.OK);
    } catch (ServingDeniedException e) {
      // A lost clock qualification or invalid grant must not strand the renewal sequence in a retry loop or revive
      // old tickets. This registration is terminal; a new process registration must re-establish qualification.
      fence("Native serving authority failed: " + e.getMessage());
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
    if (_closed.get() || _fenced || !_hydrated || !_membershipHealthy || _worker == null) {
      return false;
    }
    try {
      MembershipView view = metadata().membershipView();
      return view.live() && view.ready() && !view.draining();
    } catch (IllegalStateException e) {
      return false;
    }
  }

  /// Local admission only. The metadata predicate checks the current desired content; physical acquisition must
  /// independently pin and compare installed identities before execution. No metadata RPC is performed here.
  public QueryAdmission admitQuery(NativeQueryContext context) {
    requireNonNull(context, "context");
    checkState(context.version() == NativeQueryContext.DEADLINE_VERSION && context.deadline() != null,
        "Native query admission requires an immutable end-to-end deadline");
    _admission.readLock().lock();
    try {
      ServingPermitGuard guard = serving();
      QueryTicket ticket = guard.admit(ReadOnlyCapability.SERVER_QUERY, context.deadline(),
          requireNonNull(_servingSettings).queryDeadlines());
      checkState(metadata().isCurrentQuery(context), "Native query identity differs from current metadata");
      guard.checkDelivery(ticket);
      return new QueryAdmission(context, ticket);
    } finally {
      _admission.readLock().unlock();
    }
  }

  /// Check after resume and immediately before each result delivery. A held reader may finish after desired
  /// content changes, but its original ticket, membership, capability and control fences still apply.
  public void checkQueryDelivery(QueryAdmission admission) {
    requireNonNull(admission, "admission");
    _admission.readLock().lock();
    try {
      serving().checkDelivery(admission._ticket);
    } finally {
      _admission.readLock().unlock();
    }
  }

  private ServingPermitGuard serving() {
    checkState(_serving != null, "Native query serving requires explicit qualified clock inputs");
    return _serving;
  }

  private MembershipView servingView() {
    checkState(!_closed.get() && !_fenced && _hydrated, "Native serving metadata is unavailable");
    MembershipView view = metadata().membershipView();
    checkState(view.revision().getIndex() >= _minimumServingRevision,
        "Native serving metadata has not applied the latest renewal outcome");
    return view;
  }

  private Set<ReadOnlyCapability> allowedCapabilities() {
    return _servingSettings == null ? Set.of() : Set.of(ReadOnlyCapability.SERVER_QUERY);
  }

  /// Captures the coherent metadata revision used for local administrative observations. This permits no query
  /// serving; callers and aggregators must compare the exact physical installation identities independently.
  public MetadataRpc.Revision observationRevision() {
    _admission.readLock().lock();
    try {
      checkState(isReadyForInstallations(), "Native server is not ready for observations");
      return metadata().revision();
    } finally {
      _admission.readLock().unlock();
    }
  }

  /// Filters pinned physical identities through one coherent current assignment image without requiring an active
  /// installation operation. Released copies may remain on disk but must not inflate administrative observations.
  public NativeServerMetadata.Observation observeInstalled(List<Identity> identities) {
    _admission.readLock().lock();
    try {
      checkState(isReadyForInstallations(), "Native server is not ready for observations");
      return metadata().observeInstalled(identities);
    } finally {
      _admission.readLock().unlock();
    }
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
    if (_participant != null && response.getStatus() == ResultStatus.HISTORY_MISMATCH) {
      fence("Native registration history changed");
    }
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
    if (_serving != null) {
      _serving.invalidate();
    }
    if (_metadata != null) {
      _metadata.invalidate();
    }
  }

  private void failed(String operation, Exception e) {
    String failure = operation + ": " + e.getClass().getSimpleName() + ": " + e.getMessage();
    _admission.writeLock().lock();
    try {
      // A request already in flight must not replace the terminal fencing reason.
      if (_fenced || failure.equals(_failure)) {
        return;
      }
      _failure = failure;
    } finally {
      _admission.writeLock().unlock();
    }
    LOGGER.warn("Native server {} failed", operation, e);
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
    if (_serving != null) {
      _serving.invalidate();
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
