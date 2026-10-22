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
package org.apache.pinot.broker.broker.helix;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import org.apache.pinot.broker.routing.NativeBrokerRoutingImage;
import org.apache.pinot.common.coordination.MetadataRpcConnection;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.coordination.NativeOfflineSegmentWorker;
import org.apache.pinot.common.coordination.QueryDeadlineGuard;
import org.apache.pinot.common.coordination.ServingPermitGuard;
import org.apache.pinot.common.coordination.ServingPermitGuard.LeaseObservation;
import org.apache.pinot.common.coordination.ServingPermitGuard.MembershipView;
import org.apache.pinot.common.coordination.ServingPermitGuard.MonotonicClock;
import org.apache.pinot.common.coordination.ServingPermitGuard.PeerLeaseBound;
import org.apache.pinot.common.coordination.ServingPermitGuard.QualifiedLifetimePolicy;
import org.apache.pinot.common.coordination.ServingPermitGuard.QueryTicket;
import org.apache.pinot.common.coordination.ServingPermitGuard.ReadOnlyCapability;
import org.apache.pinot.common.coordination.ServingPermitGuard.ServingDeniedException;
import org.apache.pinot.common.request.context.NativeQueryDeadline;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;


/// Owns a broker membership session without opening query listeners. The default is membership-only; explicitly
/// qualified serving settings enable local admission and committed readiness, never a network query resource.
/// The applied revision covers only the committed own-participant/control projection, never table or routing state.
/// Lifecycle operations and the single bounded heartbeat lane are serialized; status reads never perform RPCs.
final class NativeBrokerParticipant implements AutoCloseable, ServiceStatus.ServiceStatusCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeBrokerParticipant.class);
  private static final int MAX_ROUTING_TABLES = 128;
  private static final int MAX_ROUTING_BYTES = 8 * 1024 * 1024;
  private final NativeMetadataConfig _config;
  private final MetadataRpcConnection _connection;
  private final MetadataRpcConnection _routingConnection;
  private final Object _routingLock = new Object();
  private final ReentrantReadWriteLock _admission = new ReentrantReadWriteLock();
  @Nullable
  private final ServingSettings _servingSettings;
  @Nullable
  private ServingPermitGuard _serving;
  // Query paths use only this immutable projection and the admission lock, never the membership monitor.
  private volatile ServingMembership _servingMembership = new ServingMembership(null, false, 0);
  private Map<String, RoutingCapture> _queryRoutes = Map.of();
  private long _queryGeneration;
  private final UUID _incarnation = UUID.randomUUID();
  private final ScheduledExecutorService _heartbeats = Executors.newSingleThreadScheduledExecutor(
      runnable -> new Thread(runnable, "native-broker-membership"));
  private final ScheduledExecutorService _routingPoller = Executors.newSingleThreadScheduledExecutor(
      runnable -> new Thread(runnable, "native-broker-routing"));
  @Nullable
  private MetadataRpc.ParticipantToken _participant;
  @Nullable
  private MetadataRpc.MembershipCommandRequest _pendingRenewal;
  private long _nextRenewal = 1;
  private long _appliedIndex;
  private boolean _started;
  private boolean _cleanedUp;
  private boolean _fenced;
  private boolean _membershipHealthy;
  private long _routingGeneration;
  // This is only a read-regression floor on the separate routing lane, never a global applied revision.
  private long _routingIndex;
  private Map<String, NativeBrokerRoutingImage> _routingImages = Map.of();
  private volatile boolean _closed;
  private volatile String _description = "Native broker membership starting; query listeners disabled";

  NativeBrokerParticipant(NativeMetadataConfig config, MetadataRpcConnection connection,
      MetadataRpcConnection routingConnection) {
    this(config, connection, routingConnection, null);
  }

  NativeBrokerParticipant(NativeMetadataConfig config, MetadataRpcConnection connection,
      MetadataRpcConnection routingConnection, @Nullable ServingSettings servingSettings) {
    checkArgument(connection != routingConnection, "Routing hydration requires a separate membership connection");
    _config = requireNonNull(config);
    _connection = requireNonNull(connection);
    _routingConnection = requireNonNull(routingConnection);
    _servingSettings = servingSettings;
  }

  /// Explicit qualified clock inputs, with no platform defaults and no listener activation.
  record ServingSettings(MonotonicClock clock, QualifiedLifetimePolicy lifetimePolicy,
                         QueryDeadlineGuard queryDeadlines) {
    ServingSettings {
      requireNonNull(clock);
      requireNonNull(lifetimePolicy);
      requireNonNull(queryDeadlines);
    }
  }

  /// A request-owned immutable plan and fanout, bound to its original finite serving and selected-server leases.
  static final class QueryAdmission {
    private final NativeBrokerRoutingImage _image;
    private final List<NativeBrokerRoutingImage.ServerRequest> _requests;
    private final List<PeerLeaseBound> _serverLeases;
    private final QueryTicket _ticket;
    private final long _generation;

    private QueryAdmission(NativeBrokerRoutingImage image, List<NativeBrokerRoutingImage.ServerRequest> requests,
        List<PeerLeaseBound> serverLeases, QueryTicket ticket, long generation) {
      _image = image;
      _requests = List.copyOf(requests);
      _serverLeases = List.copyOf(serverLeases);
      _ticket = ticket;
      _generation = generation;
    }

    NativeBrokerRoutingImage image() {
      return _image;
    }

    List<NativeBrokerRoutingImage.ServerRequest> requests() {
      return _requests;
    }

    NativeBrokerRoutingImage.PlanningMetadata planningMetadata(boolean ignoreCase) {
      return _image.planningMetadata(ignoreCase);
    }
  }

  private record ServingMembership(@Nullable MembershipView view, boolean available, long minimumRevision) {
  }

  private record RoutingCapture(NativeBrokerRoutingImage image, Map<String, PeerLeaseBound> serverLeases) {
  }

  synchronized void start() throws IOException {
    checkState(!_started && !_closed, "Native broker membership cannot be restarted");
    _started = true;
    try {
      MetadataRpc.MembershipResponse current = read();
      checkArgument(current.getStatus() == ResultStatus.OK || current.getStatus() == ResultStatus.NOT_FOUND,
          "Cannot inspect native broker registration: %s", current.getStatus());
      long previousEpoch = 0;
      if (current.getStatus() == ResultStatus.OK) {
        checkArgument(current.hasParticipant(), "Current broker registration is missing");
        validateToken(current.getParticipant().getParticipant());
        previousEpoch = current.getParticipant().getParticipant().getRegistrationEpoch();
      }
      checkArgument(previousEpoch < Long.MAX_VALUE, "Broker registration epochs exhausted");
      MetadataRpc.MembershipCommandRequest registration = command().setRegister(
          MetadataRpc.ParticipantRegistration.newBuilder().setIncarnation(_incarnation.toString())
              .setExpectedEpoch(previousEpoch)).build();
      MetadataRpc.MembershipResponse registered = _connection.membershipExecute(registration,
          _config.rpcTimeoutMillis());
      validateResponse(registered);
      checkArgument(registered.getStatus() == ResultStatus.OK && registered.hasParticipant(),
          "Cannot acquire native broker registration: %s", registered.getStatus());
      MetadataRpc.ParticipantToken token = registered.getParticipant().getParticipant();
      validateToken(token);
      checkArgument(token.getIncarnation().equals(_incarnation.toString())
          && token.getRegistrationEpoch() == previousEpoch + 1, "Broker registration response differs");
      // Retain confirmed ownership before checking granted capabilities so failed startup can retire this session.
      _participant = token;
      MembershipView view = validateOwnProjection(registered);
      _admission.writeLock().lock();
      try {
        checkState(!_closed, "Native broker stopped during registration");
        if (_servingSettings != null) {
          _serving = new ServingPermitGuard(token, Set.of(ReadOnlyCapability.BROKER_QUERY),
              _servingSettings.clock(), _servingSettings.lifetimePolicy(), this::servingView);
        }
        publishMembership(view, false, null);
      } finally {
        _admission.writeLock().unlock();
      }
      if (_servingSettings != null) {
        var ready = command().setReady(MetadataRpc.ParticipantReadiness.newBuilder().setSession(session())
            .setAppliedRevision(registered.getRevision()).setReady(true)).build();
        var response = _connection.membershipExecute(ready, _config.rpcTimeoutMillis());
        validateResponse(response);
        checkArgument(response.getStatus() == ResultStatus.OK && response.hasParticipant()
            && response.getParticipant().getParticipant().equals(token) && response.getParticipant().getReady(),
            "Native broker readiness was not committed");
        observeServingResponse(response);
        // Read a complete own projection; never patch the bootstrap state with the readiness response.
        var currentView = read();
        checkArgument(currentView.getStatus() == ResultStatus.OK, "Native broker readiness image is unavailable");
        view = validateOwnProjection(currentView);
        checkArgument(view.ready() && !view.draining(), "Native broker readiness image is not ready");
      }
      _membershipHealthy = true;
      publishMembership(view, true, null);
      _description = "Native broker membership active; routing unavailable and query listeners disabled";
      _heartbeats.scheduleWithFixedDelay(this::heartbeat, _config.heartbeatMillis(), _config.heartbeatMillis(),
          TimeUnit.MILLISECONDS);
      _routingPoller.scheduleWithFixedDelay(this::refreshRouting, _config.heartbeatMillis(), _config.heartbeatMillis(),
          TimeUnit.MILLISECONDS);
    } catch (IOException | RuntimeException e) {
      close();
      throw e;
    }
  }

  @VisibleForTesting
  synchronized void heartbeat() {
    if (_closed || _fenced) {
      return;
    }
    try {
      if (_pendingRenewal == null) {
        MetadataRpc.MembershipResponse image = read();
        checkArgument(image.getStatus() == ResultStatus.OK, "Broker membership is unavailable: %s", image.getStatus());
        publishMembership(validateOwnProjection(image), _membershipHealthy
            && _servingMembership.available(), null);
        if (_closed) {
          return;
        }
        _pendingRenewal = command().setRenew(MetadataRpc.ParticipantRenewal.newBuilder().setSession(session())
            .setSequence(_nextRenewal).setAppliedRevision(image.getRevision())).build();
      }
      _admission.writeLock().lock();
      try {
        if (_closed) {
          return;
        }
        if (_serving != null) {
          _serving.firstSend(_nextRenewal);
        }
      } finally {
        _admission.writeLock().unlock();
      }
      // No read or rewrite may change a logical renewal after an unknown outcome, including its applied revision.
      MetadataRpc.MembershipResponse response = _connection.membershipExecute(_pendingRenewal,
          _config.rpcTimeoutMillis());
      validateResponse(response);
      observeServingResponse(response);
      if (_closed) {
        return;
      }
      if (response.getStatus() == ResultStatus.NOT_READY) {
        membershipUnavailable();
        _membershipHealthy = false;
        _description = "Native broker control epoch unavailable; query listeners disabled";
        return;
      }
      checkArgument(response.getStatus() == ResultStatus.OK && response.getRenewalsCount() == 1,
          "Broker renewal rejected: %s", response.getStatus());
      MetadataRpc.RenewalResult renewal = response.getRenewals(0);
      checkArgument(renewal.getParticipant().equals(_participant) && renewal.getSequence() == _nextRenewal,
          "Broker renewal identity or sequence differs");
      checkArgument((renewal.getStatus() == ResultStatus.OK || renewal.getStatus() == ResultStatus.NOT_READY)
          && (renewal.getStatus() == ResultStatus.OK || !renewal.hasPermit())
          && (_servingSettings != null || !renewal.hasPermit()),
          "Broker received unexpected authority or outcome");
      checkArgument(_nextRenewal < Long.MAX_VALUE, "Broker renewal sequences exhausted");
      // The durable outcome is consumed even if the subsequent coherent read fails.
      _nextRenewal++;
      _pendingRenewal = null;
      _membershipHealthy = renewal.getStatus() == ResultStatus.OK;
      if (!_membershipHealthy) {
        membershipUnavailable();
      }
      if (_servingSettings != null) {
        var image = read();
        checkArgument(image.getStatus() == ResultStatus.OK, "Broker renewal image is unavailable: %s",
            image.getStatus());
        MembershipView view = validateOwnProjection(image);
        boolean draining = _membershipHealthy && !renewal.hasPermit() && view.draining();
        if (!renewal.hasPermit() && !draining) {
          invalidateRouting();
        }
        // The grant's minimum revision includes its own renewal command, so the pre-renewal read is insufficient.
        publishMembership(view, _membershipHealthy && (renewal.hasPermit() || draining),
            renewal.hasPermit() ? renewal.getPermit() : null);
      }
      _description = renewal.getStatus() == ResultStatus.OK
          ? routingDescription()
          : "Native broker membership projection must catch up; query listeners disabled";
    } catch (StatusRuntimeException e) {
      Status.Code code = e.getStatus().getCode();
      if (code == Status.Code.PERMISSION_DENIED || code == Status.Code.UNAUTHENTICATED
          || code == Status.Code.INVALID_ARGUMENT) {
        fence("Broker membership authorization or request rejected: " + code);
      } else {
        membershipUnavailable();
        _membershipHealthy = false;
        _description = "Native broker membership transport unavailable; query listeners disabled";
      }
    } catch (ServingDeniedException e) {
      fence("Native broker serving authority failed: " + e.getMessage());
    } catch (IOException e) {
      membershipUnavailable();
      _membershipHealthy = false;
      _description = "Native broker membership transport unavailable; query listeners disabled";
    } catch (IllegalArgumentException e) {
      fence("Invalid broker membership response: " + e.getMessage());
    } catch (RuntimeException e) {
      fence("Broker membership failed: " + e.getClass().getSimpleName());
    }
  }

  /// One bounded discovery/hydration cycle. RPCs never hold the membership monitor; a heartbeat failure, fencing or
  /// shutdown invalidates any overlapping cycle before its result can be published.
  @VisibleForTesting
  void refreshRouting() {
    synchronized (_routingLock) {
      MetadataRpc.ParticipantToken participant;
      long generation;
      synchronized (this) {
        if (_closed || _fenced || !_membershipHealthy
            || _servingMembership.view() != null && _servingMembership.view().draining()) {
          return;
        }
        participant = _participant;
        generation = _routingGeneration;
      }
      try {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(_config.rpcTimeoutMillis());
        var discovery = routingSnapshot(participant, "", MAX_ROUTING_TABLES + 2, deadline);
        var tables = NativeBrokerRoutingImage.discover(discovery, participant);
        checkArgument(tables.size() <= MAX_ROUTING_TABLES, "Routing cache exceeds its table limit");
        _routingIndex = discovery.getRevision().getIndex();
        Map<String, NativeBrokerRoutingImage> images = new HashMap<>();
        Map<String, RoutingCapture> captures = new HashMap<>();
        int bytes = discovery.getSerializedSize();
        for (String table : tables) {
          LeaseObservation observation = beginLeaseObservation();
          var snapshot = routingSnapshot(participant, table, NativeBrokerRoutingImage.MAX_RECORDS, deadline);
          NativeBrokerRoutingImage image = new NativeBrokerRoutingImage(snapshot, participant, table);
          _routingIndex = snapshot.getRevision().getIndex();
          checkArgument(image.serializedBytes() <= MAX_ROUTING_BYTES - bytes,
              "Routing cache exceeds its aggregate byte limit");
          bytes += image.serializedBytes();
          images.put(table, image);
          captures.put(table, captureRouting(image, observation));
        }
        synchronized (this) {
          if (!_closed && !_fenced && _membershipHealthy && generation == _routingGeneration
              && participant.equals(_participant)) {
            _admission.writeLock().lock();
            try {
              if (_closed) {
                return;
              }
              if (!sameRoutes(captures)) {
                _queryGeneration = Math.incrementExact(_queryGeneration);
              }
              _queryRoutes = Map.copyOf(captures);
              _routingImages = Map.copyOf(images);
            } finally {
              _admission.writeLock().unlock();
            }
            _description = routingDescription();
          }
        }
      } catch (IOException | RuntimeException e) {
        synchronized (this) {
          if (generation != _routingGeneration || _closed || _fenced) {
            return;
          }
          if (e instanceof ServingDeniedException) {
            fence("Native broker routing clock lost serving qualification");
            return;
          }
          invalidateRouting();
          if (!_closed && !_fenced) {
            _description = "Native broker routing unavailable: " + e.getMessage() + "; query listeners disabled";
          }
        }
      }
    }
  }

  private MetadataRpc.RoutingSnapshot routingSnapshot(MetadataRpc.ParticipantToken participant, String table,
      int maxRecords, long deadline) throws IOException {
    long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
    if (_closed || remaining <= 0) {
      throw new IOException("Native routing refresh deadline expired or broker stopped");
    }
    var response = _routingConnection.routingSnapshot(MetadataRpc.RoutingSnapshotRequest.newBuilder()
        .setHistoryId(_config.historyId().toString()).setSession(MetadataRpc.ParticipantSession.newBuilder()
            .setIncarnation(participant.getIncarnation()).setRegistrationEpoch(participant.getRegistrationEpoch()))
        .setTableName(table).setMaxRecords(maxRecords).setMaxBytes(NativeBrokerRoutingImage.MAX_BYTES).build(),
        remaining);
    checkArgument(response.hasRevision() && response.getRevision().getIndex() >= _routingIndex,
        "Routing revision regressed");
    if (deadline - System.nanoTime() <= 0) {
      throw new IOException("Native routing refresh deadline expired");
    }
    return response;
  }

  @VisibleForTesting
  synchronized NativeBrokerRoutingImage routingImage(String table) {
    checkState(!_closed && !_fenced && _membershipHealthy && _routingImages.containsKey(table),
        "Current native table routing is unavailable");
    return _routingImages.get(table);
  }

  private void invalidateRouting() {
    _admission.writeLock().lock();
    try {
      _routingImages = Map.of();
      _queryRoutes = Map.of();
      _routingGeneration++;
      _queryGeneration = Math.incrementExact(_queryGeneration);
    } finally {
      _admission.writeLock().unlock();
    }
  }

  private void membershipUnavailable() {
    _admission.writeLock().lock();
    try {
      invalidateRouting();
      ServingMembership current = _servingMembership;
      _servingMembership = new ServingMembership(current.view(), false, current.minimumRevision());
    } finally {
      _admission.writeLock().unlock();
    }
  }

  @Nullable
  private LeaseObservation beginLeaseObservation() {
    _admission.writeLock().lock();
    try {
      checkState(!_closed, "Native broker stopped before routing hydration");
      return _serving == null ? null : _serving.beginLeaseObservation();
    } finally {
      _admission.writeLock().unlock();
    }
  }

  private RoutingCapture captureRouting(NativeBrokerRoutingImage image, @Nullable LeaseObservation observation) {
    Map<String, PeerLeaseBound> bounds = new HashMap<>();
    _admission.writeLock().lock();
    try {
      checkState(!_closed, "Native broker stopped during routing hydration");
      if (_serving != null) {
        var control = image.control();
        for (var server : image.serverAuthorities().values()) {
          bounds.put(server.getParticipant().getParticipantId(), _serving.boundPeerLease(
              requireNonNull(observation), control.getRound(), server.getExpiryRound(),
              control.getPolicy().getMinTickIntervalMillis(), control.getPolicy().getPermitLifetimeMillis()));
        }
      }
    } finally {
      _admission.writeLock().unlock();
    }
    return new RoutingCapture(image, Map.copyOf(bounds));
  }

  private boolean sameRoutes(Map<String, RoutingCapture> next) {
    if (!_queryRoutes.keySet().equals(next.keySet())) {
      return false;
    }
    return next.entrySet().stream().allMatch(entry -> {
      NativeBrokerRoutingImage previous = _queryRoutes.get(entry.getKey()).image();
      NativeBrokerRoutingImage image = entry.getValue().image();
      return previous.queryIdentity().equals(image.queryIdentity())
          && previous.control().getControlEpoch().equals(image.control().getControlEpoch())
          && previous.control().getPolicy().getVersion() == image.control().getPolicy().getVersion();
    });
  }

  /// Originate once at authenticated request entry, before planning. Retries retain the returned deadline.
  NativeQueryDeadline originateDeadline(long requestedMillis) {
    _admission.readLock().lock();
    try {
      serving();
      return requireNonNull(_servingSettings).queryDeadlines().originate(requestedMillis);
    } finally {
      _admission.readLock().unlock();
    }
  }

  QueryAdmission admitQuery(String table, UUID queryId, UUID attemptId, NativeQueryDeadline deadline) {
    _admission.readLock().lock();
    try {
      ServingPermitGuard guard = serving();
      QueryTicket ticket = guard.admit(ReadOnlyCapability.BROKER_QUERY, deadline,
          requireNonNull(_servingSettings).queryDeadlines());
      RoutingCapture capture = _queryRoutes.get(table);
      checkState(capture != null && capture.image().hasPlanningMetadata(),
          "Complete native query planning metadata is unavailable");
      checkControl(capture.image());
      var requests = capture.image().requests(queryId, attemptId, deadline);
      List<PeerLeaseBound> leases = new ArrayList<>(requests.size());
      for (var request : requests) {
        PeerLeaseBound bound = capture.serverLeases().get(request.context().server().participantId());
        guard.checkPeerLease(bound);
        leases.add(bound);
      }
      guard.checkDelivery(ticket);
      return new QueryAdmission(capture.image(), requests, leases, ticket, _queryGeneration);
    } finally {
      _admission.readLock().unlock();
    }
  }

  void checkQueryDelivery(QueryAdmission admission) {
    _admission.readLock().lock();
    try {
      validateDelivery(admission);
    } finally {
      _admission.readLock().unlock();
    }
  }

  /// The callback must be bounded and must not call lifecycle methods or acquire membership/routing monitors.
  /// It executes under the admission read lock so a committed local revocation cannot race check-then-delivery.
  void deliverQuery(QueryAdmission admission, Runnable callback) {
    requireNonNull(callback);
    _admission.readLock().lock();
    try {
      validateDelivery(admission);
      callback.run();
    } finally {
      _admission.readLock().unlock();
    }
  }

  private void validateDelivery(QueryAdmission admission) {
    requireNonNull(admission);
    ServingPermitGuard guard = serving();
    checkState(admission._generation == _queryGeneration, "Native query routing was invalidated");
    checkControl(admission._image);
    guard.checkDelivery(admission._ticket);
    for (PeerLeaseBound bound : admission._serverLeases) {
      guard.checkPeerLease(bound);
    }
    // Peer-clock observations can consume time; recheck the original ticket immediately before the callback.
    guard.checkDelivery(admission._ticket);
  }

  private void checkControl(NativeBrokerRoutingImage image) {
    MembershipView own = servingView();
    checkState(image.control().getControlEpoch().equals(own.control())
        && image.control().getPolicy().getVersion() == own.policyVersion(),
        "Native query routing and serving control differ");
  }

  private ServingPermitGuard serving() {
    checkState(!_closed && _serving != null, "Native broker serving requires explicit qualified clock inputs");
    return _serving;
  }

  private MembershipView servingView() {
    ServingMembership current = _servingMembership;
    checkState(!_closed && current.available() && current.view() != null
        && current.view().revision().getIndex() >= current.minimumRevision(),
        "Coherent native broker serving membership is unavailable");
    return current.view();
  }

  private String routingDescription() {
    return "Native broker has " + _routingImages.size() + " table routing images; query listeners disabled";
  }

  private MetadataRpc.MembershipResponse read() throws IOException {
    MetadataRpc.MembershipResponse response = _connection.membershipRead(MetadataRpc.MembershipReadRequest.newBuilder()
        .setHistoryId(_config.historyId().toString()).build(), _config.rpcTimeoutMillis());
    validateResponse(response);
    return response;
  }

  private void validateResponse(MetadataRpc.MembershipResponse response) throws IOException {
    try {
      NativeOfflineSegmentWorker.validateMessage(response);
    } catch (IOException e) {
      throw new IllegalArgumentException("Malformed broker membership response", e);
    }
    checkArgument(response.hasRevision() && response.getRevision().getHistoryId().equals(_config.historyId().toString())
        && response.getRevision().getIndex() >= _appliedIndex
        && response.getRevision().getIndex() >= _servingMembership.minimumRevision(),
        "Broker response history or revision differs");
  }

  private void validateToken(MetadataRpc.ParticipantToken token) {
    checkArgument(token.getHistoryId().equals(_config.historyId().toString())
        && token.getParticipantId().equals(_config.participantId())
        && token.getRole() == MetadataRpc.ParticipantRole.BROKER && token.getRegistrationEpoch() > 0,
        "Broker participant identity differs");
    checkArgument(UUID.fromString(token.getIncarnation()).toString().equals(token.getIncarnation()),
        "Invalid broker incarnation");
  }

  private MembershipView validateOwnProjection(MetadataRpc.MembershipResponse response) {
    checkArgument(response.hasControl() && response.hasParticipant(), "Broker membership projection is incomplete");
    MetadataRpc.MembershipControl control = response.getControl();
    MetadataRpc.ParticipantState state = response.getParticipant();
    boolean queryCapability = state.getCapabilitiesList().equals(List.of(MetadataRpc.ServingCapability.BROKER_QUERY));
    checkArgument(state.getParticipant().equals(_participant)
        && (state.getCapabilitiesCount() == 0 || _servingSettings != null && queryCapability)
        && state.getEndpointsCount() == 0 && !state.getRetired() && !(state.getReady() && state.getDraining())
        && (_servingSettings != null || !state.getReady() && !state.getDraining()),
        "Broker membership identity, capabilities or lifecycle changed");
    checkArgument(control.hasControlEpoch() && control.hasPolicy() && control.getRound() >= 0
        && control.getControlEpoch().getHistoryId().equals(_config.historyId().toString())
        && control.getControlEpoch().getRaftTerm() > 0 && control.getControlEpoch().getBarrierIndex() > 0
        && !control.getControlEpoch().getLeaderId().isBlank()
        && control.getClockBarrierIndex() >= control.getControlEpoch().getBarrierIndex()
        && control.getClockBarrierIndex() <= response.getRevision().getIndex()
        && state.getExpiryRound() > control.getRound() && state.getLastModifiedIndex() > 0
        && state.getLastModifiedIndex() <= response.getRevision().getIndex()
        && state.getLastRenewalSequence() >= 0 && state.hasAppliedRevision()
        && state.getAppliedRevision().getHistoryId().equals(_config.historyId().toString())
        && state.getAppliedRevision().getIndex() >= 0
        && state.getAppliedRevision().getIndex() < state.getLastModifiedIndex(),
        "Broker membership lease is not live");
    String activation = control.getControlEpoch().getActivationId();
    checkArgument(UUID.fromString(activation).toString().equals(activation), "Invalid broker control activation");
    var policy = control.getPolicy();
    checkArgument(policy.getVersion() > 0 && policy.getMinTickIntervalMillis() > 0 && policy.getLeaseRounds() >= 2
        && policy.getPermitLifetimeMillis() > 0 && policy.getMaxQueryLifetimeMillis() > 0
        && policy.getMaxRevisionLag() > 0
        && (policy.getPermitLifetimeMillis() - 1) / policy.getMinTickIntervalMillis() < policy.getLeaseRounds() - 1
        && control.getMinimumTickDelayMillis() >= policy.getMinTickIntervalMillis()
        && control.getMaxConfiguredPermitLifetimeMillis() >= policy.getPermitLifetimeMillis()
        && (control.getMaxConfiguredPermitLifetimeMillis() == policy.getPermitLifetimeMillis()
            || control.getMinimumTickDelayMillis() >= control.getMaxConfiguredPermitLifetimeMillis()),
        "Invalid broker serving policy or timing barrier");
    _appliedIndex = response.getRevision().getIndex();
    return new MembershipView(response.getRevision(), control.getControlEpoch(), state.getParticipant(), true,
        state.getReady(), state.getDraining(), queryCapability ? Set.of(ReadOnlyCapability.BROKER_QUERY) : Set.of(),
        policy.getVersion());
  }

  private void observeServingResponse(MetadataRpc.MembershipResponse response) {
    if (_servingSettings == null) {
      return;
    }
    _admission.writeLock().lock();
    try {
      ServingMembership current = _servingMembership;
      _servingMembership = new ServingMembership(current.view(), false,
          Math.max(current.minimumRevision(), response.getRevision().getIndex()));
    } finally {
      _admission.writeLock().unlock();
    }
  }

  private void publishMembership(MembershipView view, boolean available, @Nullable MetadataRpc.ServingPermit permit) {
    _admission.writeLock().lock();
    try {
      if (_closed) {
        return;
      }
      ServingMembership current = _servingMembership;
      MembershipView previous = current.view();
      checkArgument(view.revision().getIndex() >= current.minimumRevision()
          && (previous == null || view.revision().getIndex() >= previous.revision().getIndex()
          && (!view.revision().equals(previous.revision()) || view.equals(previous))),
          "Broker membership image regressed or conflicts at the same revision");
      if (previous != null) {
        boolean beganDrain = !previous.draining() && view.draining();
        boolean sameAuthority = previous.participant().equals(view.participant())
            && previous.control().equals(view.control()) && previous.live() == view.live()
            && previous.capabilities().equals(view.capabilities()) && previous.policyVersion() == view.policyVersion()
            && (previous.ready() == view.ready() && previous.draining() == view.draining()
                || beganDrain && previous.ready());
        if (!sameAuthority) {
          invalidateRouting();
        } else if (beganDrain) {
          // Discard a pre-drain hydration cycle without revoking finite, already admitted work.
          _routingGeneration++;
        }
      }
      _servingMembership = new ServingMembership(view, available, current.minimumRevision());
      if (permit != null) {
        ServingPermitGuard guard = requireNonNull(_serving);
        try {
          guard.accept(permit);
        } catch (RuntimeException e) {
          // Do not expose the new view with an old grant between this lock release and lifecycle fencing.
          _servingMembership = new ServingMembership(view, false, current.minimumRevision());
          guard.invalidate();
          throw e;
        }
      }
    } finally {
      _admission.writeLock().unlock();
    }
  }

  private MetadataRpc.ParticipantSession session() {
    checkState(_participant != null, "Broker registration is absent");
    return MetadataRpc.ParticipantSession.newBuilder().setIncarnation(_participant.getIncarnation())
        .setRegistrationEpoch(_participant.getRegistrationEpoch()).build();
  }

  private MetadataRpc.MembershipCommandRequest.Builder command() {
    return MetadataRpc.MembershipCommandRequest.newBuilder().setHistoryId(_config.historyId().toString());
  }

  private void fence(String detail) {
    _fenced = true;
    _membershipHealthy = false;
    _admission.writeLock().lock();
    try {
      membershipUnavailable();
      if (_serving != null) {
        _serving.invalidate();
      }
    } finally {
      _admission.writeLock().unlock();
    }
    _description = detail + "; query listeners disabled";
    _heartbeats.shutdown();
    _routingPoller.shutdownNow();
    LOGGER.warn("{}", _description);
  }

  @Override
  public ServiceStatus.Status getServiceStatus() {
    return _closed ? ServiceStatus.Status.SHUTTING_DOWN : ServiceStatus.Status.BAD;
  }

  @Override
  public String getStatusDescription() {
    return _description;
  }

  @Override
  public void close() {
    // Fence locally before waiting for the membership monitor, whose heartbeat may be blocked in an RPC.
    _admission.writeLock().lock();
    try {
      _closed = true;
      ServingMembership current = _servingMembership;
      _servingMembership = new ServingMembership(current.view(), false, current.minimumRevision());
      _queryRoutes = Map.of();
      if (_serving != null) {
        _serving.invalidate();
      }
      _description = "Native broker stopped; query listeners disabled";
    } finally {
      _admission.writeLock().unlock();
    }
    _heartbeats.shutdownNow();
    _routingPoller.shutdownNow();
    synchronized (this) {
      if (_cleanedUp) {
        return;
      }
      _cleanedUp = true;
      invalidateRouting();
      _routingConnection.close();
      try {
        if (_participant != null) {
          try {
            _connection.membershipExecute(command().setUnregister(session()).build(), _config.rpcTimeoutMillis());
          } catch (RuntimeException e) {
            LOGGER.warn("Could not confirm native broker unregister; its lease must expire", e);
          }
        }
      } finally {
        _connection.close();
      }
    }
  }
}
