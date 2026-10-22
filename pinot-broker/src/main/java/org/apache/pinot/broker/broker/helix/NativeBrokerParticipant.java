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
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.coordination.MetadataRpcConnection;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.coordination.NativeOfflineSegmentWorker;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;


/// Owns a broker membership session without advertising query endpoints, readiness, or serving capabilities.
/// The applied revision covers only the committed own-participant/control projection, never table or routing state.
/// Lifecycle operations and the single bounded heartbeat lane are serialized; status reads never perform RPCs.
final class NativeBrokerParticipant implements AutoCloseable, ServiceStatus.ServiceStatusCallback {
  private static final Logger LOGGER = LoggerFactory.getLogger(NativeBrokerParticipant.class);
  private final NativeMetadataConfig _config;
  private final MetadataRpcConnection _connection;
  private final UUID _incarnation = UUID.randomUUID();
  private final ScheduledExecutorService _heartbeats = Executors.newSingleThreadScheduledExecutor(
      runnable -> new Thread(runnable, "native-broker-membership"));
  @Nullable
  private MetadataRpc.ParticipantToken _participant;
  @Nullable
  private MetadataRpc.MembershipCommandRequest _pendingRenewal;
  private long _nextRenewal = 1;
  private long _appliedIndex;
  private boolean _started;
  private boolean _fenced;
  private volatile boolean _closed;
  private volatile String _description = "Native broker membership starting; query listeners disabled";

  NativeBrokerParticipant(NativeMetadataConfig config, MetadataRpcConnection connection) {
    _config = config;
    _connection = connection;
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
      validateOwnProjection(registered);
      _description = "Native broker membership active; routing unavailable and query listeners disabled";
      _heartbeats.scheduleWithFixedDelay(this::heartbeat, _config.heartbeatMillis(), _config.heartbeatMillis(),
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
        validateOwnProjection(image);
        _pendingRenewal = command().setRenew(MetadataRpc.ParticipantRenewal.newBuilder().setSession(session())
            .setSequence(_nextRenewal).setAppliedRevision(image.getRevision())).build();
      }
      // No read or rewrite may change a logical renewal after an unknown outcome, including its applied revision.
      MetadataRpc.MembershipResponse response = _connection.membershipExecute(_pendingRenewal,
          _config.rpcTimeoutMillis());
      validateResponse(response);
      if (response.getStatus() == ResultStatus.NOT_READY) {
        _description = "Native broker control epoch unavailable; query listeners disabled";
        return;
      }
      checkArgument(response.getStatus() == ResultStatus.OK && response.getRenewalsCount() == 1,
          "Broker renewal rejected: %s", response.getStatus());
      MetadataRpc.RenewalResult renewal = response.getRenewals(0);
      checkArgument(renewal.getParticipant().equals(_participant) && renewal.getSequence() == _nextRenewal,
          "Broker renewal identity or sequence differs");
      checkArgument((renewal.getStatus() == ResultStatus.OK || renewal.getStatus() == ResultStatus.NOT_READY)
          && !renewal.hasPermit(), "Membership-only broker received unexpected authority or outcome");
      checkArgument(_nextRenewal < Long.MAX_VALUE, "Broker renewal sequences exhausted");
      _nextRenewal++;
      _pendingRenewal = null;
      _description = renewal.getStatus() == ResultStatus.OK
          ? "Native broker membership active; routing unavailable and query listeners disabled"
          : "Native broker membership projection must catch up; query listeners disabled";
    } catch (StatusRuntimeException e) {
      Status.Code code = e.getStatus().getCode();
      if (code == Status.Code.PERMISSION_DENIED || code == Status.Code.UNAUTHENTICATED
          || code == Status.Code.INVALID_ARGUMENT) {
        fence("Broker membership authorization or request rejected: " + code);
      } else {
        _description = "Native broker membership transport unavailable; query listeners disabled";
      }
    } catch (IOException | IllegalArgumentException e) {
      fence("Invalid broker membership response: " + e.getMessage());
    } catch (RuntimeException e) {
      fence("Broker membership failed: " + e.getClass().getSimpleName());
    }
  }

  private MetadataRpc.MembershipResponse read() throws IOException {
    MetadataRpc.MembershipResponse response = _connection.membershipRead(MetadataRpc.MembershipReadRequest.newBuilder()
        .setHistoryId(_config.historyId().toString()).build(), _config.rpcTimeoutMillis());
    validateResponse(response);
    return response;
  }

  private void validateResponse(MetadataRpc.MembershipResponse response) throws IOException {
    NativeOfflineSegmentWorker.validateMessage(response);
    checkArgument(response.hasRevision() && response.getRevision().getHistoryId().equals(_config.historyId().toString())
        && response.getRevision().getIndex() >= _appliedIndex, "Broker response history or revision differs");
  }

  private void validateToken(MetadataRpc.ParticipantToken token) {
    checkArgument(token.getHistoryId().equals(_config.historyId().toString())
        && token.getParticipantId().equals(_config.participantId())
        && token.getRole() == MetadataRpc.ParticipantRole.BROKER && token.getRegistrationEpoch() > 0,
        "Broker participant identity differs");
    checkArgument(UUID.fromString(token.getIncarnation()).toString().equals(token.getIncarnation()),
        "Invalid broker incarnation");
  }

  private void validateOwnProjection(MetadataRpc.MembershipResponse response) {
    checkArgument(response.hasControl() && response.hasParticipant(), "Broker membership projection is incomplete");
    MetadataRpc.MembershipControl control = response.getControl();
    MetadataRpc.ParticipantState state = response.getParticipant();
    checkArgument(state.getParticipant().equals(_participant) && state.getCapabilitiesCount() == 0
        && state.getEndpointsCount() == 0 && !state.getReady() && !state.getDraining() && !state.getRetired(),
        "Broker membership identity, capabilities or lifecycle changed");
    checkArgument(control.hasControlEpoch() && control.hasPolicy() && control.getRound() >= 0
        && control.getControlEpoch().getHistoryId().equals(_config.historyId().toString())
        && control.getControlEpoch().getRaftTerm() > 0 && control.getClockBarrierIndex() > 0
        && control.getClockBarrierIndex() <= response.getRevision().getIndex()
        && state.getExpiryRound() > control.getRound() && state.getLastModifiedIndex() > 0
        && state.getLastModifiedIndex() <= response.getRevision().getIndex(), "Broker membership lease is not live");
    _appliedIndex = response.getRevision().getIndex();
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
    _description = detail + "; query listeners disabled";
    _heartbeats.shutdown();
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
  public synchronized void close() {
    if (_closed) {
      return;
    }
    _closed = true;
    _description = "Native broker stopped; query listeners disabled";
    _heartbeats.shutdownNow();
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
