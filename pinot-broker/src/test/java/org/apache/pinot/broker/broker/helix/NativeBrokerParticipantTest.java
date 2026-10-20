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

import io.grpc.Status;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.common.coordination.MetadataRpcConnection;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ResultStatus;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Covers membership-only broker startup and exact renewal recovery without opening query or metadata listeners.
public class NativeBrokerParticipantTest {
  @Test(timeOut = 10_000)
  public void testNormalStarterBypassesZookeeperAndLeavesQueriesUnavailable() throws Exception {
    try (Fixture fixture = new Fixture()) {
      HelixBrokerStarter starter = new HelixBrokerStarter() {
        @Override
        NativeBrokerParticipant createNativeParticipant(NativeMetadataConfig config) {
          return fixture._participant;
        }
      };
      // No ZK configuration and no query ports are supplied. The mock avoids opening TLS connections in this unit test.
      starter.init(fixture._configuration);
      assertEquals(starter.getInstanceId(), fixture._id);
      assertNull(starter._listenerConfigs);
      starter.start();
      try {
        assertNull(starter._spectatorHelixManager);
        assertNull(starter._participantHelixManager);
        assertNull(starter._routingManager);
        assertNull(starter._brokerRequestHandler);
        assertNull(starter._brokerAdminApplication);
        assertNull(starter._brokerGrpcServer);
        assertEquals(ServiceStatus.getServiceStatus(fixture._id), ServiceStatus.Status.BAD);
        assertTrue(ServiceStatus.getStatusDescription(fixture._id).contains("query listeners disabled"));
        assertTrue(fixture._requests.stream().noneMatch(MetadataRpc.MembershipCommandRequest::hasReady));
        assertEquals(fixture._requests.getFirst().getRegister().getEndpointsCount(), 0);
        assertThrows(IllegalArgumentException.class,
            () -> new MultiClusterHelixBrokerStarter().init(fixture._configuration));
        HelixBrokerStarter initializedOnly = new HelixBrokerStarter();
        initializedOnly.init(fixture._configuration);
        initializedOnly.stop();
        assertEquals(ServiceStatus.getServiceStatus(fixture._id), ServiceStatus.Status.BAD);
        NativeBrokerParticipant rejected = mock(NativeBrokerParticipant.class);
        doThrow(new IOException("Registration rejected")).when(rejected).start();
        HelixBrokerStarter duplicate = new HelixBrokerStarter() {
          @Override
          NativeBrokerParticipant createNativeParticipant(NativeMetadataConfig config) {
            return rejected;
          }
        };
        duplicate.init(fixture._configuration);
        assertThrows(IOException.class, duplicate::start);
        duplicate.stop();
        assertTrue(ServiceStatus.getStatusDescription(fixture._id).contains("query listeners disabled"));
      } finally {
        starter.stop();
      }
      assertEquals(ServiceStatus.getServiceStatus(fixture._id), ServiceStatus.Status.NOT_STARTED);
      fixture.assertOwnUnregister();
    }
    try (Fixture fixture = new Fixture()) {
      HelixBrokerStarter starter = new HelixBrokerStarter() {
        @Override
        NativeBrokerParticipant createNativeParticipant(NativeMetadataConfig config) {
          return fixture._participant;
        }
      };
      starter.init(fixture._configuration);
      starter.start();
      ServiceStatus.setServiceStatusCallback(fixture._id,
          new ServiceStatus.LifecycleServiceStatusCallback(() -> false, () -> false));
      try {
        starter.stop();
        assertEquals(ServiceStatus.getServiceStatus(fixture._id), ServiceStatus.Status.GOOD,
            "An old process must not remove a replacement's status callback");
      } finally {
        starter.stop();
        ServiceStatus.removeServiceStatusCallback(fixture._id);
      }
    }
    HelixBrokerStarter invalid = new HelixBrokerStarter();
    assertThrows(IllegalArgumentException.class,
        () -> invalid.init(new PinotConfiguration(Map.of("pinot.coordination.backend", "RATIS"))));
    assertThrows(IllegalStateException.class, invalid::start);
    invalid.stop();
    assertNull(invalid._participantHelixManager);
  }

  @Test(timeOut = 10_000)
  public void testUnknownRenewalRetriesAndConsumedNotReadySequence() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture._participant.start();
      fixture._renewals.add(Status.DEADLINE_EXCEEDED.asRuntimeException());
      fixture._renewals.add("outer-not-ready");
      fixture._renewals.add(ResultStatus.NOT_READY);
      fixture._renewals.add(ResultStatus.OK);
      fixture._renewals.add(ResultStatus.FENCED);
      for (int i = 0; i < 5; i++) {
        fixture._participant.heartbeat();
      }
      List<MetadataRpc.MembershipCommandRequest> renewals = fixture._requests.stream()
          .filter(MetadataRpc.MembershipCommandRequest::hasRenew).toList();
      assertEquals(renewals.size(), 5);
      assertEquals(renewals.get(0), renewals.get(1), "An outer rejection must preserve the exact pending request");
      assertEquals(renewals.get(1), renewals.get(2), "A lost reply must not change the applied revision or sequence");
      assertEquals(renewals.get(2).getRenew().getSequence(), 1L);
      assertEquals(renewals.get(3).getRenew().getSequence(), 2L, "A durable inner NOT_READY consumes its sequence");
      assertEquals(renewals.get(4).getRenew().getSequence(), 3L);
      assertTrue(renewals.get(3).getRenew().getAppliedRevision().getIndex()
          > renewals.getFirst().getRenew().getAppliedRevision().getIndex());
      int requests = fixture._requests.size();
      fixture._participant.heartbeat();
      assertEquals(fixture._requests.size(), requests, "Fenced sessions must stop renewing");
      assertEquals(fixture._participant.getServiceStatus(), ServiceStatus.Status.BAD);
    }
  }

  @Test(timeOut = 10_000)
  public void testUnexpectedAuthorityAndReplacementFenceTheConfirmedSession() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture._grantCapabilities = true;
      assertThrows(IllegalArgumentException.class, fixture._participant::start);
      fixture.assertOwnUnregister();
      verify(fixture._connection).close();
    }
    try (Fixture fixture = new Fixture()) {
      fixture._participant.start();
      MetadataRpc.ParticipantToken original = fixture._token;
      fixture._token = original.toBuilder().setIncarnation(UUID.randomUUID().toString())
          .setRegistrationEpoch(original.getRegistrationEpoch() + 1).build();
      fixture._participant.heartbeat();
      assertFalse(fixture._requests.stream().anyMatch(MetadataRpc.MembershipCommandRequest::hasRenew));
      fixture._participant.close();
      MetadataRpc.MembershipCommandRequest last = fixture._requests.getLast();
      assertTrue(last.hasUnregister());
      assertEquals(last.getUnregister().getIncarnation(), original.getIncarnation());
      assertEquals(last.getUnregister().getRegistrationEpoch(), original.getRegistrationEpoch());
    }
    try (Fixture fixture = new Fixture()) {
      fixture._participant.start();
      fixture._renewals.add("unexpected-permit");
      fixture._participant.heartbeat();
      int requests = fixture._requests.size();
      fixture._participant.heartbeat();
      assertEquals(fixture._requests.size(), requests);
      assertTrue(fixture._participant.getStatusDescription().contains("unexpected authority"));
    }
  }

  private static final class Fixture implements AutoCloseable {
    private final String _history = UUID.randomUUID().toString();
    private final String _id = "Broker_native_" + UUID.randomUUID();
    private final Path _tls = Files.createTempFile("native-broker-test-", ".pem");
    private final MetadataRpcConnection _connection = mock(MetadataRpcConnection.class);
    private final PinotConfiguration _configuration;
    private final NativeBrokerParticipant _participant;
    private final List<MetadataRpc.MembershipCommandRequest> _requests = new ArrayList<>();
    private final Deque<Object> _renewals = new ArrayDeque<>();
    private final MetadataRpc.MembershipControl _control;
    private MetadataRpc.ParticipantToken _token;
    private long _index;
    private boolean _grantCapabilities;

    private Fixture() throws Exception {
      _configuration = new PinotConfiguration(Map.of("pinot.coordination.backend", "RATIS",
          NativeMetadataConfig.PREFIX + "historyId", _history,
          NativeMetadataConfig.PREFIX + "participantId", _id,
          NativeMetadataConfig.PREFIX + "metadata.endpoints", "localhost:10000",
          NativeMetadataConfig.PREFIX + "tls.certChain", _tls.toString(),
          NativeMetadataConfig.PREFIX + "tls.privateKey", _tls.toString(),
          NativeMetadataConfig.PREFIX + "tls.trustCerts", _tls.toString(),
          NativeMetadataConfig.PREFIX + "heartbeatMillis", 30_000));
      _control = MetadataRpc.MembershipControl.newBuilder().setControlEpoch(MetadataRpc.ControlToken.newBuilder()
              .setHistoryId(_history).setRaftTerm(1).setBarrierIndex(1).setLeaderId("controller")
              .setActivationId(UUID.randomUUID().toString())).setClockBarrierIndex(1)
          .setMinimumTickDelayMillis(100).setMaxConfiguredPermitLifetimeMillis(200)
          .setPolicy(MetadataRpc.MembershipPolicy.newBuilder().setVersion(1).setMinTickIntervalMillis(100)
              .setLeaseRounds(3).setPermitLifetimeMillis(200).setMaxQueryLifetimeMillis(300).setMaxRevisionLag(1000))
          .build();
      when(_connection.membershipRead(any(), anyLong())).thenAnswer(invocation -> {
        MetadataRpc.MembershipResponse.Builder response =
            response(_token == null ? ResultStatus.NOT_FOUND : ResultStatus.OK);
        if (_token != null) {
          response.setParticipant(state());
        }
        return response.build();
      });
      when(_connection.membershipExecute(any(), anyLong())).thenAnswer(invocation -> {
        MetadataRpc.MembershipCommandRequest request = invocation.getArgument(0);
        _requests.add(request);
        if (request.hasRegister()) {
          _token = MetadataRpc.ParticipantToken.newBuilder().setHistoryId(_history).setParticipantId(_id)
              .setRole(MetadataRpc.ParticipantRole.BROKER).setIncarnation(request.getRegister().getIncarnation())
              .setRegistrationEpoch(request.getRegister().getExpectedEpoch() + 1).build();
          return response(ResultStatus.OK).setParticipant(state()).build();
        }
        if (request.hasRenew()) {
          Object next = _renewals.isEmpty() ? ResultStatus.OK : _renewals.removeFirst();
          if (next instanceof RuntimeException failure) {
            throw failure;
          }
          if (next.equals("outer-not-ready")) {
            return response(ResultStatus.NOT_READY).build();
          }
          MetadataRpc.RenewalResult.Builder renewal = MetadataRpc.RenewalResult.newBuilder()
              .setParticipant(_token).setSequence(request.getRenew().getSequence());
          if (next.equals("unexpected-permit")) {
            renewal.setStatus(ResultStatus.OK).setPermit(MetadataRpc.ServingPermit.getDefaultInstance());
          } else {
            renewal.setStatus((ResultStatus) next);
          }
          return response(ResultStatus.OK).addRenewals(renewal).build();
        }
        assertTrue(request.hasUnregister(), "Membership-only brokers must never set ready or advertise endpoints");
        return response(ResultStatus.OK).build();
      });
      _participant = new NativeBrokerParticipant(new NativeMetadataConfig(_configuration), _connection);
    }

    private MetadataRpc.MembershipResponse.Builder response(ResultStatus status) {
      return MetadataRpc.MembershipResponse.newBuilder().setStatus(status).setControl(_control)
          .setRevision(MetadataRpc.Revision.newBuilder().setHistoryId(_history).setIndex(++_index));
    }

    private MetadataRpc.ParticipantState state() {
      MetadataRpc.ParticipantState.Builder state = MetadataRpc.ParticipantState.newBuilder().setParticipant(_token)
          .setExpiryRound(3).setLastModifiedIndex(1)
          .setAppliedRevision(MetadataRpc.Revision.newBuilder().setHistoryId(_history).setIndex(0));
      if (_grantCapabilities) {
        state.addCapabilities(MetadataRpc.ServingCapability.BROKER_QUERY);
      }
      return state.build();
    }

    private void assertOwnUnregister() {
      MetadataRpc.MembershipCommandRequest last = _requests.getLast();
      assertTrue(last.hasUnregister());
      assertEquals(last.getUnregister().getIncarnation(), _token.getIncarnation());
      assertEquals(last.getUnregister().getRegistrationEpoch(), _token.getRegistrationEpoch());
    }

    @Override
    public void close() throws Exception {
      _participant.close();
      Files.deleteIfExists(_tls);
    }
  }
}
