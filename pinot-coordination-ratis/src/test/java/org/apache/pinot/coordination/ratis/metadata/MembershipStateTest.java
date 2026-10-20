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
package org.apache.pinot.coordination.ratis.metadata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ActivateControlEpoch;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.BeginDrain;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ConfigurePolicy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.GetParticipant;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Operation;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Register;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.RenewBatch;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Renewal;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.RenewalResult;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Response;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.SetReady;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Tick;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Unregister;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Action;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipQuery;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RegisterClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Transaction;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Deterministic lease/permit tests exercise committed ordering, retry fences and durable recovery without wall time.
public class MembershipStateTest {
  @Test
  public void testControlEpochUsesActualTermAndTicksCompareRoundAndPolicy()
      throws Exception {
    Fixture fixture = new Fixture();
    ActivateControlEpoch activation = new ActivateControlEpoch(1, "controller-a", UUID.randomUUID());
    assertEquals(fixture.apply(activation, 2).status(), Status.FENCED);
    assertNull(fixture._catalog.membershipControl().controlEpoch());
    assertEquals(fixture.apply(activation, 1).status(), Status.OK);
    long barrier = fixture._catalog.membershipControl().clockBarrierIndex();
    assertEquals(fixture.apply(activation, 1).control().clockBarrierIndex(), barrier);
    assertEquals(fixture.apply(new ActivateControlEpoch(1, "controller-b", UUID.randomUUID()), 1).status(),
        Status.CONFLICT);
    fixture.configure();
    Tick tick = fixture.tickRequest();
    assertEquals(fixture.apply(tick).status(), Status.OK);
    assertEquals(fixture.apply(tick).status(), Status.CONFLICT);
    assertEquals(fixture._catalog.membershipControl().round(), 1L);
    assertEquals(fixture.apply(tick, 2).status(), Status.NOT_READY);
    var oldEpoch = fixture._catalog.membershipControl().controlEpoch();
    assertEquals(fixture.apply(new ActivateControlEpoch(2, "controller-b", UUID.randomUUID()), 2).status(), Status.OK);
    assertEquals(fixture.apply(new Tick(oldEpoch, 1, 1), 2).status(), Status.FENCED);
    assertEquals(fixture._catalog.membershipControl().round(), 1L);
    assertEquals(fixture._catalog.membershipControl().clockBarrierIndex(), fixture._index - 1);
  }

  @Test
  public void testLiveIdCannotBeStolenAndExpiredIncarnationCannotRenew()
      throws Exception {
    Fixture fixture = Fixture.started();
    Register register = fixture.registration("server-a", Role.SERVER, 0);
    ParticipantToken original = fixture.apply(register).participant().participant();
    ParticipantState first = fixture.read("server-a");
    assertEquals(fixture.apply(register).participant(), first);
    Register replacement = fixture.registration("server-a", Role.SERVER, 1);
    assertEquals(fixture.apply(replacement).status(), Status.CONFLICT);
    for (int i = 0; i < 3; i++) {
      assertEquals(fixture.apply(fixture.tickRequest()).status(), Status.OK);
    }
    assertEquals(fixture.read("server-a").expiryRound(), 3L);
    assertEquals(fixture.renew(original, 1).status(), Status.FENCED);
    assertEquals(fixture.apply(register).status(), Status.FENCED);
    ParticipantToken current = fixture.apply(replacement).participant().participant();
    assertEquals(current.registrationEpoch(), 2L);
    assertNotEquals(current.incarnation(), original.incarnation());
    assertFalse(fixture.read("server-a").ready());
    assertEquals(fixture.apply(new SetReady(original, fixture.revision(), true)).status(), Status.FENCED);
    assertEquals(fixture.renew(current, 1).status(), Status.OK);
    assertEquals(fixture.apply(new Unregister(current)).status(), Status.OK);
    assertEquals(fixture.apply(new Unregister(current)).status(), Status.OK);
    assertEquals(fixture.renew(current, 2).status(), Status.FENCED);
    assertEquals(fixture.apply(fixture.registration("server-a", Role.BROKER, 2)).status(), Status.FENCED);
  }

  @Test
  public void testRenewalRetryPreservesGrantAndNewerGrantNarrowsCapabilities()
      throws Exception {
    Fixture fixture = Fixture.started();
    ParticipantToken token = fixture.registerReady("broker-a", Role.BROKER);
    Renewal renewal = new Renewal(token, 1, fixture.revision(), Set.of(Capability.BROKER_QUERY));
    RenewalResult first = fixture.apply(new RenewBatch(List.of(renewal))).renewals().getFirst();
    assertEquals(first.status(), Status.OK);
    assertEquals(first.permit().minAppliedRevision().index(), fixture._index);
    var image = fixture._catalog.captureImage(fixture._scope);
    assertEquals(fixture.apply(new RenewBatch(List.of(renewal))).renewals().getFirst(), first);
    assertTrue(fixture._catalog.changes(fixture._scope, image.cursor(), 10, 10, 100_000)
        .transactions().getFirst().records().isEmpty());
    Renewal conflicting = new Renewal(token, 1, fixture.revision(), Set.of());
    assertEquals(fixture.apply(new RenewBatch(List.of(conflicting))).renewals().getFirst().status(), Status.CONFLICT);
    Renewal narrowed = new Renewal(token, 2, fixture.revision(), Set.of());
    RenewalResult newer = fixture.apply(new RenewBatch(List.of(narrowed))).renewals().getFirst();
    assertEquals(newer.status(), Status.OK);
    assertTrue(newer.permit().capabilities().isEmpty());
    assertTrue(newer.permit().minAppliedRevision().index() > first.permit().minAppliedRevision().index());
    assertEquals(fixture.apply(new BeginDrain(token)).status(), Status.OK);
    assertEquals(fixture.apply(new SetReady(token, fixture.revision(), true)).status(), Status.NOT_READY);
    assertNull(fixture.renew(token, 3).permit());
  }

  @Test
  public void testBatchValidatesEachIdentityAndRetainsRejectedSequence()
      throws Exception {
    Fixture fixture = Fixture.started();
    ParticipantToken one = fixture.registerReady("server-a", Role.SERVER);
    ParticipantToken two = fixture.registerReady("server-b", Role.SERVER);
    ParticipantToken forged = new ParticipantToken(one.historyId(), one.participantId(), Role.BROKER,
        one.incarnation(), one.registrationEpoch());
    Response batch = fixture.apply(new RenewBatch(List.of(
        new Renewal(forged, 1, fixture.revision(), Set.of(Capability.BROKER_QUERY)),
        new Renewal(two, 1, fixture.revision(), Set.of(Capability.SERVER_QUERY)))));
    assertEquals(batch.status(), Status.OK);
    assertEquals(batch.renewals().get(0).status(), Status.FENCED);
    assertEquals(batch.renewals().get(1).status(), Status.OK);
    assertEquals(fixture.renew(one, 1).status(), Status.OK);
    Renewal rejected = new Renewal(one, 2, fixture.revision(), Set.of(Capability.BROKER_QUERY));
    RenewalResult failure = fixture.apply(new RenewBatch(List.of(rejected))).renewals().getFirst();
    assertEquals(failure.status(), Status.FENCED);
    fixture.apply(fixture.tickRequest());
    assertEquals(fixture.apply(new RenewBatch(List.of(rejected))).renewals().getFirst(), failure);
    assertEquals(fixture.renew(one, 2).status(), Status.CONFLICT);
    assertEquals(fixture.renew(one, 3).status(), Status.OK);
    long seq = fixture.read("server-a").lastRenewalSequence();
    Renewal duplicate = new Renewal(one, 4, fixture.revision(), Set.of(Capability.SERVER_QUERY));
    assertEquals(fixture.apply(new RenewBatch(List.of(duplicate, duplicate))).status(), Status.INVALID_REQUEST);
    assertEquals(fixture.read("server-a").lastRenewalSequence(), seq);
  }

  @Test
  public void testLagPolicyAndTermChangeDoNotMintFreshGrantForRetriedSequence()
      throws Exception {
    Fixture fixture = Fixture.started();
    ParticipantToken token = fixture.registerReady("broker-a", Role.BROKER);
    Renewal renewal = new Renewal(token, 1, fixture.revision(), Set.of(Capability.BROKER_QUERY));
    RenewalResult first = fixture.apply(new RenewBatch(List.of(renewal))).renewals().getFirst();
    fixture.apply(new ActivateControlEpoch(2, "controller-b", UUID.randomUUID()), 2);
    assertEquals(fixture.apply(new RenewBatch(List.of(renewal)), 2).renewals().getFirst(), first);
    var control = fixture._catalog.membershipControl();
    fixture.apply(new ConfigurePolicy(control.controlEpoch(), 1, new Policy(2, 100, 3, 200, 300, 1)), 2);
    Renewal stale = new Renewal(token, 2, new Revision(fixture._history, 1), Set.of(Capability.BROKER_QUERY));
    assertEquals(fixture.apply(new RenewBatch(List.of(stale)), 2).renewals().getFirst().status(),
        Status.INVALID_REQUEST);
    Renewal next = new Renewal(token, 3, fixture.revision(), Set.of(Capability.BROKER_QUERY));
    var fresh = fixture.apply(new RenewBatch(List.of(next)), 2).renewals().getFirst().permit();
    assertEquals(fresh.controlEpoch().raftTerm(), 2L);
    assertEquals(fresh.policyVersion(), 2L);
    Renewal behind = new Renewal(token, 4, fixture.revision(), Set.of(Capability.BROKER_QUERY));
    fixture.apply(new Tick(control.controlEpoch(), 0, 2), 2);
    assertEquals(fixture.apply(new RenewBatch(List.of(behind)), 2).renewals().getFirst().status(), Status.NOT_READY);
  }

  @Test
  public void testPolicyGracePreservesEarlierLeasesAcrossFailoverAndRecovery()
      throws Exception {
    Fixture fixture = Fixture.started();
    assertEquals(fixture._catalog.membershipControl().minimumTickDelayMillis(), 100L);
    assertEquals(fixture._catalog.membershipControl().maxConfiguredPermitLifetimeMillis(), 200L);
    ParticipantToken server = fixture.registerReady("server-a", Role.SERVER);
    assertEquals(fixture.renew(server, 1).permit().permitLifetimeMillis(), 200L);
    Tick oldPolicyTick = fixture.tickRequest();

    ConfigurePolicy longer = new ConfigurePolicy(oldPolicyTick.control(), 1,
        new Policy(2, 50, 9, 400, 600, 1000));
    assertEquals(fixture.apply(longer).status(), Status.OK);
    var firstGrace = fixture._catalog.membershipControl();
    assertEquals(firstGrace.minimumTickDelayMillis(), 200L);
    assertEquals(firstGrace.maxConfiguredPermitLifetimeMillis(), 400L);
    assertEquals(fixture.apply(longer).control(), firstGrace, "An exact policy retry must not reset the barrier");
    assertEquals(fixture.renew(server, 2).permit().permitLifetimeMillis(), 400L);
    assertEquals(fixture.read("server-a").expiryRound(), 9L);
    ParticipantToken broker = fixture.registerReady("broker-a", Role.BROKER);
    Renewal retained = new Renewal(broker, 1, fixture.revision(), Set.of(Capability.BROKER_QUERY));
    RenewalResult olderGrant = fixture.apply(new RenewBatch(List.of(retained))).renewals().getFirst();
    assertEquals(olderGrant.permit().permitLifetimeMillis(), 400L);

    assertEquals(fixture.apply(new ConfigurePolicy(oldPolicyTick.control(), 2,
        new Policy(3, 10, 2, 10, 20, 1000))).status(), Status.OK);
    var pendingGrace = fixture._catalog.membershipControl();
    assertEquals(pendingGrace.minimumTickDelayMillis(), 400L);
    assertEquals(pendingGrace.maxConfiguredPermitLifetimeMillis(), 400L);
    assertTrue(pendingGrace.clockBarrierIndex() > firstGrace.clockBarrierIndex());
    assertEquals(fixture.apply(oldPolicyTick).status(), Status.CONFLICT);
    assertEquals(fixture._catalog.membershipControl(), pendingGrace);

    // This shorter renewal commits, but its caller can still hold the earlier grant after losing the reply.
    Renewal lost = new Renewal(server, 3, fixture.revision(), Set.of(Capability.SERVER_QUERY));
    RenewalResult shorter = fixture.apply(new RenewBatch(List.of(lost))).renewals().getFirst();
    assertEquals(shorter.permit().permitLifetimeMillis(), 10L);
    assertEquals(fixture.read("server-a").expiryRound(), 9L, "A newer renewal must not shorten an earlier lease");
    fixture.restore();
    assertEquals(fixture._catalog.membershipControl(), pendingGrace);
    assertEquals(fixture.read("server-a").expiryRound(), 9L);

    Tick previousLeaderTick = fixture.tickRequest();
    assertEquals(fixture.apply(new ActivateControlEpoch(2, "controller-b", UUID.randomUUID()), 2).status(), Status.OK);
    var activated = fixture._catalog.membershipControl();
    assertEquals(activated.minimumTickDelayMillis(), 400L);
    assertEquals(activated.maxConfiguredPermitLifetimeMillis(), 400L);
    assertTrue(activated.clockBarrierIndex() > pendingGrace.clockBarrierIndex());
    assertEquals(fixture.apply(previousLeaderTick, 2).status(), Status.FENCED);
    assertEquals(fixture.apply(new RenewBatch(List.of(lost)), 2).renewals().getFirst(), shorter);
    assertEquals(fixture._catalog.membershipControl(), activated);

    // The driver separately proves the full grace elapsed before committing this tick.
    assertEquals(fixture.apply(fixture.tickRequest(), 2).status(), Status.OK);
    var afterGrace = fixture._catalog.membershipControl();
    assertEquals(afterGrace.round(), 1L);
    assertEquals(afterGrace.minimumTickDelayMillis(), 10L);
    assertEquals(afterGrace.maxConfiguredPermitLifetimeMillis(), 10L);
    fixture.restore();
    assertEquals(fixture._catalog.membershipControl(), afterGrace);
    assertEquals(fixture.read("server-a").expiryRound(), 9L);
    assertEquals(fixture.apply(new RenewBatch(List.of(retained)), 2).renewals().getFirst(), olderGrant,
        "Recovery retains an older grant's original outcome even after its grace has completed");
  }

  @Test
  public void testPolicyRequiresCompleteLeaseIntervalsWithoutMultiplicationOverflow()
      throws Exception {
    Fixture fixture = Fixture.started();
    var original = fixture._catalog.membershipControl();
    for (Policy invalid : List.of(new Policy(2, 100, 1, 1, 1, 1000),
        new Policy(2, 100, 3, 201, 300, 1000))) {
      assertEquals(fixture.apply(new ConfigurePolicy(original.controlEpoch(), 1, invalid)).status(),
          Status.INVALID_REQUEST);
      assertEquals(fixture._catalog.membershipControl(), original);
    }
    long maxMillis = Long.MAX_VALUE / 1_000_000;
    Policy large = new Policy(2, maxMillis, Long.MAX_VALUE, maxMillis, maxMillis, 1000);
    assertEquals(fixture.apply(new ConfigurePolicy(original.controlEpoch(), 1, large)).status(), Status.OK);
    assertEquals(fixture._catalog.membershipControl().policy(), large);
  }

  @Test
  public void testMembershipSnapshotJournalReplayAndAtomicCorruptionRejection()
      throws Exception {
    Fixture fixture = Fixture.started();
    ParticipantToken token = fixture.registerReady("server-a", Role.SERVER);
    Renewal renewal = new Renewal(token, 1, fixture.revision(), Set.of(Capability.SERVER_QUERY));
    RenewalResult outcome = fixture.apply(new RenewBatch(List.of(renewal))).renewals().getFirst();
    JsonNode snapshot = fixture._catalog.captureSnapshot();
    MetadataCatalog restored = new MetadataCatalog(fixture._cluster, fixture._history);
    restored.restoreSnapshot(snapshot);
    assertEquals(restored.captureSnapshot(), snapshot);
    assertEquals(restored.captureImage(fixture._scope), fixture._catalog.captureImage(fixture._scope));
    byte[] retry = MetadataProtocol.encode(new MembershipCommand(fixture._history, "participant", new RenewBatch(
        List.of(renewal))));
    assertEquals(MembershipProtocol.decodeResponse(restored.execute(retry, 1, fixture._index + 1))
        .renewals().getFirst(), outcome);
    assertEquals(fixture.apply(new RenewBatch(List.of(renewal))).renewals().getFirst(), outcome);
    byte[] tick = MetadataProtocol.encode(new MembershipCommand(fixture._history, "controller", fixture.tickRequest()));
    assertEquals(restored.execute(tick, 1, fixture._index + 1), fixture._catalog.execute(tick, 1, fixture._index + 1));
    assertEquals(restored.captureSnapshot(), fixture._catalog.captureSnapshot());
    JsonNode before = restored.captureSnapshot();
    ObjectNode broken = before.deepCopy();
    ((ObjectNode) broken.at("/membership/members/server-a/state")).put("expiryRound", 999);
    assertThrows(IOException.class, () -> restored.restoreSnapshot(broken));
    assertEquals(restored.captureSnapshot(), before);
    ObjectNode brokenOutcome = before.deepCopy();
    ((ObjectNode) brokenOutcome.at("/membership/members/server-a/outcome/permit/controlEpoch")).put("raftTerm", 100);
    assertThrows(IOException.class, () -> restored.restoreSnapshot(brokenOutcome));
    assertEquals(restored.captureSnapshot(), before);
    MetadataCatalog empty = new MetadataCatalog(fixture._cluster, fixture._history);
    ObjectNode v2 = empty.captureSnapshot().deepCopy();
    v2.put("formatVersion", 2);
    v2.remove("membership");
    empty.restoreSnapshot(v2);
    assertNull(empty.membershipControl().controlEpoch());
    assertEquals(empty.captureSnapshot().path("formatVersion").asInt(), 10);
  }

  @Test
  public void testGenericCatalogCannotCreateMembershipAndStrictReplayRejectsNewProtocol()
      throws Exception {
    Fixture fixture = new Fixture();
    fixture._catalog.execute(MetadataProtocol.encode(new RegisterClient(fixture._history, "client",
        UUID.randomUUID(), 0)), 1);
    Mutation forged = new Mutation(Action.CREATE, RecordKind.MEMBERSHIP_CONTROL, MembershipProtocol.CONTROL_NAME,
        new ObjectVersion(fixture._history, UUID.randomUUID(), 0), MetadataProtocol.toTree(
            fixture._catalog.membershipControl()));
    byte[] transaction = MetadataProtocol.encode(new Transaction(fixture._history, "client", 1, "stream", 1,
        List.of(forged)));
    assertEquals(MetadataProtocol.decodeResult(fixture._catalog.execute(transaction, 2)).status(),
        Status.INVALID_REQUEST);
    assertNull(fixture._catalog.membershipControl().controlEpoch());
    byte[] valid = MetadataProtocol.encode(new MembershipCommand(fixture._history, "controller",
        new ActivateControlEpoch(1, "controller-a", UUID.randomUUID())));
    String encoded = new String(valid, StandardCharsets.UTF_8);
    for (String invalid : List.of(encoded.replace("ACTIVATE_CONTROL_EPOCH", "NEW_OPERATION"),
        encoded.replace("\"term\":1", "\"term\":1.5"), encoded + " {}")) {
      assertThrows(IOException.class, () -> fixture._catalog.execute(invalid.getBytes(StandardCharsets.UTF_8), 1, 3));
      assertEquals(fixture._catalog.currentRevision().index(), 2L);
    }
  }

  private static final class Fixture {
    private final UUID _cluster = UUID.randomUUID();
    private final UUID _history = UUID.randomUUID();
    private MetadataCatalog _catalog = new MetadataCatalog(_cluster, _history);
    private final FeedScope _scope =
        new FeedScope(Set.of(RecordKind.MEMBERSHIP_CONTROL, RecordKind.PARTICIPANT), "test");
    private long _index;

    private static Fixture started()
        throws Exception {
      Fixture fixture = new Fixture();
      fixture.apply(new ActivateControlEpoch(1, "controller-a", UUID.randomUUID()));
      fixture.configure();
      return fixture;
    }

    private void configure()
        throws Exception {
      assertEquals(apply(new ConfigurePolicy(_catalog.membershipControl().controlEpoch(), 0,
          new Policy(1, 100, 3, 200, 300, 1000))).status(), Status.OK);
    }

    private void restore()
        throws IOException {
      byte[] snapshot = MetadataProtocol.encodeValue(_catalog.captureSnapshot());
      MetadataCatalog restored = new MetadataCatalog(_cluster, _history);
      // Exercise the default integral-node decoding used by disk snapshots, not an in-memory JsonNode copy.
      restored.restoreSnapshot(new ObjectMapper().readTree(snapshot));
      assertEquals(MetadataProtocol.encodeValue(restored.captureSnapshot()), snapshot);
      _catalog = restored;
    }

    private Response apply(Operation operation)
        throws Exception {
      return apply(operation, 1);
    }

    private Response apply(Operation operation, long actualTerm)
        throws Exception {
      return MembershipProtocol.decodeResponse(_catalog.execute(MetadataProtocol.encode(
          new MembershipCommand(_history, "test-client", operation)), actualTerm, ++_index));
    }

    private Register registration(String id, Role role, long expectedEpoch) {
      Set<Capability> capabilities = role == Role.BROKER ? Set.of(Capability.BROKER_QUERY)
          : Set.of(Capability.SERVER_QUERY);
      return new Register(id, role, UUID.randomUUID(), expectedEpoch, Map.of("query", "localhost:8098"), capabilities);
    }

    private ParticipantToken registerReady(String id, Role role)
        throws Exception {
      ParticipantToken token = apply(registration(id, role, 0)).participant().participant();
      assertEquals(apply(new SetReady(token, revision(), true)).status(), Status.OK);
      return token;
    }

    private RenewalResult renew(ParticipantToken token, long sequence)
        throws Exception {
      Set<Capability> capabilities = token.role() == Role.BROKER ? Set.of(Capability.BROKER_QUERY)
          : Set.of(Capability.SERVER_QUERY);
      return apply(new RenewBatch(List.of(new Renewal(token, sequence, revision(), capabilities))))
          .renewals().getFirst();
    }

    private ParticipantState read(String id)
        throws Exception {
      return MembershipProtocol.decodeResponse(_catalog.query(MetadataProtocol.encode(
          new MembershipQuery(_history, new GetParticipant(id))))).participant();
    }

    private Revision revision() {
      return new Revision(_history, _index);
    }

    private Tick tickRequest() {
      var control = _catalog.membershipControl();
      return new Tick(control.controlEpoch(), control.round(), control.policy().version());
    }
  }
}
