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
package org.apache.pinot.common.coordination;

import com.google.protobuf.UnknownFieldSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.coordination.QueryDeadlineGuard.TimeInterval;
import org.apache.pinot.common.coordination.ServingPermitGuard.ClockReading;
import org.apache.pinot.common.coordination.ServingPermitGuard.MembershipView;
import org.apache.pinot.common.coordination.ServingPermitGuard.QualifiedLifetimePolicy;
import org.apache.pinot.common.coordination.ServingPermitGuard.QueryTicket;
import org.apache.pinot.common.coordination.ServingPermitGuard.ReadOnlyCapability;
import org.apache.pinot.common.coordination.ServingPermitGuard.ServingDeniedException;
import org.apache.pinot.common.request.context.NativeQueryDeadline;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ControlToken;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ParticipantRole;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ParticipantToken;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.Revision;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ServingPermit;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


/// Model checks use an explicitly qualified fake clock; no production clock, coordinator RPC or sleeping is used.
/// They exercise delayed/retried grants, coherent-image fences and result delivery after pauses or revocation.
public class ServingPermitGuardTest {
  @Test
  public void testFirstSendSurvivesRetryDuplicateAndLostRenewal() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    fixture._clock.millis(40);
    fixture._guard.firstSend(1);
    ServingPermit first = fixture.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY));
    fixture._guard.accept(first);
    fixture._clock.millis(70);
    fixture._guard.accept(first);
    QueryTicket ticket = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
    fixture._clock.millis(99);
    fixture._guard.checkDelivery(ticket);
    fixture._guard.firstSend(2);
    fixture._clock.millis(100);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(ticket));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    // A duplicate of the old committed response and a lost new renewal cannot restart its lifetime.
    fixture._guard.accept(first);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));

    Fixture delayed = new Fixture();
    delayed._guard.firstSend(1);
    delayed._clock.millis(101);
    delayed._guard.accept(delayed.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    expectThrows(ServingDeniedException.class, () -> delayed._guard.admit(ReadOnlyCapability.BROKER_QUERY));
  }

  @Test
  public void testNarrowingFencesImmediatelyWhileImageCatchesUp() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    fixture._guard.accept(fixture.grant(1, 10,
        Set.of(ReadOnlyCapability.BROKER_QUERY, ReadOnlyCapability.SERVER_QUERY)));
    QueryTicket brokerTicket = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
    fixture._clock.millis(5);
    fixture._guard.firstSend(2);
    fixture._guard.accept(fixture.grant(2, 20, Set.of(ReadOnlyCapability.SERVER_QUERY)));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.SERVER_QUERY));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(brokerTicket));
    fixture.image(20, fixture._control, fixture._participant, true, true, false);
    fixture._guard.admit(ReadOnlyCapability.SERVER_QUERY);
    fixture._cacheFailed.set(true);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.SERVER_QUERY));
    fixture._cacheFailed.set(false);
    fixture._guard.firstSend(3);
    fixture._guard.accept(fixture.grant(3, 20,
        Set.of(ReadOnlyCapability.BROKER_QUERY, ReadOnlyCapability.SERVER_QUERY)));
    fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(brokerTicket));
    fixture._guard.accept(fixture.grant(2, 20, Set.of(ReadOnlyCapability.SERVER_QUERY)));
    fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
  }

  @Test
  public void testControlAndRegistrationFencesDoNotResetRetryClock() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    ServingPermit original = fixture.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY));
    fixture._guard.accept(original);
    QueryTicket oldTicket = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
    fixture._clock.millis(20);
    fixture._guard.firstSend(2);
    ControlToken next = control(fixture._history, 2, 11, "leader-2", UUID.randomUUID());
    fixture.image(20, next, fixture._participant, true, true, false);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    fixture._guard.accept(original);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    fixture._clock.millis(80);
    fixture._guard.firstSend(2);
    fixture._guard.accept(permit(fixture._participant, 2, next, revision(fixture._history, 20),
        100, 50, 1, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    QueryTicket current = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(oldTicket));
    fixture._clock.millis(119);
    fixture._guard.checkDelivery(current);
    fixture._clock.millis(120);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(current));

    Fixture replaced = new Fixture();
    replaced._guard.firstSend(1);
    replaced._guard.accept(replaced.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    ParticipantToken replacement = participant(replaced._history, replaced._participant.getParticipantId(),
        ParticipantRole.BROKER, UUID.randomUUID(), 2);
    replaced.image(11, replaced._control, replacement, true, true, false);
    expectThrows(ServingDeniedException.class, () -> replaced._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    replaced.image(12, replaced._control, replaced._participant, true, true, false);
    expectThrows(ServingDeniedException.class, () -> replaced._guard.admit(ReadOnlyCapability.BROKER_QUERY));
  }

  @Test
  public void testDiscardedControlGrantStillRejectsConflictingDuplicate() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    fixture._guard.accept(fixture.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    fixture._guard.firstSend(2);
    fixture.image(20, control(fixture._history, 2, 11, "leader-2", UUID.randomUUID()), fixture._participant,
        true, true, false);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    ServingPermit stale = fixture.grant(2, 10, Set.of(ReadOnlyCapability.BROKER_QUERY));
    fixture._guard.accept(stale);
    fixture._guard.accept(stale);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    expectThrows(ServingDeniedException.class,
        () -> fixture._guard.accept(stale.toBuilder().clearCapabilities().build()));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.firstSend(3));
  }

  @Test
  public void testTicketsAreFiniteProcessBoundAndRespectDrain() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    fixture._guard.accept(fixture.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    QueryTicket ticket = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
    fixture._clock.millis(10);
    fixture._guard.firstSend(2);
    fixture._guard.accept(fixture.grant(2, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    fixture.image(11, fixture._control, fixture._participant, true, false, false);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(ticket));
    fixture.image(12, fixture._control, fixture._participant, true, false, true);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    fixture._guard.checkDelivery(ticket);
    fixture._guard.drain();
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    fixture._guard.checkDelivery(ticket);
    Fixture other = new Fixture();
    expectThrows(ServingDeniedException.class, () -> other._guard.checkDelivery(ticket));
    fixture._clock.millis(50);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(ticket));
    fixture._clock.millis(10_000);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(ticket));
    fixture._guard.invalidate();
    expectThrows(ServingDeniedException.class, () -> fixture._guard.firstSend(3));
  }

  @Test
  public void testClockRegressionUncertaintyAndConflictingDuplicateFailClosed() {
    Fixture regression = new Fixture();
    regression._clock.millis(10);
    regression._guard.firstSend(1);
    regression._guard.accept(regression.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    regression._clock.millis(9);
    expectThrows(ServingDeniedException.class, () -> regression._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    regression._clock.millis(20);
    expectThrows(ServingDeniedException.class, () -> regression._guard.admit(ReadOnlyCapability.BROKER_QUERY));

    Fixture uncertain = new Fixture();
    uncertain._guard.firstSend(1);
    uncertain._guard.accept(uncertain.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    uncertain._clock._uncertainty = 1_000_001;
    expectThrows(ServingDeniedException.class, () -> uncertain._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    uncertain._clock._uncertainty = 0;
    expectThrows(ServingDeniedException.class, () -> uncertain._guard.admit(ReadOnlyCapability.BROKER_QUERY));

    Fixture conflict = new Fixture();
    conflict._guard.firstSend(1);
    conflict._guard.accept(conflict.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    expectThrows(ServingDeniedException.class, () -> conflict._guard.accept(conflict.grant(1, 10, Set.of())));
    expectThrows(ServingDeniedException.class, () -> conflict._guard.admit(ReadOnlyCapability.BROKER_QUERY));

    Fixture overflow = new Fixture();
    overflow._clock._nanos = Long.MAX_VALUE;
    overflow._clock._uncertainty = 1;
    expectThrows(ServingDeniedException.class, () -> overflow._guard.firstSend(1));
    overflow._clock._nanos = 0;
    overflow._clock._uncertainty = 0;
    expectThrows(ServingDeniedException.class, () -> overflow._guard.firstSend(1));
  }

  @Test
  public void testPauseDuringViewReadCannotUseEarlierClockSample() {
    Fixture admission = new Fixture();
    admission._guard.firstSend(1);
    admission._guard.accept(admission.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    admission._advanceClockOnView.set(101);
    expectThrows(ServingDeniedException.class, () -> admission._guard.admit(ReadOnlyCapability.BROKER_QUERY));

    Fixture delivery = new Fixture();
    delivery._guard.firstSend(1);
    delivery._guard.accept(delivery.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    QueryTicket ticket = delivery._guard.admit(ReadOnlyCapability.BROKER_QUERY);
    delivery._advanceClockOnView.set(51);
    expectThrows(ServingDeniedException.class, () -> delivery._guard.checkDelivery(ticket));
  }

  @Test
  public void testLifetimeReductionPermanentlyRevokesEarlierTickets() {
    for (boolean permitReduction : List.of(true, false)) {
      Fixture fixture = new Fixture();
      fixture._guard.firstSend(1);
      fixture._guard.accept(fixture.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
      QueryTicket original = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
      fixture._clock.millis(5);
      fixture._guard.firstSend(2);
      fixture._guard.accept(permit(fixture._participant, 2, fixture._control,
          revision(fixture._history, 10), permitReduction ? 20 : 100, permitReduction ? 50 : 10, 1,
          Set.of(ReadOnlyCapability.BROKER_QUERY)));
      expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(original));
      QueryTicket narrowed = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
      fixture._guard.firstSend(3);
      fixture._guard.accept(fixture.grant(3, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
      expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(original));
      fixture._clock.millis(permitReduction ? 25 : 15);
      expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(narrowed));
    }
    Fixture fixture = new Fixture();
    ServingPermitGuard withMargin =
        new ServingPermitGuard(fixture._participant, Set.of(ReadOnlyCapability.BROKER_QUERY), fixture._clock,
            new QualifiedLifetimePolicy(100, 50, 1_000_000, 5, 1_000_000), fixture._image::get);
    withMargin.firstSend(1);
    withMargin.accept(fixture.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    QueryTicket ticket = withMargin.admit(ReadOnlyCapability.BROKER_QUERY);
    withMargin.firstSend(2);
    ServingPermit tooShort = permit(fixture._participant, 2, fixture._control,
        revision(fixture._history, 10), 1, 1, 1, Set.of());
    expectThrows(ServingDeniedException.class, () -> withMargin.accept(tooShort));
    expectThrows(ServingDeniedException.class, () -> withMargin.checkDelivery(ticket));
    expectThrows(ServingDeniedException.class, () -> withMargin.admit(ReadOnlyCapability.BROKER_QUERY));
    withMargin.accept(tooShort);
    expectThrows(ServingDeniedException.class, () -> withMargin.admit(ReadOnlyCapability.BROKER_QUERY));
  }

  @Test
  public void testImageRegressionAndPolicyVersionFence() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    fixture._guard.accept(fixture.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    QueryTicket ticket = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
    fixture.image(11, fixture._control, fixture._participant, true, false, false);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    fixture.image(10, fixture._control, fixture._participant, true, true, false);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    fixture._image.set(new MembershipView(revision(fixture._history, 12), fixture._control,
        fixture._participant, true, true, false, Set.of(ReadOnlyCapability.BROKER_QUERY), 2));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY));
    fixture._guard.firstSend(2);
    fixture._guard.accept(permit(fixture._participant, 2, fixture._control,
        revision(fixture._history, 12), 100, 50, 2, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(ticket));
  }

  @Test
  public void testWireNormalizationAndMalformedAuthorityFailClosed() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    ServingPermit grant = fixture.grant(1, 10,
        Set.of(ReadOnlyCapability.BROKER_QUERY, ReadOnlyCapability.SERVER_QUERY));
    fixture._guard.accept(grant);
    fixture._clock.millis(70);
    fixture._guard.accept(grant.toBuilder().clearCapabilities()
        .addCapabilities(MetadataRpc.ServingCapability.SERVER_QUERY)
        .addCapabilities(MetadataRpc.ServingCapability.BROKER_QUERY)
        .setParticipant(grant.getParticipant().toBuilder()
            .setHistoryId(grant.getParticipant().getHistoryId().toUpperCase(Locale.ROOT))).build());
    QueryTicket ticket = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY);
    fixture._clock.millis(100);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(ticket));

    UnknownFieldSet unknown = UnknownFieldSet.newBuilder().addField(99,
        UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    for (ServingPermit malformed : List.of(grant.toBuilder().setUnknownFields(unknown).build(),
        grant.toBuilder().setControlEpoch(grant.getControlEpoch().toBuilder().setUnknownFields(unknown)).build(),
        grant.toBuilder().setMinAppliedRevision(grant.getMinAppliedRevision().toBuilder()
            .setUnknownFields(unknown)).build(),
        grant.toBuilder().setParticipant(grant.getParticipant().toBuilder().setRoleValue(99)).build(),
        grant.toBuilder().setParticipant(grant.getParticipant().toBuilder()
            .setRole(MetadataRpc.ParticipantRole.PARTICIPANT_ROLE_UNSPECIFIED)).build(),
        grant.toBuilder().setControlEpoch(grant.getControlEpoch().toBuilder().setActivationId("1-1-1-1-1")).build(),
        grant.toBuilder().setMinAppliedRevision(grant.getMinAppliedRevision().toBuilder()
            .setHistoryId("not-a-uuid")).build(),
        grant.toBuilder().clearCapabilities().addCapabilitiesValue(99).build(),
        grant.toBuilder().clearCapabilities()
            .addCapabilities(MetadataRpc.ServingCapability.SERVING_CAPABILITY_UNSPECIFIED).build(),
        grant.toBuilder().clearCapabilities().addCapabilities(MetadataRpc.ServingCapability.BROKER_QUERY)
            .addCapabilities(MetadataRpc.ServingCapability.BROKER_QUERY).build())) {
      Fixture rejected = new Fixture();
      rejected._guard.firstSend(1);
      expectThrows(ServingDeniedException.class, () -> rejected._guard.accept(malformed));
      expectThrows(ServingDeniedException.class, () -> rejected._guard.firstSend(1));
    }
    expectThrows(IllegalArgumentException.class, () -> new MembershipView(
        revision(fixture._history, 10).toBuilder().setUnknownFields(unknown).build(), fixture._control,
        fixture._participant, true, true, false, Set.of(ReadOnlyCapability.BROKER_QUERY), 1));
    expectThrows(IllegalArgumentException.class, () -> new ServingPermitGuard(
        fixture._participant.toBuilder().setIncarnation("bad-id").build(), Set.of(ReadOnlyCapability.BROKER_QUERY),
        fixture._clock, new QualifiedLifetimePolicy(100, 50, 1_000_000, 0, 1_000_000), fixture._image::get));
    assertEquals(fixture._image.get(), new MembershipView(revision(fixture._history, 10), fixture._control,
        fixture._participant, true, true, false,
        Set.of(ReadOnlyCapability.BROKER_QUERY, ReadOnlyCapability.SERVER_QUERY), 1));
  }

  @Test
  public void testAbsoluteBudgetUsesConservativeLocalRateAndNeverResets() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    fixture._guard.accept(fixture.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    AtomicReference<TimeInterval> sharedClock = new AtomicReference<>(new TimeInterval(1000, 1000, true));
    QueryDeadlineGuard deadlines = new QueryDeadlineGuard(sharedClock::get, "test-utc", 50, 0, 500_000);
    NativeQueryDeadline deadline = new NativeQueryDeadline("test-utc", 990, 1030);
    QueryTicket ticket = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY, deadline, deadlines);
    fixture._clock.millis(10);
    sharedClock.set(new TimeInterval(1010, 1010, true));
    fixture._cacheFailed.set(true);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(ticket));
    fixture._cacheFailed.set(false);
    fixture._guard.checkDelivery(ticket);
    fixture._guard.firstSend(2);
    fixture._guard.accept(fixture.grant(2, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    sharedClock.set(new TimeInterval(1000, 1000, true));
    fixture._clock.millis(15);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(ticket));

    // Conservative uncertainty can shrink later, but it cannot revive a ticket already refused at its deadline.
    Fixture uncertain = new Fixture();
    uncertain._guard.firstSend(1);
    uncertain._guard.accept(uncertain.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    QueryTicket finite = uncertain._guard.admit(ReadOnlyCapability.BROKER_QUERY);
    uncertain._clock.millis(49);
    uncertain._clock._uncertainty = 1_000_000;
    expectThrows(ServingDeniedException.class, () -> uncertain._guard.checkDelivery(finite));
    uncertain._clock._uncertainty = 0;
    expectThrows(ServingDeniedException.class, () -> uncertain._guard.checkDelivery(finite));
  }

  @Test
  public void testPauseBetweenClockObservationsCannotExtendAdmissionOrDelivery() {
    Fixture admission = new Fixture();
    admission._guard.firstSend(1);
    admission._guard.accept(admission.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    NativeQueryDeadline deadline = new NativeQueryDeadline("test-utc", 1000, 1030);
    QueryDeadlineGuard delayed = new QueryDeadlineGuard(() -> {
      TimeInterval sampled = new TimeInterval(1000, 1000, true);
      admission._clock.millis(31);
      return sampled;
    }, "test-utc", 50, 0, 1_000_000);
    expectThrows(ServingDeniedException.class,
        () -> admission._guard.admit(ReadOnlyCapability.BROKER_QUERY, deadline, delayed));

    Fixture delivery = new Fixture();
    delivery._guard.firstSend(1);
    delivery._guard.accept(delivery.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    AtomicBoolean pause = new AtomicBoolean();
    QueryDeadlineGuard delayedDelivery = new QueryDeadlineGuard(() -> {
      TimeInterval sampled = new TimeInterval(1000, 1000, true);
      if (pause.get()) {
        delivery._clock.millis(31);
      }
      return sampled;
    }, "test-utc", 50, 0, 1_000_000);
    QueryTicket ticket = delivery._guard.admit(ReadOnlyCapability.BROKER_QUERY, deadline, delayedDelivery);
    pause.set(true);
    expectThrows(ServingDeniedException.class, () -> delivery._guard.checkDelivery(ticket));
  }

  @Test
  public void testAbsoluteExpiryAndClockFailurePermanentlyRejectTicket() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    fixture._guard.accept(fixture.grant(1, 10, Set.of(ReadOnlyCapability.BROKER_QUERY)));
    AtomicReference<TimeInterval> sharedClock = new AtomicReference<>(new TimeInterval(1000, 1000, true));
    QueryDeadlineGuard deadlines = new QueryDeadlineGuard(sharedClock::get, "test-utc", 50, 0, 1_000_000);
    NativeQueryDeadline deadline = new NativeQueryDeadline("test-utc", 1000, 1030);
    QueryTicket expired = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY, deadline, deadlines);
    sharedClock.set(new TimeInterval(1030, 1030, true));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(expired));
    sharedClock.set(new TimeInterval(1010, 1010, true));
    deadlines.check(deadline);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(expired));

    QueryTicket unqualified = fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY, deadline, deadlines);
    sharedClock.set(new TimeInterval(1010, 1010, false));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(unqualified));
    sharedClock.set(new TimeInterval(1010, 1010, true));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(unqualified));
    expectThrows(ServingDeniedException.class,
        () -> fixture._guard.admit(ReadOnlyCapability.BROKER_QUERY, deadline, deadlines));
  }

  private static ParticipantToken participant(UUID history, String name, ParticipantRole role, UUID incarnation,
      long epoch) {
    return ParticipantToken.newBuilder().setHistoryId(history.toString()).setParticipantId(name).setRole(role)
        .setIncarnation(incarnation.toString()).setRegistrationEpoch(epoch).build();
  }

  private static ControlToken control(UUID history, long term, long barrier, String leader, UUID activation) {
    return ControlToken.newBuilder().setHistoryId(history.toString()).setRaftTerm(term).setBarrierIndex(barrier)
        .setLeaderId(leader).setActivationId(activation.toString()).build();
  }

  private static Revision revision(UUID history, long index) {
    return Revision.newBuilder().setHistoryId(history.toString()).setIndex(index).build();
  }

  private static ServingPermit permit(ParticipantToken participant, long sequence, ControlToken control,
      Revision revision, long lifetime, long queryLifetime, long policy, Set<ReadOnlyCapability> capabilities) {
    ServingPermit.Builder grant = ServingPermit.newBuilder().setParticipant(participant).setRenewalSequence(sequence)
        .setControlEpoch(control).setMinAppliedRevision(revision).setPermitLifetimeMillis(lifetime)
        .setMaxQueryLifetimeMillis(queryLifetime).setPolicyVersion(policy);
    for (ReadOnlyCapability capability : capabilities) {
      grant.addCapabilities(capability.toWire());
    }
    return grant.build();
  }

  private static final class FakeClock implements ServingPermitGuard.MonotonicClock {
    private long _nanos;
    private long _uncertainty;

    private void millis(long millis) {
      _nanos = millis * 1_000_000;
    }

    @Override
    public ClockReading read() {
      return new ClockReading(_nanos, _uncertainty, true);
    }
  }

  private static final class Fixture {
    private final UUID _history = UUID.randomUUID();
    private final ParticipantToken _participant = participant(_history, "broker-1", ParticipantRole.BROKER,
        UUID.randomUUID(), 1);
    private final ControlToken _control = control(_history, 1, 1, "leader-1", UUID.randomUUID());
    private final FakeClock _clock = new FakeClock();
    private final AtomicBoolean _cacheFailed = new AtomicBoolean();
    private final AtomicLong _advanceClockOnView = new AtomicLong(-1);
    private final AtomicReference<MembershipView> _image = new AtomicReference<>();
    private final ServingPermitGuard _guard = new ServingPermitGuard(_participant,
        Set.of(ReadOnlyCapability.BROKER_QUERY, ReadOnlyCapability.SERVER_QUERY), _clock,
        new QualifiedLifetimePolicy(100, 50, 1_000_000, 0, 1_000_000), () -> {
          if (_cacheFailed.get()) {
            throw new IllegalStateException("Metadata cache requires a fresh snapshot");
          }
          long advance = _advanceClockOnView.getAndSet(-1);
          if (advance >= 0) {
            _clock.millis(advance);
          }
          return _image.get();
        });

    private Fixture() {
      image(10, _control, _participant, true, true, false);
    }

    private ServingPermit grant(long sequence, long revision, Set<ReadOnlyCapability> capabilities) {
      return permit(_participant, sequence, _control, revision(_history, revision), 100, 50, 1,
          capabilities);
    }

    private void image(long revision, ControlToken control, ParticipantToken participant, boolean live, boolean ready,
        boolean draining) {
      _image.set(new MembershipView(revision(_history, revision), control, participant, live, ready, draining,
          Set.of(ReadOnlyCapability.BROKER_QUERY, ReadOnlyCapability.SERVER_QUERY), 1));
    }
  }
}
