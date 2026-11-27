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
package org.apache.pinot.coordination.ratis.client;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.coordination.ratis.client.ServingPermitGuard.ClockReading;
import org.apache.pinot.coordination.ratis.client.ServingPermitGuard.MembershipView;
import org.apache.pinot.coordination.ratis.client.ServingPermitGuard.QualifiedLifetimePolicy;
import org.apache.pinot.coordination.ratis.client.ServingPermitGuard.QueryTicket;
import org.apache.pinot.coordination.ratis.client.ServingPermitGuard.ServingDeniedException;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ServingPermit;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
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
    ServingPermit first = fixture.grant(1, 10, Set.of(Capability.BROKER_QUERY));
    fixture._guard.accept(first);
    fixture._clock.millis(70);
    fixture._guard.accept(first);
    QueryTicket ticket = fixture._guard.admit(Capability.BROKER_QUERY);
    fixture._clock.millis(99);
    fixture._guard.checkDelivery(ticket);
    fixture._guard.firstSend(2);
    fixture._clock.millis(100);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(ticket));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.BROKER_QUERY));
    // A duplicate of the old committed response and a lost new renewal cannot restart its lifetime.
    fixture._guard.accept(first);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.BROKER_QUERY));

    Fixture delayed = new Fixture();
    delayed._guard.firstSend(1);
    delayed._clock.millis(101);
    delayed._guard.accept(delayed.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
    expectThrows(ServingDeniedException.class, () -> delayed._guard.admit(Capability.BROKER_QUERY));
  }

  @Test
  public void testNarrowingFencesImmediatelyWhileImageCatchesUp() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    fixture._guard.accept(fixture.grant(1, 10, Set.of(Capability.BROKER_QUERY, Capability.SERVER_QUERY)));
    QueryTicket brokerTicket = fixture._guard.admit(Capability.BROKER_QUERY);
    fixture._clock.millis(5);
    fixture._guard.firstSend(2);
    fixture._guard.accept(fixture.grant(2, 20, Set.of(Capability.SERVER_QUERY)));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.BROKER_QUERY));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.SERVER_QUERY));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(brokerTicket));
    fixture.image(20, fixture._control, fixture._participant, true, true, false);
    fixture._guard.admit(Capability.SERVER_QUERY);
    fixture._cacheFailed.set(true);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.SERVER_QUERY));
    fixture._cacheFailed.set(false);
    fixture._guard.firstSend(3);
    fixture._guard.accept(fixture.grant(3, 20, Set.of(Capability.BROKER_QUERY, Capability.SERVER_QUERY)));
    fixture._guard.admit(Capability.BROKER_QUERY);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(brokerTicket));
    fixture._guard.accept(fixture.grant(2, 20, Set.of(Capability.SERVER_QUERY)));
    fixture._guard.admit(Capability.BROKER_QUERY);
  }

  @Test
  public void testControlAndRegistrationFencesDoNotResetRetryClock() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    ServingPermit original = fixture.grant(1, 10, Set.of(Capability.BROKER_QUERY));
    fixture._guard.accept(original);
    QueryTicket oldTicket = fixture._guard.admit(Capability.BROKER_QUERY);
    fixture._clock.millis(20);
    fixture._guard.firstSend(2);
    ControlToken next = new ControlToken(fixture._history, 2, 11, "leader-2", UUID.randomUUID());
    fixture.image(20, next, fixture._participant, true, true, false);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.BROKER_QUERY));
    fixture._guard.accept(original);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.BROKER_QUERY));
    fixture._clock.millis(80);
    fixture._guard.firstSend(2);
    fixture._guard.accept(new ServingPermit(fixture._participant, 2, next, new Revision(fixture._history, 20),
        100, 50, 1, Set.of(Capability.BROKER_QUERY)));
    QueryTicket current = fixture._guard.admit(Capability.BROKER_QUERY);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(oldTicket));
    fixture._clock.millis(119);
    fixture._guard.checkDelivery(current);
    fixture._clock.millis(120);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(current));

    Fixture replaced = new Fixture();
    replaced._guard.firstSend(1);
    replaced._guard.accept(replaced.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
    ParticipantToken replacement = new ParticipantToken(replaced._history, replaced._participant.participantId(),
        Role.BROKER, UUID.randomUUID(), 2);
    replaced.image(11, replaced._control, replacement, true, true, false);
    expectThrows(ServingDeniedException.class, () -> replaced._guard.admit(Capability.BROKER_QUERY));
    replaced.image(12, replaced._control, replaced._participant, true, true, false);
    expectThrows(ServingDeniedException.class, () -> replaced._guard.admit(Capability.BROKER_QUERY));
  }

  @Test
  public void testTicketsAreFiniteProcessBoundAndRespectDrain() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    fixture._guard.accept(fixture.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
    QueryTicket ticket = fixture._guard.admit(Capability.BROKER_QUERY);
    fixture._clock.millis(10);
    fixture._guard.firstSend(2);
    fixture._guard.accept(fixture.grant(2, 10, Set.of(Capability.BROKER_QUERY)));
    fixture.image(11, fixture._control, fixture._participant, true, false, false);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(ticket));
    fixture.image(12, fixture._control, fixture._participant, true, false, true);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.BROKER_QUERY));
    fixture._guard.checkDelivery(ticket);
    fixture._guard.drain();
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.BROKER_QUERY));
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
    regression._guard.accept(regression.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
    regression._clock.millis(9);
    expectThrows(ServingDeniedException.class, () -> regression._guard.admit(Capability.BROKER_QUERY));
    regression._clock.millis(20);
    expectThrows(ServingDeniedException.class, () -> regression._guard.admit(Capability.BROKER_QUERY));

    Fixture uncertain = new Fixture();
    uncertain._guard.firstSend(1);
    uncertain._guard.accept(uncertain.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
    uncertain._clock._uncertainty = 1_000_001;
    expectThrows(ServingDeniedException.class, () -> uncertain._guard.admit(Capability.BROKER_QUERY));
    uncertain._clock._uncertainty = 0;
    expectThrows(ServingDeniedException.class, () -> uncertain._guard.admit(Capability.BROKER_QUERY));

    Fixture conflict = new Fixture();
    conflict._guard.firstSend(1);
    conflict._guard.accept(conflict.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
    expectThrows(ServingDeniedException.class, () -> conflict._guard.accept(conflict.grant(1, 10, Set.of())));
    expectThrows(ServingDeniedException.class, () -> conflict._guard.admit(Capability.BROKER_QUERY));

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
    admission._guard.accept(admission.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
    admission._advanceClockOnView.set(101);
    expectThrows(ServingDeniedException.class, () -> admission._guard.admit(Capability.BROKER_QUERY));

    Fixture delivery = new Fixture();
    delivery._guard.firstSend(1);
    delivery._guard.accept(delivery.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
    QueryTicket ticket = delivery._guard.admit(Capability.BROKER_QUERY);
    delivery._advanceClockOnView.set(51);
    expectThrows(ServingDeniedException.class, () -> delivery._guard.checkDelivery(ticket));
  }

  @Test
  public void testLifetimeReductionPermanentlyRevokesEarlierTickets() {
    for (boolean permitReduction : List.of(true, false)) {
      Fixture fixture = new Fixture();
      fixture._guard.firstSend(1);
      fixture._guard.accept(fixture.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
      QueryTicket original = fixture._guard.admit(Capability.BROKER_QUERY);
      fixture._clock.millis(5);
      fixture._guard.firstSend(2);
      fixture._guard.accept(new ServingPermit(fixture._participant, 2, fixture._control,
          new Revision(fixture._history, 10), permitReduction ? 20 : 100, permitReduction ? 50 : 10, 1,
          Set.of(Capability.BROKER_QUERY)));
      expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(original));
      QueryTicket narrowed = fixture._guard.admit(Capability.BROKER_QUERY);
      fixture._guard.firstSend(3);
      fixture._guard.accept(fixture.grant(3, 10, Set.of(Capability.BROKER_QUERY)));
      expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(original));
      fixture._clock.millis(permitReduction ? 25 : 15);
      expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(narrowed));
    }
    Fixture fixture = new Fixture();
    ServingPermitGuard withMargin = new ServingPermitGuard(fixture._participant, Set.of(Capability.BROKER_QUERY),
        fixture._clock, new QualifiedLifetimePolicy(100, 50, 1_000_000, 5, 1_000_000), fixture._image::get);
    withMargin.firstSend(1);
    withMargin.accept(fixture.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
    QueryTicket ticket = withMargin.admit(Capability.BROKER_QUERY);
    withMargin.firstSend(2);
    ServingPermit tooShort = new ServingPermit(fixture._participant, 2, fixture._control,
        new Revision(fixture._history, 10), 1, 1, 1, Set.of());
    expectThrows(ServingDeniedException.class, () -> withMargin.accept(tooShort));
    expectThrows(ServingDeniedException.class, () -> withMargin.checkDelivery(ticket));
    expectThrows(ServingDeniedException.class, () -> withMargin.admit(Capability.BROKER_QUERY));
    withMargin.accept(tooShort);
    expectThrows(ServingDeniedException.class, () -> withMargin.admit(Capability.BROKER_QUERY));
  }

  @Test
  public void testImageRegressionAndPolicyVersionFence() {
    Fixture fixture = new Fixture();
    fixture._guard.firstSend(1);
    fixture._guard.accept(fixture.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
    QueryTicket ticket = fixture._guard.admit(Capability.BROKER_QUERY);
    fixture.image(11, fixture._control, fixture._participant, true, false, false);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.BROKER_QUERY));
    fixture.image(10, fixture._control, fixture._participant, true, true, false);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.BROKER_QUERY));
    fixture._image.set(new MembershipView(new Revision(fixture._history, 12), fixture._control,
        fixture._participant, true, true, false, Set.of(Capability.BROKER_QUERY), 2));
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.BROKER_QUERY));
    fixture._guard.firstSend(2);
    fixture._guard.accept(new ServingPermit(fixture._participant, 2, fixture._control,
        new Revision(fixture._history, 12), 100, 50, 2, Set.of(Capability.BROKER_QUERY)));
    fixture._guard.admit(Capability.BROKER_QUERY);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.checkDelivery(ticket));
  }

  @Test
  public void testCoherentImageProjectionExpiresBeforeParticipantCleanup()
      throws Exception {
    Fixture fixture = new Fixture();
    Policy policy = new Policy(1, 1000, 3, 100, 50, 100);
    ParticipantState participant = new ParticipantState(fixture._participant, Map.of(),
        Set.of(Capability.BROKER_QUERY), 3, true, false, false, new Revision(fixture._history, 10), 1, 2);
    MembershipView live = MembershipView.fromImage(fixture.image(
        new ControlState(fixture._control, 2, policy, 10, 1000, 100), participant), fixture._participant);
    assertTrue(live.live());
    MembershipView expired = MembershipView.fromImage(fixture.image(
        new ControlState(fixture._control, 3, policy, 11, 1000, 100), participant), fixture._participant);
    assertFalse(expired.live(), "Advancing control round expires membership without rewriting participant state");
    fixture._guard.firstSend(1);
    fixture._guard.accept(fixture.grant(1, 10, Set.of(Capability.BROKER_QUERY)));
    fixture._image.set(expired);
    expectThrows(ServingDeniedException.class, () -> fixture._guard.admit(Capability.BROKER_QUERY));
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
    private final ParticipantToken _participant = new ParticipantToken(_history, "broker-1", Role.BROKER,
        UUID.randomUUID(), 1);
    private final ControlToken _control = new ControlToken(_history, 1, 1, "leader-1", UUID.randomUUID());
    private final FakeClock _clock = new FakeClock();
    private final AtomicBoolean _cacheFailed = new AtomicBoolean();
    private final AtomicLong _advanceClockOnView = new AtomicLong(-1);
    private final AtomicReference<MembershipView> _image = new AtomicReference<>();
    private final ServingPermitGuard _guard = new ServingPermitGuard(_participant,
        Set.of(Capability.BROKER_QUERY, Capability.SERVER_QUERY), _clock,
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

    private ServingPermit grant(long sequence, long revision, Set<Capability> capabilities) {
      return new ServingPermit(_participant, sequence, _control, new Revision(_history, revision), 100, 50, 1,
          capabilities);
    }

    private void image(long revision, ControlToken control, ParticipantToken participant, boolean live, boolean ready,
        boolean draining) {
      _image.set(new MembershipView(new Revision(_history, revision), control, participant, live, ready, draining,
          Set.of(Capability.BROKER_QUERY, Capability.SERVER_QUERY), 1));
    }

    private MetadataImage image(ControlState control, ParticipantState participant)
        throws Exception {
      List<CatalogRecord> records = List.of(new CatalogRecord(RecordKind.MEMBERSHIP_CONTROL,
              MembershipProtocol.CONTROL_NAME, UUID.randomUUID(), 1, 1, 1, false, JsonUtils.objectToJsonNode(control)),
          new CatalogRecord(RecordKind.PARTICIPANT, _participant.participantId(), UUID.randomUUID(), 1, 1, 2, false,
              JsonUtils.objectToJsonNode(participant)));
      return MetadataImage.fromSnapshot(Set.of(RecordKind.MEMBERSHIP_CONTROL, RecordKind.PARTICIPANT),
          new CatalogImage(new FeedCursor(_history, 11, "opaque-scope"), records));
    }
  }
}
