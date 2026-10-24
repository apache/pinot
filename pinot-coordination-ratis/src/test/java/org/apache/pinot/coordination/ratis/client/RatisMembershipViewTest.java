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
import org.apache.pinot.common.coordination.ServingPermitGuard.MembershipView;
import org.apache.pinot.common.coordination.ServingPermitGuard.ReadOnlyCapability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Capability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Catalog projection checks stay here; the transport-neutral timing and ticket invariants run in pinot-common.
public class RatisMembershipViewTest {
  @Test
  public void testCoherentImageProjectionExpiresBeforeParticipantCleanup()
      throws Exception {
    UUID history = UUID.randomUUID();
    ParticipantToken token = new ParticipantToken(history, "broker-1", Role.BROKER, UUID.randomUUID(), 1);
    ControlToken control = new ControlToken(history, 1, 1, "leader-1", UUID.randomUUID());
    Policy policy = new Policy(1, 1000, 3, 100, 50, 100);
    ParticipantState participant = new ParticipantState(token, Map.of(), Set.of(Capability.BROKER_QUERY),
        3, true, false, false, new Revision(history, 10), 1, 2);
    MembershipView live = RatisMembershipView.fromImage(
        image(new ControlState(control, 2, policy, 10, 1000, 100), participant), token);
    assertTrue(live.live());
    assertTrue(live.ready());
    assertEquals(live.participant().getIncarnation(), token.incarnation().toString());
    assertEquals(live.capabilities(), Set.of(ReadOnlyCapability.BROKER_QUERY));
    MembershipView expired = RatisMembershipView.fromImage(
        image(new ControlState(control, 3, policy, 11, 1000, 100), participant), token);
    assertFalse(expired.live(), "Advancing control round expires membership without rewriting participant state");
  }

  private static MetadataImage image(ControlState control, ParticipantState participant)
      throws Exception {
    List<CatalogRecord> records = List.of(new CatalogRecord(RecordKind.MEMBERSHIP_CONTROL,
            MembershipProtocol.CONTROL_NAME, UUID.randomUUID(), 1, 1, 1, false, JsonUtils.objectToJsonNode(control)),
        new CatalogRecord(RecordKind.PARTICIPANT, participant.participant().participantId(), UUID.randomUUID(),
            1, 1, 2, false, JsonUtils.objectToJsonNode(participant)));
    return MetadataImage.fromSnapshot(Set.of(RecordKind.MEMBERSHIP_CONTROL, RecordKind.PARTICIPANT),
        new CatalogImage(new FeedCursor(participant.participant().historyId(), 11, "opaque-scope"), records));
  }
}
