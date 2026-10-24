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

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.common.coordination.ServingPermitGuard.MembershipView;
import org.apache.pinot.common.coordination.ServingPermitGuard.ReadOnlyCapability;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;


/// Converts a coherent catalog image to the shared local serving view, once per published image update.
/// Cache health remains the caller's responsibility; a retained projection is not proof of current hydration.
public final class RatisMembershipView {
  private RatisMembershipView() {
  }

  public static MembershipView fromImage(MetadataImage image, ParticipantToken expected)
      throws IOException {
    CatalogRecord controlRecord = image.records(RecordKind.MEMBERSHIP_CONTROL).get(MembershipProtocol.CONTROL_NAME);
    CatalogRecord participantRecord = image.records(RecordKind.PARTICIPANT).get(expected.participantId());
    ControlState control = controlRecord == null ? null : MembershipProtocol.decodeControl(controlRecord.value());
    ParticipantState participant = participantRecord == null ? null
        : MembershipProtocol.decodeParticipant(participantRecord.value());
    boolean initialized = control != null && control.controlEpoch() != null && control.policy() != null;
    boolean live = initialized && participant != null && !participant.retired()
        && participant.expiryRound() > control.round();
    return new MembershipView(MetadataRpc.Revision.newBuilder().setHistoryId(image.cursor().historyId().toString())
        .setIndex(image.cursor().index()).build(), initialized ? control(control.controlEpoch()) : null,
        participant == null ? null : participant(participant.participant()), live,
        participant != null && participant.ready(), participant != null && participant.draining(),
        participant == null ? Set.of() : participant.capabilities().stream()
            .map(capability -> ReadOnlyCapability.valueOf(capability.name())).collect(Collectors.toSet()),
        initialized ? control.policy().version() : 0);
  }

  private static MetadataRpc.ParticipantToken participant(ParticipantToken token) {
    return MetadataRpc.ParticipantToken.newBuilder().setHistoryId(token.historyId().toString())
        .setParticipantId(token.participantId()).setRole(MetadataRpc.ParticipantRole.valueOf(token.role().name()))
        .setIncarnation(token.incarnation().toString()).setRegistrationEpoch(token.registrationEpoch()).build();
  }

  private static MetadataRpc.ControlToken control(ControlToken token) {
    return MetadataRpc.ControlToken.newBuilder().setHistoryId(token.historyId().toString())
        .setRaftTerm(token.raftTerm()).setBarrierIndex(token.barrierIndex()).setLeaderId(token.leaderId())
        .setActivationId(token.activationId().toString()).build();
  }
}
