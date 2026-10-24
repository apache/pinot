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
package org.apache.pinot.common.request.context;

import com.google.protobuf.UnknownFieldSet;
import java.util.List;
import java.util.UUID;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Identity;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.ObjectVersion;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Operation;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Owner;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Participant;
import org.apache.pinot.common.config.provider.NativeOfflineSegmentAction.Role;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.common.request.context.NativeQueryContext.ProcessIdentity;
import org.apache.pinot.common.request.context.NativeQueryContext.Segment;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Exercises read-identity matching independently of the installation operation and both transport representations.
public class NativeQueryContextTest {
  @Test
  public void testPinnedInstallationRequiresEveryReadIdentityButNotMutationAttempt() {
    NativeQueryContext context = context();
    Segment segment = context.requiredSegments().get(0);
    Identity installation = installation(context, segment, operation());
    assertTrue(context.matchesInstallation(segment, installation));
    assertTrue(context.matchesInstallation(segment, installation(context, segment, operation())),
        "A new installation operation must not change otherwise identical read inputs");

    Server.NativeQueryIdentity wire = context.toProto();
    Server.NativeQuerySegment segmentWire = wire.getRequiredSegments(0);
    for (Server.NativeQueryIdentity changed : List.of(
        wire.toBuilder().setHistoryId(UUID.randomUUID().toString()).build(),
        wire.toBuilder().setServer(wire.getServer().toBuilder().setParticipantId("other")).build(),
        wire.toBuilder().setServer(wire.getServer().toBuilder().setIncarnation(UUID.randomUUID().toString())).build(),
        wire.toBuilder().setServer(wire.getServer().toBuilder().setRegistrationEpoch(2)).build(),
        wire.toBuilder().setTableName("other_OFFLINE").setSchemaName("other").build(),
        wire.toBuilder().setTable(wire.getTable().toBuilder().setObjectId(UUID.randomUUID().toString())).build(),
        wire.toBuilder().setTable(wire.getTable().toBuilder().setVersion(2)).build(),
        wire.toBuilder().setSchema(wire.getSchema().toBuilder().setObjectId(UUID.randomUUID().toString())).build(),
        wire.toBuilder().setSchema(wire.getSchema().toBuilder().setVersion(2)).build(),
        wire.toBuilder().setRequiredSegments(0, segmentWire.toBuilder().setName("other")).build(),
        wire.toBuilder().setRequiredSegments(0, segmentWire.toBuilder()
            .setContent(segmentWire.getContent().toBuilder().setObjectId(UUID.randomUUID().toString()))).build(),
        wire.toBuilder().setRequiredSegments(0, segmentWire.toBuilder()
            .setContent(segmentWire.getContent().toBuilder().setVersion(2))).build(),
        wire.toBuilder().setRequiredSegments(0, segmentWire.toBuilder().setContentSha256("b".repeat(64))).build(),
        wire.toBuilder().setRequiredSegments(0, segmentWire.toBuilder()
            .setAssignment(segmentWire.getAssignment().toBuilder().setObjectId(UUID.randomUUID().toString()))).build(),
        wire.toBuilder().setRequiredSegments(0, segmentWire.toBuilder()
            .setAssignment(segmentWire.getAssignment().toBuilder().setVersion(2))).build(),
        wire.toBuilder().setRequiredSegments(0, segmentWire.toBuilder()
            .setMaterializationId(UUID.randomUUID().toString())).build())) {
      NativeQueryContext expected = NativeQueryContext.fromProto(changed);
      assertFalse(expected.matchesInstallation(expected.requiredSegments().get(0), installation), changed.toString());
    }
  }

  @Test
  public void testRejectsUnknownNestedFieldsAndMatchesNamesAsSets() {
    NativeQueryContext context = context();
    Server.NativeQueryIdentity wire = context.toProto();
    UnknownFieldSet unknown = UnknownFieldSet.newBuilder()
        .addField(99, UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
    assertThrows(IllegalArgumentException.class, () -> NativeQueryContext.fromProto(wire.toBuilder()
        .setRequiredSegments(0, wire.getRequiredSegments(0).toBuilder().setUnknownFields(unknown)).build()));
    assertThrows(IllegalArgumentException.class, () -> NativeQueryContext.fromProto(wire.toBuilder()
        .setTable(wire.getTable().toBuilder().setUnknownFields(unknown)).build()));
    assertThrows(IllegalArgumentException.class, () -> context.validateSegments("tbl_OFFLINE",
        List.of("segment", "segment"), null));
    context.validateSegments("tbl_OFFLINE", List.of("segment"), List.of());
  }

  private static NativeQueryContext context() {
    return new NativeQueryContext(1, UUID.randomUUID(), new ProcessIdentity("broker", UUID.randomUUID(), 1),
        new Participant("server", Role.SERVER, UUID.randomUUID(), 1), UUID.randomUUID(), UUID.randomUUID(),
        "tbl_OFFLINE", object(), "tbl", object(),
        List.of(new Segment("segment", object(), "a".repeat(64), object(), UUID.randomUUID())), List.of());
  }

  private static ObjectVersion object() {
    return new ObjectVersion(UUID.randomUUID(), 1);
  }

  private static Operation operation() {
    return new Operation(UUID.randomUUID(), 1, new Owner(1, 0, 1,
        new Participant("controller", Role.CONTROLLER, UUID.randomUUID(), 1)));
  }

  private static Identity installation(NativeQueryContext context, Segment segment, Operation operation) {
    return new Identity(context.historyId(), context.tableName(), context.table(), context.schemaName(),
        context.schema(), segment.name(), segment.content(), segment.contentSha256(), context.server(),
        segment.assignment(), segment.materializationId(), operation);
  }
}
