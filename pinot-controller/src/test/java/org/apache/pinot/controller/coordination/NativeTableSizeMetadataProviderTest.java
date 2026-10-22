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
package org.apache.pinot.controller.coordination;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.common.restlet.resources.NativeTableSizeIdentity;
import org.apache.pinot.common.restlet.resources.NativeTableSizeIdentity.SegmentIdentity;
import org.apache.pinot.common.restlet.resources.SegmentSizeInfo;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.controller.coordination.NativeControllerMetadata.Snapshot;
import org.apache.pinot.controller.util.TableSizeMetadataProvider.Replicas;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ControlToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantState;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ParticipantToken;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Role;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogRecord;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Assignment;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Compatibility;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.LoaderDescriptor;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Checks native size identity attribution and pre/post measurement consistency without synthesizing serving state.
public class NativeTableSizeMetadataProviderTest {
  @Test
  public void testAbsentEmptyAndIncompletePlacement() throws Exception {
    Fixture fixture = new Fixture(2);
    NativeTableSizeMetadataProvider absent = new NativeTableSizeMetadataProvider(fixture.snapshot(), "absent", null);
    assertNull(absent.readTableSizes("absent", false).offline());
    assertFalse(absent.empty());
    NativeTableSizeMetadataProvider empty = fixture.provider();
    assertTrue(empty.empty());
    assertTrue(empty.readReplicaAssignments("orders_OFFLINE", false).serverToSegments().isEmpty());
    fixture.content("segment", 1);
    expectThrows(IOException.class, fixture::provider);
  }

  @Test
  public void testExactObservedIdentitiesAndPartialReports() throws Exception {
    Fixture fixture = new Fixture(2);
    fixture.content("segment", 2);
    NativeTableSizeMetadataProvider provider = fixture.provider();
    Replicas replicas = provider.readReplicaAssignments("orders_OFFLINE", false);
    assertEquals(replicas.serverToSegments().size(), 2);
    TableSizeInfo good = fixture.report("Server_0", "segment", 100);
    assertEquals(provider.validateServerReports(replicas, Map.of("Server_0", good)), Map.of("Server_0", good));
    assertFalse(provider.readTableSizes("orders", false).offline().compressionStatsEnabled());
    assertFalse(provider.canPublishMetrics("orders_OFFLINE"));
    NativeTableSizeIdentity identity = good.getNativeMetadata();
    TableSizeInfo wrongSession = fixture.report("Server_0", "segment", 100).setNativeMetadata(
        new NativeTableSizeIdentity(identity.getHistoryId(), identity.getParticipantId(), UUID.randomUUID(),
            identity.getRegistrationEpoch(), identity.getRevision(), identity.getSegments()));
    SegmentIdentity segment = identity.getSegments().get("segment");
    SegmentIdentity wrongMaterialization = new SegmentIdentity(segment.getTableObjectId(), segment.getTableVersion(),
        segment.getSchemaObjectId(), segment.getSchemaVersion(), segment.getSegmentObjectId(),
        segment.getContentVersion(), segment.getContentSha256(), segment.getAssignmentObjectId(),
        segment.getAssignmentEpoch(), UUID.randomUUID());
    TableSizeInfo wrongContent = fixture.report("Server_0", "segment", 100).setNativeMetadata(
        new NativeTableSizeIdentity(identity.getHistoryId(), identity.getParticipantId(), identity.getIncarnation(),
            identity.getRegistrationEpoch(), identity.getRevision(), Map.of("segment", wrongMaterialization)));
    TableSizeInfo unknownSegment = new TableSizeInfo("orders_OFFLINE", 100,
        List.of(new SegmentSizeInfo("unowned", 100)), 1).setNativeMetadata(identity);
    TableSizeInfo unmatchedIdentity = new TableSizeInfo("orders_OFFLINE", 0, List.of(), 1).setNativeMetadata(identity);
    TableSizeInfo wrongTotal =
        new TableSizeInfo("orders_OFFLINE", 101, good.getSegments(), 1).setNativeMetadata(identity);
    TableSizeInfo legacyVersion =
        new TableSizeInfo("orders_OFFLINE", 100, good.getSegments()).setNativeMetadata(identity);
    for (TableSizeInfo invalid : List.of(wrongSession, wrongContent, unknownSegment, unmatchedIdentity, wrongTotal,
        legacyVersion, fixture.report("Server_0", "segment", -1))) {
      assertTrue(provider.validateServerReports(replicas, Map.of("Server_0", invalid)).isEmpty());
    }
    // Replica estimates must not overflow even when the only returned report has a representable total.
    assertTrue(provider.validateServerReports(replicas,
        Map.of("Server_0", fixture.report("Server_0", "segment", Long.MAX_VALUE))).isEmpty());
    provider.validateCurrent(fixture.snapshot());
  }

  @Test
  public void testChangedDependenciesAndNewContentDuringCollection() throws Exception {
    Fixture fixture = new Fixture(1);
    fixture.content("first", 1);
    NativeTableSizeMetadataProvider provider = fixture.provider();
    provider.validateCurrent(fixture.snapshot());
    fixture.content("second", 1);
    expectThrows(IOException.class, () -> provider.validateCurrent(fixture.snapshot()));

    NativeTableSizeMetadataProvider beforeSchemaChange = fixture.provider();
    CatalogRecord schema = fixture._schema;
    fixture._schema =
        new CatalogRecord(schema.kind(), schema.name(), schema.objectId(), 2, 1, 19, false, schema.value());
    expectThrows(IOException.class, () -> beforeSchemaChange.validateCurrent(fixture.snapshot()));
    expectThrows(IOException.class, fixture::provider);
    fixture._schema = schema;

    NativeTableSizeMetadataProvider beforeExpiry = fixture.provider();
    ParticipantState server = fixture._participants.get("Server_0");
    fixture._participants.put("Server_0", new ParticipantState(server.participant(), server.endpoints(),
        server.capabilities(), 5, true, false, false, server.appliedRevision(), 2, 19));
    expectThrows(IOException.class, () -> beforeExpiry.validateCurrent(fixture.snapshot()));
  }

  @Test
  public void testPlacementFanoutAndAmbiguousNamesRejected() throws Exception {
    Fixture fixture = new Fixture(1);
    fixture.content("segment", 129);
    expectThrows(IOException.class, fixture::provider);

    Fixture duplicate = new Fixture(1);
    duplicate.content("segment", 1);
    CatalogRecord content = duplicate._contents.values().iterator().next();
    duplicate._contents.put("duplicate", new CatalogRecord(content.kind(), "duplicate", UUID.randomUUID(),
        1, 1, 11, false, content.value()));
    expectThrows(IOException.class, duplicate::provider);
  }

  @Test
  public void testEndpointApprovalAndHttpsPreference() throws Exception {
    Fixture fixture = new Fixture(1);
    fixture.content("segment", 1);
    String key = NativeTableSizeMetadataProvider.ENDPOINT_APPROVAL_PREFIX + "Server_0";
    String http = fixture._clusterConfig.remove(key);
    assertTrue(fixture.provider().readReplicaAssignments("orders_OFFLINE", false).serverEndpoints().isEmpty());
    fixture._clusterConfig.put(key, "http://unapproved:9000");
    assertTrue(fixture.provider().readReplicaAssignments("orders_OFFLINE", false).serverEndpoints().isEmpty());
    ParticipantState server = fixture._participants.get("Server_0");
    String https = "https://127.0.0.1:24443";
    fixture._participants.put("Server_0", new ParticipantState(server.participant(),
        Map.of("adminHttp", http, "adminHttps", https), server.capabilities(), server.expiryRound(),
        true, false, false, server.appliedRevision(), 2, 19));
    fixture._clusterConfig.put(key, http);
    // An advertised HTTPS endpoint requires its own approval; HTTP is never a downgrade fallback.
    assertTrue(fixture.provider().readReplicaAssignments("orders_OFFLINE", false).serverEndpoints().isEmpty());
    fixture._clusterConfig.put(key, https);
    NativeTableSizeMetadataProvider provider = fixture.provider();
    assertEquals(provider.readReplicaAssignments("orders_OFFLINE", false).serverEndpoints(),
        Map.of("Server_0", https));
    fixture._clusterConfig.remove(key);
    expectThrows(IOException.class, () -> provider.validateCurrent(fixture.snapshot()));
  }

  private static final class Fixture {
    private final UUID _history = UUID.randomUUID();
    private CatalogRecord _schema;
    private final CatalogRecord _table;
    private final Map<String, CatalogRecord> _contents = new HashMap<>();
    private final Map<String, CatalogRecord> _assignments = new HashMap<>();
    private final Map<String, ParticipantState> _participants = new HashMap<>();
    private final Map<String, String> _clusterConfig = new HashMap<>();

    private Fixture(int replication) {
      _schema = record(RecordKind.SCHEMA, "orders", UUID.randomUUID(), 10,
          new Schema.SchemaBuilder().setSchemaName("orders").build());
      _table = record(RecordKind.TABLE_CONFIG, "orders_OFFLINE", UUID.randomUUID(), 10,
          new TableConfigBuilder(TableType.OFFLINE).setTableName("orders").setNumReplicas(replication).build());
    }

    private void content(String name, int replicas) {
      UUID contentId = UUID.randomUUID();
      String contentName = OfflineSegmentProtocol.contentName(_table.objectId(), name);
      Content content = new Content("orders_OFFLINE", _table.objectId(), name, contentId, "a".repeat(64),
          "file:///segment.tar.gz", new LoaderDescriptor(1, 0, false, null, null, 1, 1, Map.of()));
      _contents.put(contentName, record(RecordKind.OFFLINE_SEGMENT_CONTENT, contentName, contentId, 11, content));
      for (int i = 0; i < replicas; i++) {
        String id = "Server_" + i;
        ParticipantState server = _participants.computeIfAbsent(id, ignored -> new ParticipantState(
            new ParticipantToken(_history, id, Role.SERVER, UUID.randomUUID(), 1),
            Map.of("adminHttp", "http://127.0.0.1:" + (21000 + _participants.size())), Set.of(),
            9, true, false, false, new Revision(_history, 20), 1, 13));
        _clusterConfig.put(NativeTableSizeMetadataProvider.ENDPOINT_APPROVAL_PREFIX + id,
            server.endpoints().get("adminHttp"));
        UUID assignmentId = OfflineSegmentProtocol.assignmentObjectId(_history, contentId, id);
        String assignmentName = OfflineSegmentProtocol.assignmentName(contentId, id);
        Assignment assignment = new Assignment("orders_OFFLINE", _table.objectId(), name, contentId,
            content.contentSha256(), 1, server.participant(),
            OfflineSegmentProtocol.materializationId(_history, assignmentId, 1),
            new Compatibility(new ObjectVersion(_history, _table.objectId(), 1),
                new ObjectVersion(_history, _schema.objectId(), 1)), true);
        _assignments.put(assignmentName,
            record(RecordKind.OFFLINE_REPLICA_ASSIGNMENT, assignmentName, assignmentId, 12, assignment));
      }
    }

    private TableSizeInfo report(String serverId, String name, long size) throws IOException {
      CatalogRecord content = _contents.get(OfflineSegmentProtocol.contentName(_table.objectId(), name));
      CatalogRecord assigned = _assignments.get(OfflineSegmentProtocol.assignmentName(content.objectId(), serverId));
      Assignment assignment = OfflineSegmentProtocol.decodeAssignment(assigned.value());
      SegmentIdentity identity = new SegmentIdentity(_table.objectId(), _table.version(), _schema.objectId(),
          _schema.version(), content.objectId(), content.version(), assignment.contentSha256(), assigned.objectId(),
          assignment.assignmentEpoch(), assignment.materializationId());
      ParticipantToken server = _participants.get(serverId).participant();
      return new TableSizeInfo("orders_OFFLINE", size, List.of(new SegmentSizeInfo(name, size)), 1)
          .setNativeMetadata(new NativeTableSizeIdentity(_history, serverId, server.incarnation(),
              server.registrationEpoch(), 20, Map.of(name, identity)));
    }

    private Snapshot snapshot() {
      return new Snapshot(new Revision(_history, 20), _clusterConfig, Map.of("orders", _schema),
          Map.of("orders_OFFLINE", _table), new ControlState(
              new ControlToken(_history, 1, 1, "controller", UUID.randomUUID()), 5,
              new Policy(1, 1000, 4, 1000, 500, 100), 18, 1000, 1000), _participants, null,
          Set.of("orders_OFFLINE"), _contents, _assignments);
    }

    private NativeTableSizeMetadataProvider provider() throws IOException {
      return new NativeTableSizeMetadataProvider(snapshot(), "orders", null);
    }

    private static CatalogRecord record(RecordKind kind, String name, UUID id, long index, Object value) {
      return new CatalogRecord(kind, name, id, 1, 1, index, false, JsonUtils.objectToJsonNode(value));
    }
  }
}
