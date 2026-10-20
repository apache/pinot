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
package org.apache.pinot.broker.routing;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryDeadline;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ParticipantRole;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ParticipantToken;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ReplicaRouting;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.RoutingSnapshot;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.SegmentRouting;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Ensures complete immutable fanout and rejects identities that could mix physical replicas across requests.
public class NativeBrokerRoutingImageTest {
  @Test
  public void testCompleteFanoutPreservesPhysicalIdentityAndOneDeadline() throws Exception {
    Fixture fixture = new Fixture();
    ReplicaRouting first = fixture.replica("Server_a", "grpcs://server-a:8090");
    ReplicaRouting second = fixture.replica("Server_b", "grpcs://server-b:8090");
    SegmentRouting one = fixture.segment("one", first);
    SegmentRouting two = fixture.segment("two", second);
    RoutingSnapshot snapshot = fixture.snapshot(one, two);
    NativeBrokerRoutingImage image = new NativeBrokerRoutingImage(snapshot, fixture._broker, "orders_OFFLINE");
    UUID query = UUID.randomUUID();
    UUID attempt = UUID.randomUUID();
    NativeQueryDeadline deadline = new NativeQueryDeadline("qualified-test-clock", 100, 150);
    var requests = image.requests(query, attempt, deadline);
    assertEquals(requests.size(), 2);
    Set<String> names = new HashSet<>();
    for (var request : requests) {
      NativeQueryContext context = request.context();
      assertEquals(context.version(), NativeQueryContext.DEADLINE_VERSION);
      assertEquals(context.queryId(), query);
      assertEquals(context.attemptId(), attempt);
      assertSame(context.deadline(), deadline);
      assertTrue(context.optionalSegments().isEmpty());
      assertTrue(names.addAll(context.requiredNames()));
      assertEquals(context.broker().incarnation().toString(), fixture._broker.getIncarnation());
      SegmentRouting source = context.requiredNames().contains("one") ? one : two;
      ReplicaRouting replica = source.getReplicas(0);
      assertEquals(context.server().incarnation().toString(), replica.getServer().getIncarnation());
      assertEquals(context.segment(source.getName()).content().objectId().toString(),
          source.getContent().getObjectId());
      assertEquals(context.segment(source.getName()).assignment().objectId().toString(),
          replica.getAssignment().getObjectId());
      assertEquals(context.segment(source.getName()).materializationId().toString(), replica.getMaterializationId());
      assertEquals(request.endpoint().toString(), replica.getEndpoint());
    }
    assertEquals(names, Set.of("one", "two"));
    assertThrows(UnsupportedOperationException.class, () -> requests.clear());
    // Mutating a protobuf builder cannot alter the captured image or already produced contexts.
    RoutingSnapshot.Builder changed = snapshot.toBuilder();
    changed.getTableBuilder().clearSegments();
    assertEquals(image.segmentCount(), 2);
    assertEquals(image.requests(query, attempt, deadline).getFirst().context().toProto(),
        requests.getFirst().context().toProto());
  }

  @Test
  public void testUninstalledUnapprovedAndEmptyTablesCannotProducePartialRequests() throws Exception {
    Fixture fixture = new Fixture();
    SegmentRouting installed = fixture.segment("installed",
        fixture.replica("Server_a", "grpcs://server-a:8090"));
    SegmentRouting missing = fixture.segment("missing");
    var image = new NativeBrokerRoutingImage(fixture.snapshot(installed, missing), fixture._broker, "orders_OFFLINE");
    assertEquals(image.segmentCount(), 2, "Missing replicas must remain represented in complete table metadata");
    assertThrows(IllegalStateException.class, () -> image.requests(UUID.randomUUID(), UUID.randomUUID(),
        new NativeQueryDeadline("test-clock", 100, 150)));
    ReplicaRouting unapproved = fixture.replica("Server_b", "grpcs://server-b:8090").toBuilder()
        .clearEndpoint().build();
    var notEligible = new NativeBrokerRoutingImage(fixture.snapshot(fixture.segment("not-eligible", unapproved)),
        fixture._broker, "orders_OFFLINE");
    assertThrows(IllegalStateException.class, () -> notEligible.requests(UUID.randomUUID(), UUID.randomUUID(),
        new NativeQueryDeadline("test-clock", 100, 150)));
    var empty = new NativeBrokerRoutingImage(fixture.snapshot(), fixture._broker, "orders_OFFLINE");
    assertThrows(IllegalStateException.class, () -> empty.requests(UUID.randomUUID(), UUID.randomUUID(),
        new NativeQueryDeadline("test-clock", 100, 150)));
  }

  @Test
  public void testCrossServerIdentityCollisionsAndEndpointConfusionAreRejected() {
    Fixture fixture = new Fixture();
    ReplicaRouting first = fixture.replica("Server_a", "grpcs://server-a:8090");
    ReplicaRouting second = fixture.replica("Server_b", "grpcs://server-b:8090");
    SegmentRouting one = fixture.segment("one", first);
    SegmentRouting two = fixture.segment("two", second);
    assertThrows(IllegalArgumentException.class, () -> new NativeBrokerRoutingImage(
        fixture.snapshot(one, two.toBuilder().setContent(one.getContent()).build()), fixture._broker,
        "orders_OFFLINE"));
    assertThrows(IllegalArgumentException.class, () -> new NativeBrokerRoutingImage(fixture.snapshot(one,
        two.toBuilder().setReplicas(0, second.toBuilder().setAssignment(first.getAssignment())).build()),
        fixture._broker, "orders_OFFLINE"));
    assertThrows(IllegalArgumentException.class, () -> new NativeBrokerRoutingImage(fixture.snapshot(one,
        two.toBuilder().setReplicas(0, second.toBuilder().setMaterializationId(first.getMaterializationId())).build()),
        fixture._broker, "orders_OFFLINE"));
    assertThrows(IllegalArgumentException.class, () -> new NativeBrokerRoutingImage(fixture.snapshot(one,
        two.toBuilder().setReplicas(0, second.toBuilder().setEndpoint(first.getEndpoint())).build()),
        fixture._broker, "orders_OFFLINE"));
    assertThrows(IllegalArgumentException.class, () -> new NativeBrokerRoutingImage(fixture.snapshot(one,
        two.toBuilder().setReplicas(0, second.toBuilder().setServer(first.getServer())).build()),
        fixture._broker, "orders_OFFLINE"));
  }

  private static final class Fixture {
    private final String _history = UUID.randomUUID().toString();
    private final ParticipantToken _broker = token("Broker_native", ParticipantRole.BROKER);
    private final MetadataRpc.ObjectVersion _table = object();
    private final MetadataRpc.ObjectVersion _schema = object();

    private ParticipantToken token(String name, ParticipantRole role) {
      return ParticipantToken.newBuilder().setHistoryId(_history).setParticipantId(name).setRole(role)
          .setIncarnation(UUID.randomUUID().toString()).setRegistrationEpoch(1).build();
    }

    private MetadataRpc.ObjectVersion object() {
      return MetadataRpc.ObjectVersion.newBuilder().setHistoryId(_history)
          .setObjectId(UUID.randomUUID().toString()).setVersion(1).build();
    }

    private ReplicaRouting replica(String name, String endpoint) {
      return ReplicaRouting.newBuilder().setServer(token(name, ParticipantRole.SERVER)).setAssignment(object())
          .setAssignmentEpoch(1).setMaterializationId(UUID.randomUUID().toString())
          .setInstallationId(UUID.randomUUID().toString()).setEndpoint(endpoint).build();
    }

    private SegmentRouting segment(String name, ReplicaRouting... replicas) {
      return SegmentRouting.newBuilder().setName(name).setContent(object()).setContentSha256("a".repeat(64))
          .addAllReplicas(List.of(replicas)).build();
    }

    private RoutingSnapshot snapshot(SegmentRouting... segments) {
      var revision = MetadataRpc.Revision.newBuilder().setHistoryId(_history).setIndex(10);
      var control = MetadataRpc.MembershipControl.newBuilder().setControlEpoch(MetadataRpc.ControlToken.newBuilder()
              .setHistoryId(_history).setRaftTerm(1).setBarrierIndex(1).setLeaderId("controller")
              .setActivationId(UUID.randomUUID().toString())).setClockBarrierIndex(1).setMinimumTickDelayMillis(100)
          .setMaxConfiguredPermitLifetimeMillis(200).setPolicy(MetadataRpc.MembershipPolicy.newBuilder().setVersion(1)
              .setMinTickIntervalMillis(100).setLeaseRounds(3).setPermitLifetimeMillis(200)
              .setMaxQueryLifetimeMillis(300).setMaxRevisionLag(100));
      return RoutingSnapshot.newBuilder().setStatus(MetadataRpc.ResultStatus.OK).setRevision(revision)
          .setParticipant(MetadataRpc.ParticipantState.newBuilder().setParticipant(_broker).setExpiryRound(3)
              .setLastModifiedIndex(1).setAppliedRevision(revision))
          .setControl(control).setTable(MetadataRpc.TableRouting.newBuilder().setTableName("orders_OFFLINE")
              .setTable(_table).setSchemaName("orders").setSchema(_schema).addAllSegments(List.of(segments))).build();
    }
  }
}
