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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.requesthandler.BaseSingleStageBrokerRequestHandler;
import org.apache.pinot.broker.requesthandler.BrokerRequestIdGenerator;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.QuerySource;
import org.apache.pinot.common.request.context.NativeQueryContext;
import org.apache.pinot.common.request.context.NativeQueryDeadline;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.CatalogRecord;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ParticipantRole;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ParticipantToken;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.ReplicaRouting;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.RoutingSnapshot;
import org.apache.pinot.coordination.ratis.rpc.MetadataRpc.SegmentRouting;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
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
    var planning = image.planningMetadata(true);
    var otherRequest = image.planningMetadata(true);
    assertEquals(planning.tableCache().getActualTableName("ORDERS"), "orders");
    planning.tableCache().getTableConfig("orders_OFFLINE").setTableName("changed");
    planning.tableCache().getSchema("orders").setSchemaName("changed");
    assertEquals(otherRequest.tableCache().getTableConfig("orders_OFFLINE").getTableName(), "orders_OFFLINE");
    assertEquals(otherRequest.tableCache().getSchema("orders").getSchemaName(), "orders");
    assertEquals(image.newTableCache(false).getSchema("orders").getSchemaName(), "orders");
    BrokerRequest compiled = new BrokerRequest();
    QuerySource source = new QuerySource();
    source.setTableName("orders_OFFLINE");
    compiled.setQuerySource(source);
    var routing = planning.routingManager();
    var route = routing.getRoutingTable(compiled, 1);
    assertTrue(route.getUnavailableSegments().isEmpty());
    assertEquals(routing.getSegments(compiled), List.of("one", "two"));
    assertEquals(routing.getServingInstances("orders_OFFLINE"), Set.of("Server_a", "Server_b"));
    assertNull(routing.getRoutingTable(compiled, "other_OFFLINE", 1));
    assertFalse(routing.routingExists("orders_REALTIME"));
    route.getServerInstanceToSegmentsMap().forEach((server, segments) -> {
      var request = requests.stream().filter(value -> value.context().server().participantId()
          .equals(server.getInstanceId())).findFirst().orElseThrow();
      assertEquals(server.getHostname(), request.endpoint().getHost());
      assertEquals(server.getGrpcPort(), request.endpoint().getPort());
      assertEquals(server.getPort(), -1);
      assertEquals(server.getNettyTlsPort(), -1);
      assertNull(server.getAdminEndpoint());
      assertEquals(segments.getSegments(), request.context().requiredNames());
      assertTrue(segments.getOptionalSegments().isEmpty());
      assertThrows(UnsupportedOperationException.class, () -> segments.getSegments().clear());
    });
    assertThrows(UnsupportedOperationException.class, () -> route.getServerInstanceToSegmentsMap().clear());
    assertThrows(UnsupportedOperationException.class, () -> image.serverAuthorities().clear());
    assertThrows(IllegalArgumentException.class, () -> routing.getSegments(compiled, "sample"));
    assertThrows(IllegalArgumentException.class, () -> ServerInstance.forNativeGrpc("Server_a", "SERVER-A", 8090));
    assertThrows(IllegalArgumentException.class, () -> ServerInstance.forNativeGrpc("Server_a", "server-a", -1));
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
    assertThrows(IllegalStateException.class, () -> image.planningMetadata(false));
    ReplicaRouting unapproved = fixture.replica("Server_b", "grpcs://server-b:8090").toBuilder()
        .clearEndpoint().build();
    var notEligible = new NativeBrokerRoutingImage(fixture.snapshot(fixture.segment("not-eligible", unapproved)),
        fixture._broker, "orders_OFFLINE");
    assertThrows(IllegalStateException.class, () -> notEligible.requests(UUID.randomUUID(), UUID.randomUUID(),
        new NativeQueryDeadline("test-clock", 100, 150)));
    assertThrows(IllegalStateException.class, () -> notEligible.newRoutingManager());
    var empty = new NativeBrokerRoutingImage(fixture.snapshot(), fixture._broker, "orders_OFFLINE");
    assertThrows(IllegalStateException.class, () -> empty.requests(UUID.randomUUID(), UUID.randomUUID(),
        new NativeQueryDeadline("test-clock", 100, 150)));
    assertThrows(IllegalStateException.class, () -> empty.newRoutingManager());
  }

  @Test
  public void testCrossServerIdentityCollisionsAndEndpointConfusionAreRejected() throws Exception {
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

  @Test
  public void testPlanningRequiresExactRecordsAndCompleteCurrentServerAuthorities() throws Exception {
    Fixture fixture = new Fixture();
    RoutingSnapshot snapshot = fixture.snapshot(fixture.segment("one",
        fixture.replica("Server_a", "grpcs://server-a:8090")));
    var table = snapshot.getTable();
    var authority = table.getServerAuthorities(0);
    NativeBrokerRoutingImage image = new NativeBrokerRoutingImage(snapshot, fixture._broker, "orders_OFFLINE");
    assertTrue(image.hasPlanningMetadata());
    assertEquals(image.control(), snapshot.getControl());
    var renewed = snapshot.toBuilder().setRevision(snapshot.getRevision().toBuilder().setIndex(11))
        .setTable(table.toBuilder().setServerAuthorities(0, authority.toBuilder()
            .setLastRenewalSequence(1).setExpiryRound(4).setLastModifiedIndex(11))).build();
    assertEquals(new NativeBrokerRoutingImage(renewed, fixture._broker, "orders_OFFLINE").queryIdentity(),
        image.queryIdentity(), "Lease renewal must not change captured query identity");
    for (var invalid : List.of(
        table.toBuilder().clearTableConfig().build(),
        table.toBuilder().setTableConfig(table.getTableConfig().toBuilder().setVersion(2)).build(),
        table.toBuilder().setTableConfig(table.getTableConfig().toBuilder()
            .setObjectId(UUID.randomUUID().toString())).build(),
        table.toBuilder().setSchemaRecord(table.getSchemaRecord().toBuilder().setLastModifiedIndex(11)).build(),
        table.toBuilder().setSchemaRecord(table.getSchemaRecord().toBuilder().setName("other")).build(),
        table.toBuilder().clearServerAuthorities().build(),
        table.toBuilder().addServerAuthorities(authority).build(),
        table.toBuilder().setServerAuthorities(0, authority.toBuilder().setExpiryRound(0)).build(),
        table.toBuilder().setServerAuthorities(0, authority.toBuilder().setReady(false)).build(),
        table.toBuilder().setServerAuthorities(0, authority.toBuilder().setDraining(true)).build(),
        table.toBuilder().setServerAuthorities(0, authority.toBuilder().clearCapabilities()).build(),
        table.toBuilder().setServerAuthorities(0, authority.toBuilder().setParticipant(
            authority.getParticipant().toBuilder().setIncarnation(UUID.randomUUID().toString()))).build(),
        table.toBuilder().setServerAuthorities(0, authority.toBuilder()
            .putEndpoints("queryGrpc", "grpcs://other:8090")).build())) {
      assertThrows(IllegalArgumentException.class, () -> new NativeBrokerRoutingImage(
          snapshot.toBuilder().setTable(invalid).build(), fixture._broker, "orders_OFFLINE"));
    }
    assertThrows(IOException.class, () -> new NativeBrokerRoutingImage(snapshot.toBuilder()
        .setTable(table.toBuilder().setSchemaRecord(table.getSchemaRecord().toBuilder()
            .setValue(MetadataRpc.CatalogValue.newBuilder().setSchemaJson(ByteString.copyFromUtf8("[]"))))).build(),
        fixture._broker, "orders_OFFLINE"));
    assertThrows(IOException.class, () -> new NativeBrokerRoutingImage(snapshot.toBuilder()
        .setTable(table.toBuilder().setTableConfig(table.getTableConfig().toBuilder()
            .setKind(MetadataRpc.RecordKind.SCHEMA))).build(), fixture._broker, "orders_OFFLINE"));
    for (var invalid : List.of(
        snapshot.getControl().toBuilder().setMinimumTickDelayMillis(99).build(),
        snapshot.getControl().toBuilder().setMaxConfiguredPermitLifetimeMillis(199).build(),
        snapshot.getControl().toBuilder().setMaxConfiguredPermitLifetimeMillis(201).build())) {
      assertThrows(IllegalArgumentException.class, () -> new NativeBrokerRoutingImage(
          snapshot.toBuilder().setControl(invalid).build(), fixture._broker, "orders_OFFLINE"));
    }

    NativeBrokerRoutingImage legacy = new NativeBrokerRoutingImage(snapshot.toBuilder().setTable(table.toBuilder()
        .clearTableConfig().clearSchemaRecord().clearServerAuthorities()).build(), fixture._broker, "orders_OFFLINE");
    assertFalse(legacy.hasPlanningMetadata(), "Old observation images do not acquire planning authority");
    assertThrows(IllegalStateException.class, () -> legacy.planningMetadata(false));
  }

  @Test
  public void testNormalCompilerRetainsCapturedSchemaAndRoutingAcrossRefresh() throws Exception {
    Fixture fixture = new Fixture();
    NativeBrokerRoutingImage initial = new NativeBrokerRoutingImage(
        fixture.snapshot(fixture.segment("one", fixture.replica("Server_a", "grpcs://server-a:8090"))),
        fixture._broker, "orders_OFFLINE");
    var next = fixture.snapshot(
        fixture.segment("two", fixture.replica("Server_b", "grpcs://server-b:8090"))).toBuilder();
    Schema changedSchema = new Schema.SchemaBuilder().setSchemaName("orders")
        .addSingleValueDimension("newValue", DataType.INT).build();
    next.getRevisionBuilder().setIndex(20);
    next.getTableBuilder().setSchema(fixture._schema.toBuilder().setVersion(2))
        .setSchemaRecord(next.getTable().getSchemaRecord().toBuilder().setVersion(2).setLastModifiedIndex(20)
            .setValue(MetadataRpc.CatalogValue.newBuilder()
                .setSchemaJson(ByteString.copyFrom(JsonUtils.objectToBytes(changedSchema)))));
    NativeBrokerRoutingImage replacement =
        new NativeBrokerRoutingImage(next.build(), fixture._broker, "orders_OFFLINE");
    AtomicReference<NativeBrokerRoutingImage> current = new AtomicReference<>(initial);
    AtomicReference<NativeBrokerRoutingImage.PlanningMetadata> captured = new AtomicReference<>();
    AtomicInteger captures = new AtomicInteger();
    AtomicInteger dispatches = new AtomicInteger();
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);
    QueryQuotaManager quotas = mock(QueryQuotaManager.class);
    when(quotas.acquire(anyString())).thenReturn(true);
    when(quotas.acquireDatabase(anyString())).thenReturn(true);
    when(quotas.acquireApplication(anyString())).thenReturn(true);
    TableCache unusedCache = mock(TableCache.class, invocation -> {
      throw new AssertionError("Compilation consulted the global table cache: " + invocation.getMethod());
    });
    RoutingManager unusedRouting = mock(RoutingManager.class, invocation -> {
      throw new AssertionError("Compilation consulted the global routing manager: " + invocation.getMethod());
    });
    BaseSingleStageBrokerRequestHandler handler = new BaseSingleStageBrokerRequestHandler(config, "Broker_native",
        new BrokerRequestIdGenerator(), unusedRouting, new AllowAllAccessControlFactory(), quotas, unusedCache,
        ThreadAccountantUtils.getNoOpAccountant(), null) {
      @Override
      protected RequestEnvironment captureRequestEnvironment(PinotQuery query, @Nullable HttpHeaders headers) {
        captures.incrementAndGet();
        // Simulate publication of a different schema and route immediately after this request captures its image.
        var metadata = current.getAndSet(replacement).planningMetadata(true);
        captured.set(metadata);
        return new RequestEnvironment(metadata.tableCache(), metadata.routingManager());
      }

      @Override
      protected boolean canCompileWithMultiStageEngine(String query, String database) {
        return false;
      }

      @Override
      protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest original,
          BrokerRequest server, TableRouteInfo route, long timeoutMs, ServerStats stats, RequestContext context) {
        throw new AssertionError("Captured metadata was lost before dispatch");
      }

      @Override
      protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest original,
          BrokerRequest server, TableRouteInfo route, long timeoutMs, ServerStats stats, RequestContext context,
          RequestEnvironment environment) {
        assertSame(environment.tableCache(), captured.get().tableCache());
        assertSame(environment.routingManager(), captured.get().routingManager());
        assertEquals(server.getPinotQuery().getDataSource().getTableName(), "orders_OFFLINE");
        assertEquals(route.getOfflineRoutingTable().size(), 1);
        var target = route.getOfflineRoutingTable().entrySet().iterator().next();
        boolean first = dispatches.getAndIncrement() == 0;
        assertEquals(target.getKey().getInstanceId(), first ? "Server_a" : "Server_b");
        assertEquals(target.getKey().getGrpcPort(), 8090);
        assertEquals(target.getValue().getSegments(), List.of(first ? "one" : "two"));
        assertTrue(target.getValue().getOptionalSegments().isEmpty());
        BrokerResponseNative response = new BrokerResponseNative();
        response.setResultTable(new ResultTable(new DataSchema(new String[]{"total"},
            new ColumnDataType[]{ColumnDataType.LONG}), List.<Object[]>of(new Object[]{6L})));
        return response;
      }
    };
    try {
      assertTrue(handler.handleRequest("SELECT SUM(VALUE) AS total FROM ORDERS WHERE VALUE > 1")
          .getExceptions().isEmpty());
      assertEquals(captures.get(), 1);
      assertEquals(dispatches.get(), 1);
      assertSame(current.get(), replacement);
      assertFalse(handler.handleRequest("SELECT SUM(VALUE) FROM ORDERS").getExceptions().isEmpty(),
          "The next request must observe the replacement schema");
      assertEquals(dispatches.get(), 1);
      assertTrue(handler.handleRequest("SELECT SUM(NEWVALUE) AS total FROM ORDERS WHERE NEWVALUE > 1")
          .getExceptions().isEmpty());
      assertEquals(captures.get(), 3);
      assertEquals(dispatches.get(), 2);
    } finally {
      handler.shutDown();
    }
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

    private RoutingSnapshot snapshot(SegmentRouting... segments) throws IOException {
      var revision = MetadataRpc.Revision.newBuilder().setHistoryId(_history).setIndex(10);
      var control = MetadataRpc.MembershipControl.newBuilder().setControlEpoch(MetadataRpc.ControlToken.newBuilder()
              .setHistoryId(_history).setRaftTerm(1).setBarrierIndex(1).setLeaderId("controller")
              .setActivationId(UUID.randomUUID().toString())).setClockBarrierIndex(1).setMinimumTickDelayMillis(100)
          .setMaxConfiguredPermitLifetimeMillis(200).setPolicy(MetadataRpc.MembershipPolicy.newBuilder().setVersion(1)
              .setMinTickIntervalMillis(100).setLeaseRounds(3).setPermitLifetimeMillis(200)
              .setMaxQueryLifetimeMillis(300).setMaxRevisionLag(100));
      Map<String, MetadataRpc.ParticipantState> authorities = new HashMap<>();
      for (SegmentRouting segment : segments) {
        for (ReplicaRouting replica : segment.getReplicasList()) {
          var state = MetadataRpc.ParticipantState.newBuilder().setParticipant(replica.getServer())
              .setReady(true).setExpiryRound(3).setLastModifiedIndex(1).setAppliedRevision(revision)
              .addCapabilities(MetadataRpc.ServingCapability.SERVER_QUERY);
          if (replica.hasEndpoint()) {
            state.putEndpoints("queryGrpc", replica.getEndpoint()).putEndpoints("nativeQueryVersion", "2");
          }
          authorities.putIfAbsent(replica.getServer().getParticipantId(), state.build());
        }
      }
      var tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("orders").build();
      var schema = new Schema.SchemaBuilder().setSchemaName("orders")
          .addSingleValueDimension("value", DataType.INT).addSingleValueDimension("label", DataType.STRING).build();
      return RoutingSnapshot.newBuilder().setStatus(MetadataRpc.ResultStatus.OK).setRevision(revision)
          .setParticipant(MetadataRpc.ParticipantState.newBuilder().setParticipant(_broker).setExpiryRound(3)
              .setLastModifiedIndex(1).setAppliedRevision(revision))
          .setControl(control).setTable(MetadataRpc.TableRouting.newBuilder().setTableName("orders_OFFLINE")
              .setTable(_table).setSchemaName("orders").setSchema(_schema).addAllSegments(List.of(segments))
              .setTableConfig(record(_table, "orders_OFFLINE", MetadataRpc.RecordKind.TABLE_CONFIG,
                  MetadataRpc.CatalogValue.newBuilder()
                      .setTableConfigJson(ByteString.copyFrom(JsonUtils.objectToBytes(tableConfig))).build()))
              .setSchemaRecord(record(_schema, "orders", MetadataRpc.RecordKind.SCHEMA,
                  MetadataRpc.CatalogValue.newBuilder()
                      .setSchemaJson(ByteString.copyFrom(JsonUtils.objectToBytes(schema))).build()))
              .addAllServerAuthorities(authorities.values())).build();
    }

    private CatalogRecord record(MetadataRpc.ObjectVersion identity, String name, MetadataRpc.RecordKind kind,
        MetadataRpc.CatalogValue value) {
      return CatalogRecord.newBuilder().setKind(kind).setName(name).setObjectId(identity.getObjectId())
          .setVersion(identity.getVersion()).setSchemaVersion(1).setLastModifiedIndex(1).setValue(value).build();
    }
  }
}
