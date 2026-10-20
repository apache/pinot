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

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.access.AccessType;
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
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Revision;
import org.apache.pinot.coordination.ratis.metadata.NativeBrokerRoutingPolicy;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.Content;
import org.apache.pinot.coordination.ratis.metadata.OfflineSegmentProtocol.LoaderDescriptor;
import org.apache.pinot.coordination.ratis.transport.MetadataAccess;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Checks native REST shapes, request authorization and the separate recoverable audit-config consumer.
public class NativeControllerResourceTest {
  @Test
  public void testNormalReadShapesDatabaseAndAccessFiltering() throws Exception {
    try (Fixture fixture = new Fixture()) {
      // Unsupported old names must not become visible after repeated auth-layer suffix normalization.
      fixture.record(RecordKind.SCHEMA, "orders_OFFLINE",
          new Schema.SchemaBuilder().setSchemaName("orders_OFFLINE").build());
      fixture.record(RecordKind.TABLE_CONFIG, "orders_OFFLINE_OFFLINE",
          new TableConfigBuilder(TableType.OFFLINE).setTableName("orders_OFFLINE_OFFLINE").build());
      HttpHeaders headers = headers(null);
      NativeControllerResource resource = fixture.resource();
      Response response = resource.tables(null, null, null, true, headers);
      assertEquals(json(response).get("tables"), JsonUtils.objectToJsonNode(List.of("orders")));
      assertEquals(response.getHeaderString(NativeControllerResource.REVISION_HEADER), fixture._history + ":10");
      assertEquals(response.getHeaderString(HttpHeaders.CACHE_CONTROL), "no-store");
      assertEquals(json(resource.tables("offline", null, "name", true, headers)).get("tables"),
          JsonUtils.objectToJsonNode(List.of("orders_OFFLINE")));
      assertEquals(json(resource.schemas(headers)), JsonUtils.objectToJsonNode(List.of("orders")));
      assertEquals(json(resource.schema("orders", headers)).path("schemaName").asText(), "orders");
      assertEquals(json(resource.table("orders", null, headers)).path("OFFLINE").path("tableName").asText(),
          "orders_OFFLINE");
      assertEquals(json(resource.tableSchema("orders_OFFLINE", headers)).path("schemaName").asText(), "orders");
      assertEquals(json(resource.schemasInfo(headers)).path("schemasInfo").size(), 1);
      assertEquals(json(resource.tables(null, null, null, true, headers("sales"))).path("tables"),
          JsonUtils.objectToJsonNode(List.of("sales.orders")));
      assertEquals(json(resource.schema("orders", headers("sales"))).path("schemaName").asText(), "sales.orders");
      assertStatus(403, () -> resource.schema("hidden", headers));
      assertStatus(404, () -> resource.table("absent", null, headers));
      assertStatus(400, () -> resource.schema("sales.orders", headers("other")));
      assertStatus(501, () -> resource.tables(null, null, "creationTime", true, headers));
      assertEquals(json(resource.clusterInfo(headers)).path("clusterName").asText(), "native-test");
      assertEquals(json(resource.clusterConfigs(headers)), JsonUtils.objectToJsonNode(Map.of("audit", "old")));
    }
  }

  @Test
  public void testWritesRejectNativeStateRequiringReconciliation() throws Exception {
    try (Fixture fixture = new Fixture()) {
      NativeControllerResource resource = fixture.resource();
      var snapshot = fixture._metadata.snapshot();
      CatalogRecord table = snapshot.tables().get("orders_OFFLINE");
      fixture.record(RecordKind.OFFLINE_SEGMENT_CONTENT, "segment",
          new Content("orders_OFFLINE", table.objectId(), "segment", UUID.randomUUID(), "a".repeat(64),
              "file:///segment.tar.gz", new LoaderDescriptor(1, 0, false, null, null, 1, 1, Map.of())));
      String tableTag = resource.table("orders_OFFLINE", null, headers(null)).getHeaderString(HttpHeaders.ETAG);
      HttpHeaders tableHeaders = headers(null);
      when(tableHeaders.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn(tableTag);
      assertStatus(501, () -> resource.updateTable("orders_OFFLINE", table.value().toString(), null, false,
          tableHeaders));
      String schemaTag = resource.schema("orders", headers(null)).getHeaderString(HttpHeaders.ETAG);
      HttpHeaders schemaHeaders = headers(null);
      when(schemaHeaders.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn(schemaTag);
      assertStatus(501, () -> resource.updateSchema("orders", snapshot.schemas().get("orders").value().toString(),
          false, false, schemaHeaders));
      String clusterTag = resource.clusterConfigs(headers(null)).getHeaderString(HttpHeaders.ETAG);
      HttpHeaders clusterHeaders = headers(null);
      when(clusterHeaders.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn(clusterTag);
      assertStatus(501, () -> resource.updateClusterConfig("{}", clusterHeaders));
      // Observation approvals may rotate or be revoked while segment state exists; loader settings require reconcile.
      Map<String, String> approved = new HashMap<>(fixture._metadata.snapshot().clusterConfig());
      approved.put(NativeTableSizeMetadataProvider.ENDPOINT_APPROVAL_PREFIX + "Server_native", "https://host:8443");
      fixture._metadata.validateClusterConfig(fixture._metadata.snapshot(), approved);
      approved.put(NativeBrokerRoutingPolicy.CONFIG_KEY,
          "{\"brokers\":{\"Broker_native\":[\"DefaultTenant\"]},\"servers\":{}}");
      fixture._metadata.validateClusterConfig(fixture._metadata.snapshot(), approved);
      fixture.config(approved, 11);
      Map<String, String> revoked = new HashMap<>(approved);
      revoked.put(NativeBrokerRoutingPolicy.CONFIG_KEY, "{\"brokers\":{},\"servers\":{}}");
      fixture._metadata.validateClusterConfig(fixture._metadata.snapshot(), revoked);
      Map<String, String> malformed = new HashMap<>(revoked);
      malformed.put(NativeBrokerRoutingPolicy.CONFIG_KEY, "{}");
      assertThrows(IllegalArgumentException.class,
          () -> fixture._metadata.validateClusterConfig(fixture._metadata.snapshot(), malformed));
      fixture.config(revoked, 12);
      revoked.remove(NativeBrokerRoutingPolicy.CONFIG_KEY);
      fixture._metadata.validateClusterConfig(fixture._metadata.snapshot(), revoked);
      revoked.put("loader.setting", "changed");
      assertThrows(UnsupportedOperationException.class,
          () -> fixture._metadata.validateClusterConfig(fixture._metadata.snapshot(), revoked));
    }
  }

  @Test
  public void testMembershipAndUnavailableReadsNeverUseCache() throws Exception {
    try (Fixture fixture = new Fixture()) {
      NativeControllerResource resource = fixture.resource();
      JsonNode membership = json(resource.participants(headers(null)));
      assertEquals(membership.path("revision").path("index").asText(), "10");
      assertEquals(membership.path("control").path("round").asText(), "5");
      JsonNode broker = membership.path("participants").get(0);
      assertEquals(broker.path("participant").path("role").asText(), "BROKER");
      assertTrue(broker.path("live").asBoolean());
      assertFalse(broker.path("ready").asBoolean());
      JsonNode server = membership.path("participants").get(1);
      assertTrue(server.path("ready").asBoolean());
      assertFalse(server.path("live").asBoolean());
      assertEquals(server.path("expiryRound").asText(), "5");
      fixture._unavailable.set(true);
      assertStatus(503, () -> resource.clusterConfigs(headers(null)));
      assertEquals(fixture._metadata.getClusterConfigs(), Map.of("audit", "old"));
      fixture._unavailable.set(false);
      fixture._responseHistory.set(UUID.randomUUID());
      assertStatus(503, () -> resource.schemas(headers(null)));
    }
  }

  @Test
  public void testCurrentAuthorizationIsRecheckedAfterRead() throws Exception {
    try (Fixture fixture = new Fixture()) {
      fixture._afterRead = () -> fixture._authorized.set(false);
      assertStatus(403, () -> fixture.resource().clusterConfigs(headers(null)));
    }
  }

  @Test
  public void testAuditConfigInitialChangesUnsetAndCallbackRecovery() throws Exception {
    try (Fixture fixture = new Fixture()) {
      List<Set<String>> changed = new ArrayList<>();
      AtomicBoolean rejectNext = new AtomicBoolean();
      fixture._metadata.registerClusterConfigChangeListener((keys, values) -> {
        changed.add(Set.copyOf(keys));
        if (rejectNext.getAndSet(false)) {
          throw new IllegalStateException("listener failed");
        }
      });
      assertEquals(changed, List.of(Set.of("audit")));
      fixture.config(Map.of("new", "enabled"), 11);
      rejectNext.set(true);
      assertThrows(IllegalStateException.class, fixture._metadata::refreshClusterConfig);
      assertEquals(fixture._metadata.clusterConfigFailure(), "listener failed");
      fixture._metadata.refreshClusterConfig();
      assertEquals(changed, List.of(Set.of("audit"), Set.of("audit", "new"), Set.of("audit", "new")));
      assertEquals(fixture._metadata.getClusterConfigs(), Map.of("new", "enabled"));
      assertEquals(fixture._metadata.clusterConfigFailure(), null);
      fixture._metadata.refreshClusterConfig();
      assertEquals(changed.size(), 3);
    }
  }

  private static JsonNode json(Response response) throws IOException {
    assertEquals(response.getStatus(), 200);
    return JsonUtils.stringToJsonNode((String) response.getEntity());
  }

  private static HttpHeaders headers(String database) {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(CommonConstants.DATABASE)).thenReturn(database);
    return headers;
  }

  private static void assertStatus(int status, Runnable request) {
    WebApplicationException failure = expectThrows(WebApplicationException.class, request::run);
    assertEquals(failure.getResponse().getStatus(), status);
  }

  private static final class Fixture implements AutoCloseable {
    private final UUID _history = UUID.randomUUID();
    private final AtomicReference<UUID> _responseHistory = new AtomicReference<>(_history);
    private final AtomicBoolean _unavailable = new AtomicBoolean();
    private final AtomicBoolean _authorized = new AtomicBoolean(true);
    private final List<CatalogRecord> _records = new ArrayList<>();
    private final NativeControllerMetadata _metadata;
    private final AccessControlFactory _factory;
    private long _index = 10;
    private Runnable _afterRead = () -> { };

    private Fixture() throws Exception {
      for (String name : List.of("orders", "hidden", "sales.orders")) {
        Schema schema = new Schema.SchemaBuilder().setSchemaName(name).addSingleValueDimension("name", DataType.STRING)
            .build();
        record(RecordKind.SCHEMA, name, schema);
        record(RecordKind.TABLE_CONFIG, name + "_OFFLINE",
            new TableConfigBuilder(TableType.OFFLINE).setTableName(name).build());
      }
      record(RecordKind.MEMBERSHIP_CONTROL, "control", new ControlState(
          new ControlToken(_history, 1, 1, "controller", UUID.randomUUID()), 5,
          new Policy(1, 1000, 4, 1000, 500, 100), 8, 1000, 1000));
      participant("Broker_live", Role.BROKER, 6, false);
      participant("Server_expired", Role.SERVER, 5, true);
      config(Map.of("audit", "old"), 10);
      MetadataAccess access = mock(MetadataAccess.class);
      when(access.image(any(), anyInt(), anyInt(), anyLong())).thenAnswer(invocation -> {
        if (_unavailable.get()) {
          throw new IOException("quorum unavailable");
        }
        FeedScope scope = invocation.getArgument(0);
        CatalogImage image = new CatalogImage(new FeedCursor(_responseHistory.get(), _index, scope.scopeId()),
            _records.stream().filter(record -> scope.kinds().contains(record.kind())).toList());
        _afterRead.run();
        return image;
      });
      ControllerConf conf = new ControllerConf();
      conf.setProperty(NativeMetadataConfig.PREFIX + "id", "test-controller");
      conf.setProperty(ControllerConf.HELIX_CLUSTER_NAME, "native-test");
      conf.setProperty(NativeMetadataConfig.PREFIX + "controller.http.configRefreshMs", 60_000L);
      _metadata = new NativeControllerMetadata(access, _history, conf);
      _metadata.start();
      _factory = () -> new AccessControl() {
        @Override
        public boolean hasAccess(String tableName, AccessType accessType, HttpHeaders headers, String endpoint) {
          return _authorized.get() && !"hidden".equals(tableName);
        }

        @Override
        public boolean hasAccess(HttpHeaders headers, TargetType targetType, String targetId, String action) {
          return _authorized.get() && !"hidden".equals(targetId);
        }
      };
    }

    private void record(RecordKind kind, String name, Object value) {
      _records.add(new CatalogRecord(kind, name, UUID.randomUUID(), 1, 1, 1, false,
          JsonUtils.objectToJsonNode(value)));
    }

    private void participant(String name, Role role, long expiryRound, boolean ready) {
      record(RecordKind.PARTICIPANT, name,
          new ParticipantState(new ParticipantToken(_history, name, role, UUID.randomUUID(), 1), Map.of(),
              Set.<Capability>of(), expiryRound, ready, false, false, new Revision(_history, 1), 1, 1));
    }

    private void config(Map<String, String> values, long index) {
      _records.removeIf(record -> record.kind() == RecordKind.CLUSTER_CONFIG);
      record(RecordKind.CLUSTER_CONFIG, "cluster", values);
      _index = index;
    }

    private NativeControllerResource resource() {
      return new NativeControllerResource(_metadata, _factory);
    }

    @Override
    public void close() throws InterruptedException {
      _metadata.close();
    }
  }
}
