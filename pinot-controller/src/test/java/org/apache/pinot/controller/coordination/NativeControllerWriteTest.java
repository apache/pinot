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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.auth.BasicAuthTokenUtils;
import org.apache.pinot.common.coordination.NativeMetadataConfig;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ActivateControlEpoch;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.ConfigurePolicy;
import org.apache.pinot.coordination.ratis.metadata.MembershipProtocol.Policy;
import org.apache.pinot.coordination.ratis.metadata.MetadataCatalog;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.CatalogImage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ChangePage;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Command;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ConfigTransaction;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedCursor;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.FeedScope;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetRevision;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.GetStreamTip;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.MembershipCommand;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Mutation;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ObjectVersion;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Query;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.ReadBarrier;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RecordKind;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.RegisterClient;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Result;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Status;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.StreamTipResult;
import org.apache.pinot.coordination.ratis.metadata.MetadataProtocol.Transaction;
import org.apache.pinot.coordination.ratis.metadata.NativeReplicaPlacementPolicy;
import org.apache.pinot.coordination.ratis.transport.MetadataAccess;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Exercises HTTP preconditions and lost replies against the actual durable catalog and serialized controller stream.
public class NativeControllerWriteTest {
  @Test
  public void testValidatedSchemaTableAndClusterWrites() throws Exception {
    try (Fixture fixture = new Fixture()) {
      NativeControllerResource resource = fixture._resource;
      Schema schema = schema("orders");
      assertOk(resource.createSchema(schema.toSingleLineJsonString(), false, false, createHeaders()));
      String firstSchemaTag = resource.schema("orders", headers()).getHeaderString(HttpHeaders.ETAG);
      String table = JsonUtils.objectToString(new TableConfigBuilder(TableType.OFFLINE).setTableName("orders")
          .setTimeColumnName("ts").setNumReplicas(1).build());
      assertOk(resource.createTable(table, null, createHeaders()));
      assertError(409, () -> resource.deleteSchema("orders", matchHeaders(firstSchemaTag)));
      Schema changed = schema("orders");
      changed.addField(new DimensionFieldSpec("added", DataType.STRING, true));
      assertOk(resource.updateSchema("orders", changed.toSingleLineJsonString(), false, false,
          matchHeaders(firstSchemaTag)));
      assertError(412, () -> resource.updateSchema("orders", changed.toSingleLineJsonString(), false, false,
          matchHeaders(firstSchemaTag)));
      String tableTag = resource.table("orders_OFFLINE", null, headers()).getHeaderString(HttpHeaders.ETAG);
      JsonNode updated = JsonUtils.stringToJsonNode(table);
      ((ObjectNode) updated).put("description", "metadata-only edit");
      Response tableUpdated = resource.updateTable("orders_OFFLINE", updated.toString(), null, false,
          matchHeaders(tableTag));
      assertOk(tableUpdated);
      ((ObjectNode) updated).put("description", "${HOST_SPECIFIC_SETTING:default}");
      assertError(501, () -> resource.updateTable("orders_OFFLINE", updated.toString(), null, false,
          matchHeaders(tableUpdated.getHeaderString(HttpHeaders.ETAG))));
      assertOk(resource.updateClusterConfig("{\"one\":\"1\",\"two\":\"2\"}", createHeaders()));
      String clusterTag = resource.clusterConfigs(headers()).getHeaderString(HttpHeaders.ETAG);
      assertOk(resource.updateClusterConfig("{\"one\":null,\"two\":\"changed\"}", matchHeaders(clusterTag)));
      assertEquals(JsonUtils.stringToJsonNode((String) resource.clusterConfigs(headers()).getEntity()),
          JsonUtils.objectToJsonNode(Map.of("two", "changed")));
      assertError(412, () -> resource.deleteClusterConfig("two", matchHeaders(clusterTag)));
      assertError(428, () -> resource.updateClusterConfig("{}", headers()));
      String currentClusterTag = resource.clusterConfigs(headers()).getHeaderString(HttpHeaders.ETAG);
      assertError(501, () -> resource.updateClusterConfig("{\"key\":\"${HOST_SPECIFIC_SETTING}\"}",
          matchHeaders(currentClusterTag)));
      assertError(400, () -> resource.createSchema("{\"schemaName\":\"a\",\"schemaName\":\"b\"}",
          false, false, createHeaders()));
      assertError(400, () -> resource.createSchema("{\"schemaName\":\"a\",\"unknownProperty\":true}",
          false, false, createHeaders()));
      ObjectNode misspelledTable = (ObjectNode) JsonUtils.stringToJsonNode(table);
      misspelledTable.withObject("/segmentsConfig").put("replicaton", "3");
      assertError(400, () -> resource.createTable(misspelledTable.toString(), null, createHeaders()));
      assertError(400, () -> resource.createSchema("{\"schemaName\":\"script\",\"dimensionFieldSpecs\":["
          + "{\"name\":\"id\",\"dataType\":\"STRING\",\"transformFunction\":\"Groovy({1}, x)\"}]}",
          false, false, createHeaders()));
      String realtime = JsonUtils.objectToString(new TableConfigBuilder(TableType.REALTIME).setTableName("orders")
          .build());
      assertError(501, () -> resource.createTable(realtime, null, createHeaders()));
      assertEquals(fixture._catalog.captureSnapshot().path("clients").size(), 1,
          "HTTP writes must reuse one bounded controller stream rather than allocating a client per request");
    }
  }

  @Test
  public void testPlacementPolicyIsStrictAndOnlyItsExactKeyBypassesLifecycleGate() throws Exception {
    try (Fixture fixture = new Fixture()) {
      String key = NativeReplicaPlacementPolicy.CONFIG_KEY;
      assertError(400, () -> fixture._resource.updateClusterConfig(
          JsonUtils.objectToString(Map.of(key, "{\"servers\":{},\"ignored\":true}")), createHeaders()));
      assertError(400, () -> fixture._resource.updateClusterConfig(
          JsonUtils.objectToString(Map.of(key, "{\"servers\":{},\"servers\":{}}")), createHeaders()));
      Map<String, String> approved = Map.of(key,
          "{\"servers\":{\"Server_one\":{\"tenant\":\"DefaultTenant\",\"failureDomain\":\"zone-a\"}}}");
      assertOk(fixture._resource.updateClusterConfig(JsonUtils.objectToString(approved), createHeaders()));
      var captured = fixture._metadata.snapshot();
      var withNativeState = new NativeControllerMetadata.Snapshot(captured.revision(), captured.clusterConfig(),
          captured.schemas(), captured.tables(), captured.control(), captured.participants(), captured.clusterRecord(),
          Set.of("orders_OFFLINE"), captured.contents(), captured.assignments());
      fixture._metadata.validateClusterConfig(withNativeState, Map.of(key, "{\"servers\":{}}"));
      fixture._metadata.validateClusterConfig(withNativeState, Map.of());
      fixture._metadata.validateClusterConfig(withNativeState, Map.of(key, "{\"servers\":{}}",
          NativeTableSizeMetadataProvider.ENDPOINT_APPROVAL_PREFIX + "Server_one", "https://server:8443"));
      expectThrows(UnsupportedOperationException.class, () -> fixture._metadata.validateClusterConfig(withNativeState,
          Map.of(key, "{\"servers\":{}}", key + ".ignored", "true")));
      expectThrows(IllegalArgumentException.class, () -> fixture._metadata.validateClusterConfig(withNativeState,
          Map.of(key, "{\"servers\":{\"Server_one\":{\"tenant\":\"DefaultTenant\"}}}")));
    }
  }

  @Test
  public void testAmbiguousNamesCannotEscapeBasicAuthTableScope() throws Exception {
    try (Fixture fixture = new Fixture()) {
      BasicAuthAccessControlFactory factory = new BasicAuthAccessControlFactory();
      factory.initWithoutHelix(new PinotConfiguration(Map.of(
          "controller.admin.access.control.principals", "scoped",
          "controller.admin.access.control.principals.scoped.password", "password",
          "controller.admin.access.control.principals.scoped.tables", "orders",
          "controller.admin.access.control.principals.scoped.permissions", "create,read,update,delete")));
      NativeControllerResource resource = new NativeControllerResource(fixture._metadata, factory);
      HttpHeaders authenticated = createHeaders();
      when(authenticated.getRequestHeader(HttpHeaders.AUTHORIZATION))
          .thenReturn(List.of(BasicAuthTokenUtils.toBasicAuthToken("scoped", "password")));
      assertOk(resource.createSchema(schema("orders").toSingleLineJsonString(), false, false, authenticated));
      for (String name : List.of("orders_OFFLINE", "orders_OFFLINE_OFFLINE", "orders_REALTIME")) {
        assertError(400, () -> resource.createSchema(schema(name).toSingleLineJsonString(), false, false,
            authenticated));
        assertError(400, () -> resource.schema(name, authenticated));
      }
      String ambiguousTable = JsonUtils.objectToString(new TableConfigBuilder(TableType.OFFLINE)
          .setTableName("orders_OFFLINE_OFFLINE_OFFLINE").setTimeColumnName("ts").setNumReplicas(1).build());
      assertError(400, () -> resource.createTable(ambiguousTable, null, authenticated));
      assertError(400, () -> resource.table("orders_OFFLINE_OFFLINE", null, authenticated));
      assertError(400, () -> resource.tableSchema("orders_OFFLINE_OFFLINE", authenticated));
      assertEquals(fixture._metadata.snapshot().schemas().keySet(), Set.of("orders"));
      assertTrue(fixture._metadata.snapshot().tables().isEmpty());
    }
  }

  @Test
  public void testLostReplyRestartAndCreateTombstoneFence() throws Exception {
    try (Fixture fixture = new Fixture()) {
      Schema schema = schema("orders");
      HttpHeaders create = createHeaders();
      fixture._loseReply = true;
      WebApplicationException lost = assertError(503,
          () -> fixture._resource.createSchema(schema.toSingleLineJsonString(), false, false, create));
      assertTrue(JsonUtils.objectToJsonNode(lost.getResponse().getEntity()).path("outcomeUnknown").asBoolean());
      fixture.restart();
      assertError(409, () -> fixture._resource.createSchema(schema.toSingleLineJsonString(), false, false, create));
      String tag = fixture._resource.schema("orders", headers()).getHeaderString(HttpHeaders.ETAG);
      assertOk(fixture._resource.deleteSchema("orders", matchHeaders(tag)));
      assertError(409, () -> fixture._resource.createSchema(schema.toSingleLineJsonString(), false, false, create));
      assertError(404, () -> fixture._resource.schema("orders", headers()));
      assertOk(fixture._resource.createSchema(schema.toSingleLineJsonString(), false, false, createHeaders()));
      assertEquals(fixture._catalog.captureSnapshot().path("clients").size(), 1);
      StreamTipResult tip = fixture.query(new GetStreamTip(fixture._history, "controller-http/test-controller", 1,
          "config"), 1000);
      assertTrue(tip.sequence() > 1);
      assertEquals(tip.floor(), tip.sequence() - 1);
    }
  }

  @Test
  public void testDependencyMutationAfterValidationCannotCommit() throws Exception {
    try (Fixture fixture = new Fixture()) {
      assertOk(fixture._resource.createSchema(schema("orders").toSingleLineJsonString(), false, false,
          createHeaders()));
      var record = fixture._metadata.snapshot().schemas().get("orders");
      fixture._beforeWrite = () -> {
        assertEquals(fixture.execute(new RegisterClient(fixture._history, "other-controller", UUID.randomUUID(), 0),
            1000).status(), Status.OK);
        assertEquals(fixture.execute(new Transaction(fixture._history, "other-controller", 1, "config", 1,
            List.of(Mutation.delete(RecordKind.SCHEMA, "orders",
                new ObjectVersion(fixture._history, record.objectId(), record.version())))), 1000).status(), Status.OK);
      };
      String table = JsonUtils.objectToString(new TableConfigBuilder(TableType.OFFLINE).setTableName("orders")
          .setTimeColumnName("ts").setNumReplicas(1).build());
      WebApplicationException conflict = assertError(409,
          () -> fixture._resource.createTable(table, null, createHeaders()));
      assertFalse(JsonUtils.objectToJsonNode(conflict.getResponse().getEntity()).path("outcomeUnknown").asBoolean());
      assertTrue(fixture._metadata.snapshot().tables().isEmpty());
      assertTrue(fixture._metadata.snapshot().schemas().isEmpty());
    }
  }

  private static Schema schema(String name) {
    return new Schema.SchemaBuilder().setSchemaName(name).addSingleValueDimension("id", DataType.STRING)
        .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
  }

  private static HttpHeaders headers() {
    return mock(HttpHeaders.class);
  }

  private static HttpHeaders createHeaders() {
    HttpHeaders headers = headers();
    when(headers.getHeaderString(HttpHeaders.IF_NONE_MATCH)).thenReturn("*");
    when(headers.getHeaderString("Idempotency-Key")).thenReturn(UUID.randomUUID().toString());
    return headers;
  }

  private static HttpHeaders matchHeaders(String tag) {
    HttpHeaders headers = headers();
    when(headers.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn(tag);
    return headers;
  }

  private static void assertOk(Response response) {
    assertEquals(response.getStatus(), 200);
    assertEquals(JsonUtils.objectToJsonNode(response.getEntity()).path("status").asText(), "OK");
    assertTrue(response.getHeaderString(NativeControllerResource.REVISION_HEADER).contains(":"));
    assertTrue(response.getHeaderString(HttpHeaders.ETAG).startsWith("\""));
  }

  private static WebApplicationException assertError(int status, ThrowingRequest request) {
    WebApplicationException failure = expectThrows(WebApplicationException.class, request::run);
    assertEquals(failure.getResponse().getStatus(), status);
    return failure;
  }

  @FunctionalInterface
  private interface ThrowingRequest {
    void run() throws Exception;
  }

  private static final class Fixture implements MetadataAccess, AutoCloseable {
    private final UUID _history = UUID.randomUUID();
    private final MetadataCatalog _catalog = new MetadataCatalog(UUID.randomUUID(), _history);
    private final ControllerConf _config = new ControllerConf();
    private NativeControllerMetadata _metadata;
    private NativeControllerResource _resource;
    private long _index;
    private boolean _loseReply;
    private ThrowingRequest _beforeWrite;

    private Fixture() throws Exception {
      _config.setProperty(NativeMetadataConfig.PREFIX + "id", "test-controller");
      _config.setProperty(ControllerConf.HELIX_CLUSTER_NAME, "native-writes");
      _config.setProperty(NativeMetadataConfig.PREFIX + "controller.http.configRefreshMs", 60_000L);
      var activated = MembershipProtocol.decodeResponse(apply(new MembershipCommand(_history, "bootstrap",
          new ActivateControlEpoch(1, "leader", UUID.randomUUID()))));
      assertEquals(activated.status(), Status.OK);
      var configured = MembershipProtocol.decodeResponse(apply(new MembershipCommand(_history, "bootstrap",
          new ConfigurePolicy(activated.control().controlEpoch(), 0, new Policy(1, 1000, 4, 1000, 500, 100)))));
      assertEquals(configured.status(), Status.OK);
      restart();
    }

    private void restart() throws Exception {
      if (_metadata != null) {
        _metadata.close();
      }
      _metadata = new NativeControllerMetadata(this, _history, _config);
      _metadata.start();
      _resource = new NativeControllerResource(_metadata, () -> new AccessControl() { });
    }

    private byte[] apply(Command command) throws IOException {
      return _catalog.execute(MetadataProtocol.encode(command), 1, ++_index);
    }

    @Override
    public synchronized Result execute(Command command, long timeoutMillis) throws IOException {
      if (command instanceof ConfigTransaction && _beforeWrite != null) {
        ThrowingRequest before = _beforeWrite;
        _beforeWrite = null;
        try {
          before.run();
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      Result result = MetadataProtocol.decodeResult(apply(command));
      if (command instanceof ConfigTransaction && _loseReply) {
        _loseReply = false;
        throw new IOException("reply lost after commit");
      }
      return result;
    }

    @Override
    public synchronized Result query(Query query, long timeoutMillis) throws IOException {
      return MetadataProtocol.decodeResult(apply(new ReadBarrier(query)));
    }

    @Override
    public synchronized StreamTipResult query(GetStreamTip query, long timeoutMillis) throws IOException {
      return MetadataProtocol.decodeStreamTipResult(apply(new ReadBarrier(query)));
    }

    @Override
    public synchronized CatalogImage image(FeedScope scope, int maxRecords, int maxBytes, long timeoutMillis)
        throws IOException {
      query(new GetRevision(_history), timeoutMillis);
      return _catalog.captureImage(scope, maxRecords, maxBytes);
    }

    @Override
    public ChangePage changes(FeedScope scope, FeedCursor cursor, int maxTransactions, int maxRecords, int maxBytes,
        long timeoutMillis) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws InterruptedException {
      _metadata.close();
    }
  }
}
