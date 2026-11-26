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
package org.apache.pinot.controller.api.resources;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.SimpleHttpErrorInfo;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.controller.api.resources.ddl.DdlExecutionRequest;
import org.apache.pinot.controller.api.resources.ddl.DdlExecutionResponse;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.tuner.TableConfigTunerUtils;
import org.apache.pinot.core.auth.TargetType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests the CREATE TABLE LIKE executor's metadata isolation, hybrid planning, authorization, and failure contract.
/// Static validation collaborators are mocked; execution and the explicit hybrid compatibility check remain real.
/// Each fixture owns its mocks and is used by one test invocation at a time.
public class PinotDdlRestletResourceCreateTableLikeUnitTest {
  @DataProvider(name = "sourceShapes")
  public Object[][] sourceShapes() {
    return new Object[][]{
        {true, false, "", List.of("copied_table_OFFLINE")},
        {false, true, "", List.of("copied_table_REALTIME")},
        {true, true, "", List.of("copied_table_OFFLINE", "copied_table_REALTIME")},
        {true, true, " TYPE OFFLINE", List.of("copied_table_OFFLINE")},
        {true, true, " TYPE REALTIME", List.of("copied_table_REALTIME")}
    };
  }

  @Test(dataProvider = "sourceShapes")
  public void createsSelectedSourceShape(boolean offline, boolean realtime, String typeClause,
      List<String> expectedTables)
      throws Exception {
    try (Fixture fixture = new Fixture("old", offline, realtime)) {
      Response response = fixture.execute("CREATE TABLE copied_table LIKE old" + typeClause, false);
      assertEquals(response.getStatus(), 201);
      DdlExecutionResponse body = body(response);
      assertEquals(body.getOperation().name(), "CREATE_TABLE_LIKE");
      assertEquals(body.getSourceTableName(), "old");
      assertEquals(body.getTableName(), "copied_table");
      assertEquals(body.getCreatedTables(), expectedTables);
      assertEquals(body.getTableConfigs().get("schema").get("schemaName").asText(), "copied_table");
      assertEquals(body.getTableConfigs().has("offline"), expectedTables.contains("copied_table_OFFLINE"));
      assertEquals(body.getTableConfigs().has("realtime"), expectedTables.contains("copied_table_REALTIME"));

      ArgumentCaptor<TableConfig> configs = ArgumentCaptor.forClass(TableConfig.class);
      verify(fixture._manager, times(expectedTables.size())).addTable(configs.capture());
      assertEquals(configs.getAllValues().stream().map(TableConfig::getTableName).toList(), expectedTables);
      verify(fixture._manager).addSchema(any(Schema.class), eq(false), eq(false));
    }
  }

  @Test
  public void deepCopyPreservesSourceAndSymbolicStreamSettings()
      throws Exception {
    try (Fixture fixture = new Fixture("old", false, true)) {
      IngestionConfig ingestionConfig = new IngestionConfig();
      ingestionConfig.setStreamIngestionConfig(
          new StreamIngestionConfig(List.of(Map.of("stream.kafka.topic.name", "${LIKE_TOPIC}"))));
      fixture._realtime.setIngestionConfig(ingestionConfig);
      String originalSchema = JsonUtils.objectToString(fixture._schema);
      String originalConfig = JsonUtils.objectToString(fixture._realtime);
      fixture._tuner.when(() -> TableConfigTunerUtils.applyTunerConfigs(any(), any(), any(), any()))
          .thenAnswer(invocation -> {
            TableConfig config = invocation.getArgument(1);
            config.getIndexingConfig().setSortedColumn(List.of("id"));
            return null;
          });

      Response response = fixture.execute("CREATE TABLE copied_table LIKE old", false);
      assertEquals(response.getStatus(), 201);
      ArgumentCaptor<TableConfig> configCaptor = ArgumentCaptor.forClass(TableConfig.class);
      verify(fixture._manager).addTable(configCaptor.capture());
      TableConfig copied = configCaptor.getValue();
      assertEquals(copied.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps().getFirst()
          .get("stream.kafka.topic.name"), "${LIKE_TOPIC}");
      assertEquals(copied.getIndexingConfig().getSortedColumn(), List.of("id"));
      assertEquals(JsonUtils.objectToString(fixture._schema), originalSchema);
      assertEquals(JsonUtils.objectToString(fixture._realtime), originalConfig);
      assertEquals(body(response).getTableConfigs().get("schema").get("primaryKeyColumns").get(0).asText(), "id");
      verify(fixture._manager).getRealtimeTableConfig("old", false, false);
    }
  }

  @Test
  public void bothHybridVariantsValidateAgainstSchemaAfterAllTunersFinish()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, true)) {
      String originalSchema = JsonUtils.objectToString(fixture._schema);
      EnumSet<TableType> validatedTypes = EnumSet.noneOf(TableType.class);
      fixture._tuner.when(() -> TableConfigTunerUtils.applyTunerConfigs(any(), any(), any(), any()))
          .thenAnswer(invocation -> {
            TableConfig config = invocation.getArgument(1);
            if (config.getTableType() == TableType.REALTIME) {
              Schema schema = invocation.getArgument(2);
              schema.setDescription("schema prepared by realtime tuner");
            }
            return null;
          });
      fixture._validation.when(() -> TableConfigValidationUtils.validateTableConfig(
          any(), any(), any(), any(), any(), any()))
          .thenAnswer(invocation -> {
            TableConfig config = invocation.getArgument(0);
            Schema schema = invocation.getArgument(1);
            assertEquals(schema.getDescription(), "schema prepared by realtime tuner",
                "Every variant must validate the final shared schema");
            validatedTypes.add(config.getTableType());
            return null;
          });

      Response response = fixture.execute("CREATE TABLE copied_table LIKE old", false);
      assertEquals(response.getStatus(), 201);
      assertEquals(validatedTypes, EnumSet.of(TableType.OFFLINE, TableType.REALTIME));
      assertEquals(body(response).getTableConfigs().get("schema").get("description").asText(),
          "schema prepared by realtime tuner");
      assertEquals(JsonUtils.objectToString(fixture._schema), originalSchema);
    }
  }

  @Test
  public void dryRunReturnsHybridPlanWithoutWriting()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, true)) {
      Response response = fixture.execute("CREATE TABLE copied_table LIKE old", true);
      assertEquals(response.getStatus(), 200);
      assertTrue(body(response).isDryRun());
      assertNotNull(body(response).getTableConfigs().get("offline"));
      assertNotNull(body(response).getTableConfigs().get("realtime"));
      fixture.verifyNoWrites();
    }
  }

  @Test
  public void incompatibleHybridIsRejectedBeforeAnyWrite()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, true)) {
      fixture._realtime.getValidationConfig().setTimeColumnName("otherTime");
      fixture.assertStatus("CREATE TABLE copied_table LIKE old", 400);
      fixture.verifyNoWrites();
    }
  }

  @Test
  public void realtimeValidationFailureDoesNotLeaveOfflineTable()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, true)) {
      fixture._validation.when(() -> TableConfigValidationUtils.validateTableConfig(
          argThat(config -> config != null && config.getTableType() == TableType.REALTIME),
          any(), any(), any(), any(), any()))
          .thenThrow(new IllegalStateException("realtime config rejected"));

      fixture.assertStatus("CREATE TABLE copied_table LIKE old", 400);
      fixture.verifyNoWrites();
    }
  }

  @Test
  public void missingSourceAndMissingSelectedVariantReturn404()
      throws Exception {
    try (Fixture fixture = new Fixture("old", false, false)) {
      fixture.assertStatus("CREATE TABLE copied_table LIKE old", 404);
      fixture.verifyNoWrites();
    }
    try (Fixture fixture = new Fixture("old", true, false)) {
      fixture.assertStatus("CREATE TABLE copied_table LIKE old TYPE REALTIME", 404);
      fixture.verifyNoWrites();
    }
  }

  @Test
  public void unreadableSourceVariantIsRejectedOnlyWhenSelected()
      throws Exception {
    for (TableType unreadableType : TableType.values()) {
      boolean offlineReadable = unreadableType == TableType.REALTIME;
      try (Fixture fixture = new Fixture("old", offlineReadable, !offlineReadable)) {
        String unreadableTable = "old_" + unreadableType;
        String path = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(unreadableTable);
        when(fixture._store.exists(path, AccessOption.PERSISTENT)).thenReturn(true);
        TableType readableType = offlineReadable ? TableType.OFFLINE : TableType.REALTIME;
        Response response = fixture.execute("CREATE TABLE copied_table LIKE old TYPE " + readableType, true);
        assertEquals(response.getStatus(), 200);
        verify(fixture._store, never()).exists(path, AccessOption.PERSISTENT);

        for (String typeClause : List.of("", " TYPE " + unreadableType)) {
          ControllerApplicationException failure = expectThrows(ControllerApplicationException.class,
              () -> fixture.execute("CREATE TABLE copied_table LIKE old" + typeClause, false));
          assertEquals(failure.getResponse().getStatus(), 500);
          assertTrue(failure.getMessage().contains(unreadableTable));
        }
        fixture.verifyNoWrites();
      }
    }
  }

  @Test
  public void malformedTargetConfigWithoutIdealStateFailsBeforeSchemaCreation()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      String path = ZKMetadataProvider.constructPropertyStorePathForResourceConfig("copied_table_REALTIME");
      when(fixture._store.exists(path, AccessOption.PERSISTENT)).thenReturn(true);

      fixture.assertStatus("CREATE TABLE IF NOT EXISTS copied_table LIKE old TYPE OFFLINE", 500);
      fixture.verifyNoWrites();
    }
  }

  @Test
  public void unreadableSourceOrTargetSchemaFailsBeforeAnyWrite()
      throws Exception {
    for (String name : List.of("old", "copied_table")) {
      try (Fixture fixture = new Fixture("old", true, false)) {
        when(fixture._manager.getSchema(name)).thenReturn(null);
        String path = ZKMetadataProvider.constructPropertyStorePathForSchema(name);
        when(fixture._store.exists(path, AccessOption.PERSISTENT)).thenReturn(true);
        ControllerApplicationException failure = expectThrows(ControllerApplicationException.class,
            () -> fixture.execute("CREATE TABLE copied_table LIKE old", false));
        assertEquals(failure.getResponse().getStatus(), 500);
        assertTrue(failure.getMessage().contains(name));
        fixture.verifyNoWrites();
      }
    }
  }

  @Test
  public void materializedViewSourceIsRejected()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      TableConfig mv = new TableConfigBuilder(TableType.OFFLINE).setTableName("old")
          .setIsMaterializedView(true).build();
      fixture.storeConfig(mv);
      fixture.assertStatus("CREATE TABLE copied_table LIKE old", 400);
      fixture.verifyNoWrites();
    }
  }

  @Test
  public void logicalSourceOrTargetIsRejectedEvenWhenConfigIsUnreadable()
      throws Exception {
    for (String name : List.of("old", "copied_table")) {
      try (Fixture fixture = new Fixture("old", true, false)) {
        String path = ZKMetadataProvider.constructPropertyStorePathForLogical(name);
        when(fixture._store.exists(path, AccessOption.PERSISTENT)).thenReturn(true);
        for (boolean dryRun : List.of(false, true)) {
          ControllerApplicationException failure = expectThrows(ControllerApplicationException.class,
              () -> fixture.execute("CREATE TABLE IF NOT EXISTS copied_table LIKE old", dryRun));
          assertEquals(failure.getResponse().getStatus(), name.equals("old") ? 400 : 409);
        }
        verify(fixture._manager, never()).getLogicalTableConfig(name);
        fixture.verifyNoWrites();
      }
    }
  }

  @Test
  public void ifNotExistsDoesNotCompleteExistingHalfOfHybrid()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, true)) {
      when(fixture._manager.hasOfflineTable("copied_table")).thenReturn(true);
      fixture.storeConfig(newConfig("copied_table", TableType.OFFLINE));

      Response response = fixture.execute("CREATE TABLE IF NOT EXISTS copied_table LIKE old", false);
      assertEquals(response.getStatus(), 200);
      assertNull(body(response).getTableConfigs(), "A no-op must not advertise a plan as applied configuration");
      fixture.verifyNoWrites();
      fixture.assertStatus("CREATE TABLE copied_table LIKE old", 409);
      verify(fixture._store, never()).exists(ZKMetadataProvider.constructPropertyStorePathForLogical("old"),
          AccessOption.PERSISTENT);
      verify(fixture._manager, never()).getOfflineTableConfig("old", false, false);
      verify(fixture._manager, never()).getRealtimeTableConfig("old", false, false);
      verify(fixture._manager, never()).getSchema("old");
    }
  }

  @Test
  public void ifNotExistsDoesNotSuppressMaterializedViewTargetMismatch()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      TableConfig mv = new TableConfigBuilder(TableType.OFFLINE).setTableName("copied_table")
          .setIsMaterializedView(true).build();
      fixture.storeConfig(mv);
      fixture.assertStatus("CREATE TABLE IF NOT EXISTS copied_table LIKE old", 400);
      fixture.verifyNoWrites();
    }
  }

  @Test
  public void ifNotExistsHonorsTargetPublishedAfterInitialAbsenceCheck()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      TableConfig target = newConfig("copied_table", TableType.OFFLINE);
      fixture.storeConfig(target);
      String path = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(target.getTableName());
      when(fixture._store.exists(path, AccessOption.PERSISTENT)).thenReturn(false, true);

      Response response = fixture.execute("CREATE TABLE IF NOT EXISTS copied_table LIKE old", false);
      assertEquals(response.getStatus(), 200);
      assertTrue(body(response).getCreatedTables().isEmpty());
      verify(fixture._manager).addSchema(any(), eq(false), eq(false));
      verify(fixture._manager, never()).addTable(any());
    }
  }

  @Test
  public void appliesExistencePolicyWhenTargetWinsAtFirstWrite()
      throws Exception {
    for (boolean ifNotExists : List.of(false, true)) {
      try (Fixture fixture = new Fixture("old", true, true)) {
        doAnswer(invocation -> {
          fixture.storeConfig(newConfig("copied_table", TableType.OFFLINE));
          throw new TableAlreadyExistsException("Target already exists");
        }).when(fixture._manager).addTable(any());
        String sql = "CREATE TABLE " + (ifNotExists ? "IF NOT EXISTS " : "") + "copied_table LIKE old";
        Response response = fixture.execute(sql, false);
        assertEquals(response.getStatus(), ifNotExists ? 200 : 409);
        if (ifNotExists) {
          assertTrue(body(response).getCreatedTables().isEmpty());
        } else {
          JsonNode error = JsonUtils.objectToJsonNode(response.getEntity());
          assertEquals(error.get("code").intValue(), 409);
          assertTrue(error.get("createdTables").isEmpty());
          assertTrue(error.get("schemaCreated").booleanValue());
        }
        verify(fixture._manager, never()).addTable(argThat(config -> config.getTableType() == TableType.REALTIME));
      }
    }
  }

  @Test
  public void existingSchemaMustMatchAllCopiedMetadata()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      Schema existing = Schema.cloneSchemaWithName(fixture._schema, "copied_table");
      existing.setPrimaryKeyColumns(List.of());
      fixture.storeSchema(existing);
      fixture.assertStatus("CREATE TABLE copied_table LIKE old", 409);
      fixture.verifyNoWrites();
    }
    try (Fixture fixture = new Fixture("old", true, false)) {
      fixture.storeSchema(Schema.cloneSchemaWithName(fixture._schema, "copied_table"));
      assertEquals(fixture.execute("CREATE TABLE copied_table LIKE old", false).getStatus(), 201);
      verify(fixture._manager, never()).addSchema(any(), eq(false), eq(false));
      verify(fixture._manager).addTable(any(TableConfig.class));
    }
  }

  @Test
  public void unconfirmedSchemaWriteDoesNotReportCreatedResources()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      doNothing().when(fixture._manager).addSchema(any(Schema.class), eq(false), eq(false));
      Response response = fixture.execute("CREATE TABLE IF NOT EXISTS copied_table LIKE old", false);
      assertEquals(response.getStatus(), 500);
      JsonNode error = JsonUtils.objectToJsonNode(response.getEntity());
      assertFalse(error.get("schemaCreated").booleanValue());
      assertTrue(error.get("createdTables").isEmpty());
      verify(fixture._manager, never()).addTable(any());
    }
  }

  @Test
  public void rejectsSchemaChangedBeforeFirstTableWrite()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, true)) {
      Schema matching = Schema.cloneSchemaWithName(fixture._schema, "copied_table");
      Schema replacement = Schema.cloneSchemaWithName(matching, "copied_table");
      replacement.setDescription("concurrent update");
      fixture.storeSchema(matching, replacement);

      Response response = fixture.execute("CREATE TABLE IF NOT EXISTS copied_table LIKE old", false);
      assertEquals(response.getStatus(), 409);
      JsonNode error = JsonUtils.objectToJsonNode(response.getEntity());
      assertFalse(error.get("schemaCreated").booleanValue());
      assertTrue(error.get("createdTables").isEmpty());
      fixture.verifyNoWrites();
    }
  }

  @Test
  @SuppressWarnings("deprecation") // FieldSpec transforms can still occur in stored schemas.
  public void validatesIngestionGroovyPolicyBeforeSchemaExpressions()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      fixture._schema.getFieldSpecFor("id").setTransformFunction("Groovy(");
      when(fixture._resource._controllerConf.isDisableIngestionGroovy()).thenReturn(true);
      fixture._validation.when(() -> TableConfigValidationUtils.validateSchemaGroovy(any(), any()))
          .thenCallRealMethod();
      for (boolean stored : List.of(false, true)) {
        if (stored) {
          fixture.storeSchema(Schema.cloneSchemaWithName(fixture._schema, "copied_table"));
        }
        for (boolean dryRun : List.of(false, true)) {
          ControllerApplicationException failure = expectThrows(ControllerApplicationException.class,
              () -> fixture.execute("CREATE TABLE copied_table LIKE old", dryRun));
          assertEquals(failure.getResponse().getStatus(), 400);
          assertTrue(failure.getMessage().contains("Groovy transform functions are disabled"), failure.getMessage());
        }
      }
      fixture.verifyNoWrites();
    }
  }

  @Test
  @SuppressWarnings("deprecation") // Recreate a stored TIME schema from before the deprecation.
  public void deprecatedTimeFieldCanOnlyBeReusedFromExistingTargetSchema()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      Schema legacy = new Schema.SchemaBuilder().setSchemaName("old")
          .addSingleValueDimension("id", DataType.INT)
          .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "ts"), null).build();
      fixture.storeSchema(legacy);
      for (boolean dryRun : List.of(false, true)) {
        ControllerApplicationException failure = expectThrows(ControllerApplicationException.class,
            () -> fixture.execute("CREATE TABLE copied_table LIKE old", dryRun));
        assertEquals(failure.getResponse().getStatus(), 400);
        assertTrue(failure.getMessage().contains("TimeFieldSpec"));
      }
      fixture.verifyNoWrites();

      fixture.storeSchema(Schema.cloneSchemaWithName(legacy, "copied_table"));
      assertEquals(fixture.execute("CREATE TABLE copied_table LIKE old", false).getStatus(), 201);
      verify(fixture._manager, never()).addSchema(any(), eq(false), eq(false));
    }
  }

  @Test
  public void authorizesBothSourceCandidatesBeforeDiscoveringEither()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      fixture.deny("old_REALTIME");
      fixture.assertStatus("CREATE TABLE copied_table LIKE old", 403);
      verify(fixture._manager, never()).getOfflineTableConfig("old", false, false);
      verify(fixture._manager, never()).getRealtimeTableConfig("old", false, false);
      fixture.verifyNoWrites();

      // An explicit selector needs permission only on that source variant.
      assertEquals(fixture.execute("CREATE TABLE copied_table LIKE old TYPE OFFLINE", true).getStatus(), 200);
    }
  }

  @Test
  public void authorizesDestinationSiblingBeforeCheckingWholeTargetExistence()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      fixture.deny("copied_table_REALTIME");
      fixture.assertStatus("CREATE TABLE copied_table LIKE old TYPE OFFLINE", 403);
      verify(fixture._manager, never()).getOfflineTableConfig("copied_table", false, false);
      verify(fixture._manager, never()).getRealtimeTableConfig("copied_table", false, false);
      fixture.verifyNoWrites();
    }
  }

  @Test
  public void explicitSourceDatabaseIsIndependentOfDestinationHeader()
      throws Exception {
    try (Fixture fixture = new Fixture("src.old", true, false)) {
      when(fixture._headers.getHeaderString(CommonConstants.DATABASE)).thenReturn("dst");
      Response response = fixture.execute("CREATE TABLE copied_table LIKE src.old", false);
      assertEquals(response.getStatus(), 201);
      assertEquals(body(response).getSourceTableName(), "src.old");
      assertEquals(body(response).getTableName(), "dst.copied_table");
      assertEquals(body(response).getCreatedTables(), List.of("dst.copied_table_OFFLINE"));
    }
  }

  @Test
  public void typedSuffixesAreRejectedBeforeReadingMetadata()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, true)) {
      fixture.assertStatus("CREATE TABLE copied_table_OFFLINE LIKE old", 400);
      fixture.assertStatus("CREATE TABLE copied_table LIKE old_REALTIME", 400);
      verify(fixture._manager, never()).getSchema(any());
      fixture.verifyNoWrites();
    }
  }

  @DataProvider(name = "failedVariants")
  public Object[][] failedVariants() {
    return new Object[][]{
        {TableType.OFFLINE, List.of()},
        {TableType.REALTIME, List.of("copied_table_OFFLINE")}
    };
  }

  @Test(dataProvider = "failedVariants")
  public void reportsCreationProgressAndNeverDeletesSuccessfulResources(TableType failedType,
      List<String> createdTables)
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, true)) {
      doThrow(new IOException("injected create failure")).when(fixture._manager)
          .addTable(argThat(config -> config.getTableType() == failedType));
      Response response = fixture.execute("CREATE TABLE copied_table LIKE old", false);
      assertEquals(response.getStatus(), 500);
      JsonNode error = JsonUtils.objectToJsonNode(response.getEntity());
      assertEquals(error.get("createdTables"), JsonUtils.objectToJsonNode(createdTables));
      assertTrue(error.get("schemaCreated").booleanValue());
      assertFalse(error.has("tableConfigs"));
      // Use the same deserializer as HttpClient.getErrorMessage to verify that clients receive the concise reason.
      SimpleHttpErrorInfo reason = JsonUtils.stringToObject(error.toString(), SimpleHttpErrorInfo.class);
      assertEquals(reason.getCode(), 500);
      assertTrue(reason.getError().contains("injected create failure"));
      assertTrue(reason.getError().contains("created schema copied_table"));
      verify(fixture._manager, never()).deleteOfflineTable(any());
      verify(fixture._manager, never()).deleteRealtimeTable(any());
      verify(fixture._manager, never()).deleteSchema(any());
      verify(fixture._manager, never()).deleteTable(any(), any(), any());
    }
  }

  private static DdlExecutionResponse body(Response response) {
    return (DdlExecutionResponse) response.getEntity();
  }

  private static TableConfig newConfig(String rawName, TableType type) {
    return new TableConfigBuilder(type).setTableName(rawName).setTimeColumnName("ts").setNumReplicas(1).build();
  }

  private static final class Fixture implements AutoCloseable {
    private final PinotDdlRestletResource _resource = new PinotDdlRestletResource();
    private final PinotHelixResourceManager _manager = mock(PinotHelixResourceManager.class);
    private final ZkHelixPropertyStore<ZNRecord> _store = mock();
    private final HttpHeaders _headers = mock(HttpHeaders.class);
    private final Request _request = mock(Request.class);
    private final Schema _schema;
    private final TableConfig _realtime;
    private final MockedStatic<TableConfigValidationUtils> _validation = mockStatic(TableConfigValidationUtils.class);
    private final MockedStatic<PinotTableRestletResource> _tasks = mockStatic(PinotTableRestletResource.class);
    private final MockedStatic<TableConfigTunerUtils> _tuner = mockStatic(TableConfigTunerUtils.class);

    private Fixture(String source, boolean offline, boolean realtime)
        throws Exception {
      _resource._pinotHelixResourceManager = _manager;
      when(_manager.getPropertyStore()).thenReturn(_store);
      _resource._pinotHelixTaskResourceManager = mock(PinotHelixTaskResourceManager.class);
      _resource._pinotTaskManager = mock(PinotTaskManager.class);
      _resource._controllerConf = mock(ControllerConf.class);
      _resource._accessControlFactory = mock(AccessControlFactory.class);
      when(_resource._accessControlFactory.create()).thenReturn(new AccessControl() { });
      when(_request.getRequestURL()).thenReturn(new StringBuilder("http://localhost/sql/ddl"));
      _schema = new Schema.SchemaBuilder().setSchemaName(source)
          .addSingleValueDimension("id", DataType.INT)
          .addDateTime("ts", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
          .addDateTime("otherTime", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
          .setPrimaryKeyColumns(List.of("id")).setEnableColumnBasedNullHandling(true).build();
      _realtime = realtime ? newConfig(source, TableType.REALTIME) : null;
      if (offline) {
        storeConfig(newConfig(source, TableType.OFFLINE));
      }
      if (realtime) {
        storeConfig(_realtime);
      }
      storeSchema(_schema);
      doAnswer(invocation -> {
        storeSchema(invocation.getArgument(0, Schema.class));
        return null;
      }).when(_manager).addSchema(any(Schema.class), eq(false), eq(false));
      when(_manager.getTableCache()).thenReturn(mock(TableCache.class));
    }

    private void storeSchema(Schema schema, Schema... updates) {
      String name = schema.getSchemaName();
      when(_store.exists(ZKMetadataProvider.constructPropertyStorePathForSchema(name), AccessOption.PERSISTENT))
          .thenReturn(true);
      when(_manager.getSchema(name)).thenReturn(schema, updates);
    }

    private void storeConfig(TableConfig config) {
      String table = config.getTableName();
      String path = ZKMetadataProvider.constructPropertyStorePathForResourceConfig(table);
      when(_store.exists(path, AccessOption.PERSISTENT)).thenReturn(true);
      String name = TableNameBuilder.extractRawTableName(table);
      if (config.getTableType() == TableType.OFFLINE) {
        when(_manager.getOfflineTableConfig(name, false, false)).thenReturn(config);
      } else {
        when(_manager.getRealtimeTableConfig(name, false, false)).thenReturn(config);
      }
    }

    private Response execute(String sql, boolean dryRun) {
      return _resource.executeDdl(new DdlExecutionRequest(sql), dryRun, _headers, _request);
    }

    private void assertStatus(String sql, int expectedStatus) {
      ControllerApplicationException exception = expectThrows(ControllerApplicationException.class,
          () -> execute(sql, false));
      assertEquals(exception.getResponse().getStatus(), expectedStatus, exception.getMessage());
    }

    private void deny(String tableName) {
      when(_resource._accessControlFactory.create()).thenReturn(new AccessControl() {
        @Override
        public boolean hasAccess(HttpHeaders headers, TargetType targetType, String targetId, String action) {
          return !tableName.equals(targetId);
        }
      });
    }

    private void verifyNoWrites()
        throws Exception {
      verify(_manager, never()).addSchema(any(), eq(false), eq(false));
      verify(_manager, never()).addTable(any(TableConfig.class));
    }

    @Override
    public void close() {
      _tuner.close();
      _tasks.close();
      _validation.close();
    }
  }
}
