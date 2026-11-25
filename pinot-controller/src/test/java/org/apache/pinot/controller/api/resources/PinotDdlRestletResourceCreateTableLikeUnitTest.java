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

import java.io.IOException;
import java.util.ConcurrentModificationException;
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
import org.glassfish.grizzly.http.server.Request;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
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
      verify(fixture._manager).addTableIfRawNameAbsent(configs.capture(),
          eq(Schema.cloneSchemaWithName(fixture._schema, "copied_table")));
      verify(fixture._manager, times(expectedTables.size() - 1)).addTable(configs.capture());
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
      verify(fixture._manager).addTableIfRawNameAbsent(configCaptor.capture(), any(Schema.class));
      TableConfig copied = configCaptor.getValue();
      assertEquals(copied.getIngestionConfig().getStreamIngestionConfig().getStreamConfigMaps().getFirst()
          .get("stream.kafka.topic.name"), "${LIKE_TOPIC}");
      assertEquals(copied.getIndexingConfig().getSortedColumn(), List.of("id"));
      assertEquals(JsonUtils.objectToString(fixture._schema), originalSchema);
      assertEquals(JsonUtils.objectToString(fixture._realtime), originalConfig);
      assertEquals(body(response).getTableConfigs().get("schema").get("primaryKeyColumns").get(0).asText(), "id");
      verify(fixture._manager, never()).getTableConfig("old_REALTIME");
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

  @DataProvider(name = "unreadableSourceVariants")
  public Object[][] unreadableSourceVariants() {
    return new Object[][]{
        {TableType.OFFLINE, new IOException("invalid source metadata")},
        {TableType.REALTIME, new IOException("invalid source metadata")},
        {TableType.OFFLINE, new IllegalStateException("missing required metadata field")},
        {TableType.REALTIME, new IllegalStateException("missing required metadata field")}
    };
  }

  @Test(dataProvider = "unreadableSourceVariants")
  public void unreadableSourceVariantFailsBeforeAnyWrite(TableType unreadableType, Exception readFailure)
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, true)) {
      String unreadableTable = "old_" + unreadableType;
      when(fixture._manager.getRawTableConfig(unreadableTable)).thenThrow(readFailure);

      for (String typeClause : List.of("", " TYPE " + unreadableType)) {
        ControllerApplicationException failure = expectThrows(ControllerApplicationException.class,
            () -> fixture.execute("CREATE TABLE copied_table LIKE old" + typeClause, false));
        assertEquals(failure.getResponse().getStatus(), 500);
        assertTrue(failure.getMessage().contains(unreadableTable));
        fixture.verifyNoWrites();
      }
    }
  }

  @Test(dataProvider = "unreadableSourceVariants")
  public void explicitHealthyVariantSkipsUnreadableSibling(TableType unreadableType, Exception readFailure)
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, true)) {
      String unreadableTable = "old_" + unreadableType;
      when(fixture._manager.getRawTableConfig(unreadableTable)).thenThrow(readFailure);
      TableType selectedType = unreadableType == TableType.OFFLINE ? TableType.REALTIME : TableType.OFFLINE;

      Response response = fixture.execute("CREATE TABLE copied_table LIKE old TYPE " + selectedType, false);
      assertEquals(response.getStatus(), 201);
      assertEquals(body(response).getCreatedTables(), List.of("copied_table_" + selectedType));
      verify(fixture._manager, never()).getRawTableConfig(unreadableTable);
    }
  }

  @Test
  public void malformedTargetConfigWithoutIdealStateFailsBeforeSchemaCreation()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      when(fixture._manager.getRawTableConfig("copied_table_REALTIME")).thenThrow(new IOException("invalid metadata"));

      fixture.assertStatus("CREATE TABLE IF NOT EXISTS copied_table LIKE old TYPE OFFLINE", 500);
      fixture.verifyNoWrites();
    }
  }

  @Test
  public void unreadableSourceOrTargetSchemaFailsBeforeAnyWrite()
      throws Exception {
    for (String name : List.of("old", "copied_table")) {
      try (Fixture fixture = new Fixture("old", true, false)) {
        when(fixture._manager.getSchemaStrict(name)).thenThrow(new IOException("invalid schema"));
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
      when(fixture._manager.getRawTableConfig("old_OFFLINE")).thenReturn(mv);
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
      when(fixture._manager.getRawTableConfig("copied_table_OFFLINE"))
          .thenReturn(newConfig("copied_table", TableType.OFFLINE));

      Response response = fixture.execute("CREATE TABLE IF NOT EXISTS copied_table LIKE old", false);
      assertEquals(response.getStatus(), 200);
      assertNull(body(response).getTableConfigs(), "A no-op must not advertise a plan as applied configuration");
      fixture.verifyNoWrites();
      fixture.assertStatus("CREATE TABLE copied_table LIKE old", 409);
      verify(fixture._store, never()).exists(ZKMetadataProvider.constructPropertyStorePathForLogical("old"),
          AccessOption.PERSISTENT);
      verify(fixture._manager, never()).getRawTableConfig("old_OFFLINE");
      verify(fixture._manager, never()).getRawTableConfig("old_REALTIME");
      verify(fixture._manager, never()).getSchemaStrict("old");
    }
  }

  @Test
  public void ifNotExistsDoesNotSuppressMaterializedViewTargetMismatch()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      TableConfig mv = new TableConfigBuilder(TableType.OFFLINE).setTableName("copied_table")
          .setIsMaterializedView(true).build();
      when(fixture._manager.getRawTableConfig("copied_table_OFFLINE")).thenReturn(mv);
      fixture.assertStatus("CREATE TABLE IF NOT EXISTS copied_table LIKE old", 400);
      fixture.verifyNoWrites();
    }
  }

  @Test
  public void appliesExistencePolicyWhenSiblingWinsAtFirstWrite()
      throws Exception {
    for (boolean ifNotExists : List.of(false, true)) {
      try (Fixture fixture = new Fixture("old", true, false)) {
        doAnswer(invocation -> {
          when(fixture._manager.getRawTableConfig("copied_table_REALTIME"))
              .thenReturn(newConfig("copied_table", TableType.REALTIME));
          throw new TableAlreadyExistsException("Sibling already exists");
        }).when(fixture._manager).addTableIfRawNameAbsent(any(), any());
        String sql = "CREATE TABLE " + (ifNotExists ? "IF NOT EXISTS " : "") + "copied_table LIKE old";
        Response response = fixture.execute(sql, false);
        assertEquals(response.getStatus(), ifNotExists ? 200 : 409);
        assertTrue(body(response).getCreatedTables().isEmpty());
        verify(fixture._manager, never()).addTable(any());
      }
    }
  }

  @Test
  public void existingSchemaMustMatchAllCopiedMetadata()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, false)) {
      Schema existing = Schema.cloneSchemaWithName(fixture._schema, "copied_table");
      existing.setPrimaryKeyColumns(List.of());
      when(fixture._manager.getSchemaStrict("copied_table")).thenReturn(existing);
      fixture.assertStatus("CREATE TABLE copied_table LIKE old", 409);
      fixture.verifyNoWrites();
    }
    try (Fixture fixture = new Fixture("old", true, false)) {
      when(fixture._manager.getSchemaStrict("copied_table"))
          .thenReturn(Schema.cloneSchemaWithName(fixture._schema, "copied_table"));
      assertEquals(fixture.execute("CREATE TABLE copied_table LIKE old", false).getStatus(), 201);
      verify(fixture._manager, never()).addSchema(any(), eq(false), eq(false));
      verify(fixture._manager).addTableIfRawNameAbsent(any(TableConfig.class), any(Schema.class));
    }
  }

  @Test
  public void schemaChangedDuringPublicationReturnsConflictEvenWithIfNotExists()
      throws Exception {
    try (Fixture fixture = new Fixture("old", true, true)) {
      doThrow(new ConcurrentModificationException("Destination schema changed")).when(fixture._manager)
          .addTableIfRawNameAbsent(any(), any());
      Response response = fixture.execute("CREATE TABLE IF NOT EXISTS copied_table LIKE old", false);
      assertEquals(response.getStatus(), 409);
      assertTrue(body(response).getCreatedTables().isEmpty());
      verify(fixture._manager, never()).addTable(any());
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
          when(fixture._manager.getSchemaStrict("copied_table"))
              .thenReturn(Schema.cloneSchemaWithName(fixture._schema, "copied_table"));
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
      when(fixture._manager.getSchemaStrict("old")).thenReturn(legacy);
      for (boolean dryRun : List.of(false, true)) {
        ControllerApplicationException failure = expectThrows(ControllerApplicationException.class,
            () -> fixture.execute("CREATE TABLE copied_table LIKE old", dryRun));
        assertEquals(failure.getResponse().getStatus(), 400);
        assertTrue(failure.getMessage().contains("TimeFieldSpec"));
      }
      fixture.verifyNoWrites();

      when(fixture._manager.getSchemaStrict("copied_table"))
          .thenReturn(Schema.cloneSchemaWithName(legacy, "copied_table"));
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
      verify(fixture._manager, never()).getRawTableConfig("old_OFFLINE");
      verify(fixture._manager, never()).getRawTableConfig("old_REALTIME");
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
      verify(fixture._manager, never()).getRawTableConfig("copied_table_OFFLINE");
      verify(fixture._manager, never()).getRawTableConfig("copied_table_REALTIME");
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
      verify(fixture._manager, never()).getSchemaStrict(any());
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
      PinotHelixResourceManager failingManager = doThrow(new IOException("injected create failure"))
          .when(fixture._manager);
      if (failedType == TableType.OFFLINE) {
        failingManager.addTableIfRawNameAbsent(any(), any());
      } else {
        failingManager.addTable(any());
      }
      Response response = fixture.execute("CREATE TABLE copied_table LIKE old", false);
      assertEquals(response.getStatus(), 500);
      assertEquals(body(response).getCreatedTables(), createdTables);
      assertTrue(body(response).getMessage().contains("injected create failure"));
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
        throws IOException {
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
      when(_manager.getRawTableConfig(source + "_OFFLINE"))
          .thenReturn(offline ? newConfig(source, TableType.OFFLINE) : null);
      when(_manager.getRawTableConfig(source + "_REALTIME")).thenReturn(_realtime);
      when(_manager.getSchemaStrict(source)).thenReturn(_schema);
      when(_manager.getTableCache()).thenReturn(mock(TableCache.class));
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
      verify(_manager, never()).addTableIfRawNameAbsent(any(TableConfig.class), any(Schema.class));
    }

    @Override
    public void close() {
      _tuner.close();
      _tasks.close();
      _validation.close();
    }
  }
}
