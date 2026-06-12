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
import java.util.Collections;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.exception.SchemaAlreadyExistsException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.exception.TableAlreadyExistsException;
import org.apache.pinot.controller.api.resources.ddl.DdlExecutionRequest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.MaterializedViewTask;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.ddl.compile.CompiledCreateMaterializedView;
import org.apache.pinot.sql.ddl.compile.DdlCompiler;
import org.glassfish.grizzly.http.server.Request;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Unit tests for the MV-specific paths on [PinotDdlRestletResource].
///
/// Covers (a) `CREATE MATERIALIZED VIEW`: authorization, idempotent CREATE,
/// pre-existing-schema reuse, dry-run, and the schedule-key contract (present iff DDL had
/// `REFRESH EVERY`); (b) `SHOW CREATE MATERIALIZED VIEW`: happy path, 404, and both halves
/// of the Q2=B contract (SHOW CREATE TABLE on MV → 400, SHOW CREATE MV on plain table →
/// 400); (c) `DROP MATERIALIZED VIEW`: happy path, 404 with and without `IF EXISTS`,
/// both halves of the Q2=B contract (DROP TABLE on MV → 400, DROP MV on plain table →
/// 400 even under IF EXISTS), dry-run, and the active-task short-circuit; and
/// (d) `SHOW MATERIALIZED VIEWS`: happy path, empty cluster, FROM-database scoping, and
/// permission denied.
///
/// These tests use mocks rather than a live controller because the real validation chain
/// pulls in `MaterializedViewTaskGenerator`, which lives in `pinot-minion-builtin-tasks`
/// and is NOT on the `pinot-controller` test classpath. Mocking
/// [TableConfigValidationUtils] / [PinotTableRestletResource] / [TableConfigTunerUtils]
/// lets the tests focus purely on the controller's own logic.
///
/// Integration coverage that runs `MaterializedViewAnalyzer.analyze` end-to-end lives
/// in `pinot-integration-tests` once the MV plugin is on the classpath.
public class PinotDdlRestletResourceMaterializedViewUnitTest {

  private static final String MATERIALIZED_VIEW_RAW = "mv_orders";
  private static final String MATERIALIZED_VIEW_WITH_TYPE = MATERIALIZED_VIEW_RAW + "_OFFLINE";
  private static final String SOURCE = "orders";

  /// Happy path: REFRESH EVERY supplied → schedule key set on persisted TableConfig,
  /// addSchema + addTable called, 201 CREATED.
  @Test
  public void happyPathPersistsTableWithSchedule()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(false);
    when(resource._pinotHelixResourceManager.getSchema(MATERIALIZED_VIEW_RAW)).thenReturn(null);

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      Response response = resource.executeDdl(
          new DdlExecutionRequest(buildCreateMaterializedViewSql(true)), false, mockHeaders(),
          mockRequest());

      assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode(),
          "Successful MV CREATE must return 201.");
      JsonNode body = readBody(response);
      assertEquals(body.get("operation").asText(), "CREATE_MATERIALIZED_VIEW");
      assertEquals(body.get("tableName").asText(), MATERIALIZED_VIEW_WITH_TYPE);
      assertEquals(body.get("tableType").asText(), "OFFLINE");
      assertNull(body.get("warnings"),
          "Successful create has no warnings; got " + body);
      // PR3.5 contract: the response surface must NOT advertise a watermark field — that
      // concept lives entirely in the scheduler now.
      assertNull(body.get("watermarkStartMs"),
          "PR3.5 removed the watermark seed path; response must omit watermarkStartMs.");

      verify(resource._pinotHelixResourceManager).addSchema(any(Schema.class), eq(false), eq(false));
      ArgumentCaptor<TableConfig> tableCaptor = ArgumentCaptor.forClass(TableConfig.class);
      verify(resource._pinotHelixResourceManager).addTable(tableCaptor.capture());

      TableTaskConfig taskConfig = tableCaptor.getValue().getTaskConfig();
      assertTrue(taskConfig.getConfigsForTaskType(MaterializedViewTask.TASK_TYPE)
              .containsKey(PinotTaskManager.SCHEDULE_KEY),
          "REFRESH EVERY must surface as a schedule key on the persisted TableConfig; got "
              + taskConfig.getConfigsForTaskType(MaterializedViewTask.TASK_TYPE));
    }
  }

  /// REFRESH-less DDL: persisted TableConfig must NOT contain a `schedule` key, so the
  /// PinotTaskManager falls back to the cluster-wide MV cron. Without this contract the
  /// compiler/router would have to invent a default cron, exactly the leaky abstraction
  /// the optional-REFRESH design is meant to avoid.
  @Test
  public void omittingRefreshClausePersistsTableWithoutSchedule()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(false);
    when(resource._pinotHelixResourceManager.getSchema(MATERIALIZED_VIEW_RAW)).thenReturn(null);

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      Response response = resource.executeDdl(
          new DdlExecutionRequest(buildCreateMaterializedViewSql(false)), false, mockHeaders(),
          mockRequest());

      assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());
      ArgumentCaptor<TableConfig> tableCaptor = ArgumentCaptor.forClass(TableConfig.class);
      verify(resource._pinotHelixResourceManager).addTable(tableCaptor.capture());

      assertFalse(tableCaptor.getValue().getTaskConfig()
              .getConfigsForTaskType(MaterializedViewTask.TASK_TYPE)
              .containsKey(PinotTaskManager.SCHEDULE_KEY),
          "Without REFRESH the persisted TableConfig must NOT contain a schedule key — "
              + "PinotTaskManager.getTaskToCronExpressionMap relies on key-absence to "
              + "fall back to the cluster-wide MV cron.");
    }
  }

  /// Dry-run must validate but never call `addTable` or `addSchema`. This regression-pins the
  /// contract that a dry-run can be safely re-issued in CI.
  @Test
  public void dryRunValidatesButNeverPersists()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(false);
    when(resource._pinotHelixResourceManager.getSchema(MATERIALIZED_VIEW_RAW)).thenReturn(null);

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      Response response = resource.executeDdl(
          new DdlExecutionRequest(buildCreateMaterializedViewSql(true)), true, mockHeaders(),
          mockRequest());

      assertEquals(response.getStatus(), Response.Status.OK.getStatusCode(),
          "Dry-run must return 200 (no resource was created).");
      JsonNode body = readBody(response);
      assertTrue(body.get("dryRun").asBoolean(),
          "Response must echo dryRun=true so the caller can distinguish from a live CREATE.");
      verify(resource._pinotHelixResourceManager, never()).addSchema(any(), any(Boolean.class), any(Boolean.class));
      verify(resource._pinotHelixResourceManager, never()).addTable(any(TableConfig.class));
    }
  }

  /// IF NOT EXISTS + table already present AS A MATERIALIZED VIEW → 200 no-op (matches
  /// SQL-standard semantics). Without this, idempotent deployment scripts break on the second
  /// pass. The preflight is type-aware: we seed an MV-shaped TableConfig (not just a hasTable
  /// stub) because the rewritten Q2=B preflight reads `getTableConfig` first and dispatches by
  /// `isMaterializedView`. The plain-table-at-same-name case is covered by the symmetric
  /// `createMvIfNotExistsOnExistingPlainOfflineTableReturns400` test below.
  @Test
  public void ifNotExistsIsNoOpWhenTableAlreadyExists()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    seedStoredMaterializedView(resource);

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      Response response = resource.executeDdl(
          new DdlExecutionRequest(
              "CREATE MATERIALIZED VIEW IF NOT EXISTS " + commonMaterializedViewBody(true)),
          false, mockHeaders(), mockRequest());

      assertEquals(response.getStatus(), Response.Status.OK.getStatusCode(),
          "IF NOT EXISTS on an existing MV must be a successful no-op (200).");
      JsonNode body = readBody(response);
      assertTrue(body.get("message").asText().toLowerCase().contains("already exists"),
          "Message must communicate idempotent no-op; got " + body.get("message"));
      verify(resource._pinotHelixResourceManager, never()).addTable(any(TableConfig.class));
    }
  }

  /// IF NOT EXISTS no-op response shape: the `tableConfig` and `schema` fields must echo
  /// what is *persisted* in ZK, not what the client submitted. Without this contract an
  /// operator running an idempotent deployment script who has since drifted the local DDL
  /// body sees their drifted body echoed back with HTTP 200 and incorrectly concludes the
  /// drift was applied — the actual persisted MV has not changed. The seeded MV is
  /// compiled from `REFRESH EVERY '1d'`; the client re-submission below uses
  /// `REFRESH EVERY '6h'`, so reading `task.taskTypeConfigsMap.MaterializedViewTask.schedule`
  /// off the response disambiguates persisted vs. client-attempted unambiguously.
  @Test
  public void ifNotExistsNoOpEchoesPersistedConfigNotClientAttempted()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    seedStoredMaterializedView(resource);

    TableConfig persisted = resource._pinotHelixResourceManager
        .getTableConfig(MATERIALIZED_VIEW_WITH_TYPE);
    String persistedCron = persisted.getTaskConfig()
        .getConfigsForTaskType(MaterializedViewTask.TASK_TYPE)
        .get(PinotTaskManager.SCHEDULE_KEY);

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      String driftedBody = "CREATE MATERIALIZED VIEW IF NOT EXISTS " + MATERIALIZED_VIEW_RAW + " ("
          + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:MILLISECONDS',"
          + "  city STRING DIMENSION,"
          + "  cnt LONG METRIC"
          + ") REFRESH EVERY '6h' "
          + "PROPERTIES ('timeColumnName' = 'ts', 'bucketTimePeriod' = '1d', 'replication' = '1') "
          + "AS SELECT ts, city, count(*) AS cnt FROM " + SOURCE + " GROUP BY ts, city";

      Response response = resource.executeDdl(new DdlExecutionRequest(driftedBody),
          false, mockHeaders(), mockRequest());

      assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode body = readBody(response);
      JsonNode echoedSchedule = body.path("tableConfig").path("task").path("taskTypeConfigsMap")
          .path(MaterializedViewTask.TASK_TYPE).path(PinotTaskManager.SCHEDULE_KEY);
      assertFalse(echoedSchedule.isMissingNode(),
          "Response.tableConfig must carry the persisted task config; got " + body.get("tableConfig"));
      assertEquals(echoedSchedule.asText(), persistedCron,
          "No-op response must echo the persisted schedule, not the client-attempted one. "
              + "Persisted=" + persistedCron + ", got=" + echoedSchedule.asText());
      // The persisted schema name must also be present, not the (potentially drifted)
      // client-attempted one. The seeder stubs `getSchema(MATERIALIZED_VIEW_RAW)` to the
      // persisted instance, so the schemaName field is a sufficient marker.
      assertEquals(body.path("schema").path("schemaName").asText(), MATERIALIZED_VIEW_RAW,
          "Response.schema must echo the persisted schema; got " + body.get("schema"));
    }
  }

  /// Without IF NOT EXISTS a duplicate CREATE MATERIALIZED VIEW against an existing MV must
  /// surface as 409 Conflict — silent overwrite would hide a real operator mistake (drift
  /// between two checked-in CREATE scripts). The plain-table-at-same-name case is a 400 (type
  /// mismatch, not idempotency) and is covered separately by
  /// `createMvWithoutIfNotExistsOnExistingPlainOfflineTableReturns400`.
  @Test
  public void duplicateCreateWithoutIfNotExistsReturns409() {
    PinotDdlRestletResource resource = newResource();
    seedStoredMaterializedView(resource);

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
          () -> resource.executeDdl(new DdlExecutionRequest(buildCreateMaterializedViewSql(true)),
              false, mockHeaders(), mockRequest()));
      assertEquals(e.getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode(),
          "Duplicate CREATE without IF NOT EXISTS must be 409.");
      assertTrue(e.getMessage().contains("already exists"), e.getMessage());
    }
  }

  /// Q2=B mirror on the CREATE side: a plain OFFLINE table at the same name as the MV must
  /// 400 even under IF NOT EXISTS. IF NOT EXISTS suppresses "object not found at the
  /// requested type", not "wrong DDL verb for the conflicting object" — the same policy that
  /// keeps `DROP MATERIALIZED VIEW IF EXISTS` from silently leaving a plain table in place.
  /// Without this guard the preflight would fall through to the legacy `hasTable` check and
  /// either silently no-op (under IF NOT EXISTS) or report a generic 409 "already exists"
  /// that does not name the actual blocker.
  @Test
  public void createMvIfNotExistsOnExistingPlainOfflineTableReturns400()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    TableConfig plainTable = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_WITH_TYPE)
        .build();
    when(resource._pinotHelixResourceManager.getTableConfig(MATERIALIZED_VIEW_WITH_TYPE))
        .thenReturn(plainTable);

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
          () -> resource.executeDdl(
              new DdlExecutionRequest(
                  "CREATE MATERIALIZED VIEW IF NOT EXISTS " + commonMaterializedViewBody(true)),
              false, mockHeaders(), mockRequest()));

      assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode(),
          "Type-mismatched target must 400 even under IF NOT EXISTS — IF NOT EXISTS handles "
              + "absence, not type confusion.");
      assertTrue(e.getMessage().contains("plain OFFLINE table"), e.getMessage());
      assertTrue(e.getMessage().contains("pick a different name")
              && e.getMessage().contains("drop the existing table"),
          "Error must surface both actionable resolutions; got: " + e.getMessage());
      verify(resource._pinotHelixResourceManager, never()).addTable(any(TableConfig.class));
    }
  }

  /// Q2=B mirror on the CREATE side, without IF NOT EXISTS: same 400 outcome, distinct from
  /// the 409 we return when the existing object is itself an MV. The two contrasting tests
  /// pin the contract that the response code maps to the kind of conflict (type mismatch =
  /// 400, same-kind duplicate = 409), not to whether IF NOT EXISTS was present.
  @Test
  public void createMvWithoutIfNotExistsOnExistingPlainOfflineTableReturns400()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    TableConfig plainTable = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_WITH_TYPE)
        .build();
    when(resource._pinotHelixResourceManager.getTableConfig(MATERIALIZED_VIEW_WITH_TYPE))
        .thenReturn(plainTable);

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
          () -> resource.executeDdl(new DdlExecutionRequest(buildCreateMaterializedViewSql(true)),
              false, mockHeaders(), mockRequest()));

      assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode(),
          "Plain-table collision is a type mismatch (400), not a same-kind duplicate (409).");
      assertTrue(e.getMessage().contains("plain OFFLINE table"), e.getMessage());
      verify(resource._pinotHelixResourceManager, never()).addTable(any(TableConfig.class));
    }
  }

  /// Raw-name exclusivity: an MV cannot squat on a name whose REALTIME half is already taken
  /// by a base table. Without this guard an MV CREATE at `foo` while `foo_REALTIME` exists
  /// would succeed, producing a hybrid pair whose OFFLINE half is a materialized view — a
  /// state with no defined semantics in the broker rewrite, minion task generator, or
  /// consistency manager. The check runs even when the OFFLINE znode is empty.
  @Test
  public void createMvWhenRealtimeTableExistsReturns400()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    String realtimeNameWithType = MATERIALIZED_VIEW_RAW + "_REALTIME";
    when(resource._pinotHelixResourceManager.getTableConfig(MATERIALIZED_VIEW_WITH_TYPE))
        .thenReturn(null);
    when(resource._pinotHelixResourceManager.hasTable(realtimeNameWithType)).thenReturn(true);

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
          () -> resource.executeDdl(new DdlExecutionRequest(buildCreateMaterializedViewSql(true)),
              false, mockHeaders(), mockRequest()));

      assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode(),
          "Raw-name collision with a REALTIME base table must 400.");
      assertTrue(e.getMessage().contains("REALTIME table"), e.getMessage());
      assertTrue(e.getMessage().contains("pick a different name")
              && e.getMessage().contains("drop the existing REALTIME table"),
          "Error must surface both actionable resolutions; got: " + e.getMessage());
      verify(resource._pinotHelixResourceManager, never()).addTable(any(TableConfig.class));
    }
  }

  /// A schema left behind from a prior failed CREATE with the SAME column shape must be
  /// reused (no addSchema attempt), and addTable must still be invoked. Without this, a
  /// retry after a partial failure would always 409 on the schema name.
  @Test
  public void preExistingMatchingSchemaIsReused()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(false);
    Schema storedSchema = buildExpectedMaterializedViewSchema();
    when(resource._pinotHelixResourceManager.getSchema(MATERIALIZED_VIEW_RAW)).thenReturn(storedSchema);

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      Response response = resource.executeDdl(
          new DdlExecutionRequest(buildCreateMaterializedViewSql(true)), false, mockHeaders(),
          mockRequest());

      assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());
      verify(resource._pinotHelixResourceManager, never())
          .addSchema(any(Schema.class), any(Boolean.class), any(Boolean.class));
      verify(resource._pinotHelixResourceManager).addTable(any(TableConfig.class));
    }
  }

  /// A schema left behind with a DIFFERENT column shape must 409 — silently mutating the
  /// stored schema would change the contract for any other reader.
  @Test
  public void preExistingMismatchedSchemaReturns409() {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(false);
    Schema mismatchedSchema = new Schema.SchemaBuilder()
        .setSchemaName(MATERIALIZED_VIEW_RAW)
        .addSingleValueDimension("unexpected_column", FieldSpec.DataType.STRING)
        .build();
    when(resource._pinotHelixResourceManager.getSchema(MATERIALIZED_VIEW_RAW))
        .thenReturn(mismatchedSchema);

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
          () -> resource.executeDdl(new DdlExecutionRequest(buildCreateMaterializedViewSql(true)),
              false, mockHeaders(), mockRequest()));
      assertEquals(e.getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode());
      assertTrue(e.getMessage().contains("Schema") && e.getMessage().contains("does not match"),
          "Mismatch error must call out the schema and the diff; got " + e.getMessage());
    }
  }

  /// Race-catch path: `addTable` throws `TableAlreadyExistsException` but the raced winner
  /// is a plain OFFLINE table (not an MV). IF NOT EXISTS must NOT silently report success —
  /// it suppresses "already exists at this type", not "wrong type collided with us". The
  /// caller should see a 409 so a follow-up retry hits the preflight (which 400s with a
  /// detailed plain-table message).
  @Test
  public void tableRaceIfNotExistsAgainstPlainOfflineReturns409()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    // Preflight returns null (no MV yet), so we enter the addTable path; addTable races and
    // throws; the race-recovery getTableConfig call sees the plain OFFLINE winner.
    TableConfig plainTable = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_WITH_TYPE)
        .build();
    when(resource._pinotHelixResourceManager.getTableConfig(MATERIALIZED_VIEW_WITH_TYPE))
        .thenReturn(null)
        .thenReturn(plainTable);
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(false);
    when(resource._pinotHelixResourceManager.getSchema(MATERIALIZED_VIEW_RAW)).thenReturn(null);
    Mockito.doThrow(new TableAlreadyExistsException("race"))
        .when(resource._pinotHelixResourceManager).addTable(any(TableConfig.class));

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
          () -> resource.executeDdl(
              new DdlExecutionRequest(
                  "CREATE MATERIALIZED VIEW IF NOT EXISTS " + commonMaterializedViewBody(true)),
              false, mockHeaders(), mockRequest()));
      assertEquals(e.getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode(),
          "IF NOT EXISTS must NOT collapse a plain-table type mismatch in the race-catch path "
              + "into a silent 200 — only same-kind (MV) duplicates are no-ops.");
    }
  }

  /// `addTable` losing a CREATE race must surface as 409 (not 500). The caller can then
  /// retry IF NOT EXISTS or accept that someone else won the race.
  @Test
  public void tableRaceWithoutIfNotExistsReturns409()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(false);
    when(resource._pinotHelixResourceManager.getSchema(MATERIALIZED_VIEW_RAW)).thenReturn(null);
    Mockito.doThrow(new TableAlreadyExistsException("race"))
        .when(resource._pinotHelixResourceManager).addTable(any(TableConfig.class));

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
          () -> resource.executeDdl(new DdlExecutionRequest(buildCreateMaterializedViewSql(true)),
              false, mockHeaders(), mockRequest()));
      assertEquals(e.getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode(),
          "TableAlreadyExistsException without IF NOT EXISTS must surface as 409.");
    }
  }

  /// A schema-create race recoverable via the retry path: the racing schema matches, so the
  /// retry should validate-then-addTable and ultimately succeed with 201. Without this, an
  /// otherwise-compatible CREATE would always lose to a concurrent winner.
  @Test
  public void schemaRaceWithMatchingRacedSchemaRecoversTo201()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(false);
    // First lookup: nothing stored (so addSchema runs and races). Second lookup (recovery
    // path): the racing writer's schema, which matches the DDL.
    when(resource._pinotHelixResourceManager.getSchema(MATERIALIZED_VIEW_RAW))
        .thenReturn(null)
        .thenReturn(buildExpectedMaterializedViewSchema());
    Mockito.doThrow(new SchemaAlreadyExistsException("racing"))
        .when(resource._pinotHelixResourceManager).addSchema(any(Schema.class), eq(false), eq(false));

    try (QuietValidationMocks ignored = new QuietValidationMocks()) {
      Response response = resource.executeDdl(
          new DdlExecutionRequest(buildCreateMaterializedViewSql(true)), false, mockHeaders(),
          mockRequest());

      assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode(),
          "Recovery path must succeed when the raced schema is compatible.");
      // Both addSchema (which threw) and the recovery's addTable must have been attempted.
      verify(resource._pinotHelixResourceManager, times(1))
          .addSchema(any(Schema.class), eq(false), eq(false));
      verify(resource._pinotHelixResourceManager, times(1)).addTable(any(TableConfig.class));
    }
  }

  // -------------------------------------------------------------------------------------------
  // SHOW CREATE MATERIALIZED VIEW
  //
  // These tests exercise the controller's `executeShowCreateMaterializedView` path plus the
  // Q2=B contract enforcement on the regular `executeShowCreate` path (an MV must always be
  // inspected via its dedicated SHOW form, never via SHOW CREATE TABLE — and the inverse).
  //
  // TableConfig + Schema fixtures are built by compiling a real `CREATE MATERIALIZED VIEW`
  // DDL. Mocking them by hand would either drift from what the compiler actually persists or
  // would silently bypass the canonical `TableConfig#isMaterializedView` flag (PR #18564) the
  // Q2=B dispatch reads, defeating the point of the Q2=B test.
  // -------------------------------------------------------------------------------------------

  @Test
  public void showCreateMaterializedViewHappyPath() {
    PinotDdlRestletResource resource = newResource();
    seedStoredMaterializedView(resource);

    Response response = resource.executeDdl(
        new DdlExecutionRequest("SHOW CREATE MATERIALIZED VIEW " + MATERIALIZED_VIEW_RAW),
        false, mockHeaders(), mockRequest());

    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    JsonNode body = readBody(response);
    assertEquals(body.get("operation").asText(), "SHOW_CREATE_MATERIALIZED_VIEW");
    assertEquals(body.get("tableName").asText(), MATERIALIZED_VIEW_WITH_TYPE);
    assertEquals(body.get("tableType").asText(), "OFFLINE",
        "SHOW CREATE MATERIALIZED VIEW must always advertise OFFLINE (MVs have no realtime form).");
    String ddl = body.get("ddl").asText();
    assertTrue(ddl.startsWith("CREATE MATERIALIZED VIEW "),
        "Emitted DDL must lead with the MV header so a copy-paste reproduces the right shape; got:\n"
            + ddl);
    assertTrue(ddl.contains("AS SELECT"),
        "MV DDL must carry the AS <query> clause; got:\n" + ddl);
  }

  /// Without a stored table the endpoint must 404 — and the message must name the MV-specific
  /// form rather than the generic "Table not found" so tooling can distinguish missing-MV
  /// from missing-table errors when both DDLs run side by side.
  @Test
  public void showCreateMaterializedViewNotFoundReturns404() {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(false);

    ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
        () -> resource.executeDdl(
            new DdlExecutionRequest("SHOW CREATE MATERIALIZED VIEW " + MATERIALIZED_VIEW_RAW),
            false, mockHeaders(), mockRequest()));

    assertEquals(e.getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    assertTrue(e.getMessage().toLowerCase().contains("materialized view"),
        "404 must name the MV-specific form so callers see which DDL endpoint missed; got: "
            + e.getMessage());
  }

  /// Q2=B mirror: handing the MV-specific form a plain OFFLINE table must 400 and point the
  /// caller at SHOW CREATE TABLE. Silently emitting `CREATE TABLE` would mislead any tooling
  /// that compares response DDL headers against the request shape.
  @Test
  public void showCreateMaterializedViewOnPlainTableReturns400() {
    PinotDdlRestletResource resource = newResource();
    TableConfig plainTable = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_WITH_TYPE)
        .build();
    Schema plainSchema = new Schema.SchemaBuilder()
        .setSchemaName(MATERIALIZED_VIEW_RAW)
        .addSingleValueDimension("id", FieldSpec.DataType.INT)
        .build();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(true);
    when(resource._pinotHelixResourceManager.getTableConfig(MATERIALIZED_VIEW_WITH_TYPE))
        .thenReturn(plainTable);
    when(resource._pinotHelixResourceManager.getTableSchema(MATERIALIZED_VIEW_WITH_TYPE))
        .thenReturn(plainSchema);

    ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
        () -> resource.executeDdl(
            new DdlExecutionRequest("SHOW CREATE MATERIALIZED VIEW " + MATERIALIZED_VIEW_RAW),
            false, mockHeaders(), mockRequest()));

    assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    assertTrue(e.getMessage().contains("not a materialized view"), e.getMessage());
    assertTrue(e.getMessage().contains("SHOW CREATE TABLE"),
        "Error must redirect the caller to the correct DDL form; got: " + e.getMessage());
  }

  /// Q2=B primary: handing the regular SHOW CREATE TABLE form an MV table must 400 and point
  /// the caller at SHOW CREATE MATERIALIZED VIEW. This is the bug the contract closes —
  /// without the check, the emitter's dispatch would happily render `CREATE MATERIALIZED VIEW`
  /// even though the caller asked for `SHOW CREATE TABLE`, and the response DDL header would
  /// no longer match the request shape.
  @Test
  public void showCreateTableOnMaterializedViewReturns400() {
    PinotDdlRestletResource resource = newResource();
    seedStoredMaterializedView(resource);

    ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
        () -> resource.executeDdl(
            new DdlExecutionRequest(
                "SHOW CREATE TABLE " + MATERIALIZED_VIEW_RAW + " TYPE OFFLINE"),
            false, mockHeaders(), mockRequest()));

    assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    assertTrue(e.getMessage().contains("materialized view"),
        "Error must call out that the table is an MV; got: " + e.getMessage());
    assertTrue(e.getMessage().contains("SHOW CREATE MATERIALIZED VIEW"),
        "Error must redirect the caller to the MV-specific form; got: " + e.getMessage());
  }

  // NOTE: The "corrupted MV" preflight (MaterializedViewTask block present but definedSQL
  // missing) is no longer reachable for any persisted config. PR #18564 added the SPI invariant
  // `TableConfigUtils.validateMaterializedViewInvariants` which rejects this shape at addTable /
  // updateTable time. Identity is now decided by the canonical `TableConfig#isMaterializedView`
  // flag, so anything that fails the SHOW CREATE MATERIALIZED VIEW dispatch is just a plain
  // table — exercised by `showCreateMaterializedViewOnPlainTableReturns400` above.

  // -------------------------------------------------------------------------------------------
  // DROP MATERIALIZED VIEW
  //
  // These tests exercise `executeDropMaterializedView` plus the Q2=B contract enforcement on
  // the regular `executeDrop` path. The underlying `PinotHelixResourceManager#deleteTable`
  // call is the same code the legacy `DELETE /materializedViews/{name}` REST endpoint uses,
  // so znode cleanup is verified at that layer; here we pin the controller's own contract
  // (dispatch, authorization, IF EXISTS, Q2=B partitioning, active-task guard).
  // -------------------------------------------------------------------------------------------

  /// Happy path: real MV → 200, `deleteTable(rawName, OFFLINE, null)` invoked exactly once.
  /// We don't double-assert znode cleanup here because that lives inside `deleteTable`; this
  /// test instead pins the wiring from `executeDropMaterializedView` to the underlying call
  /// (the same call the legacy DELETE endpoint makes).
  @Test
  public void dropMaterializedViewHappyPath()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    seedStoredMaterializedView(resource);
    quietActiveTaskScan(resource);

    try (MockedStatic<ZKMetadataProvider> ignored = stubNoLogicalTableConfigs()) {
      Response response = resource.executeDdl(
          new DdlExecutionRequest("DROP MATERIALIZED VIEW " + MATERIALIZED_VIEW_RAW),
          false, mockHeaders(), mockRequest());

      assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode body = readBody(response);
      assertEquals(body.get("operation").asText(), "DROP_MATERIALIZED_VIEW");
      assertEquals(body.get("tableName").asText(), MATERIALIZED_VIEW_WITH_TYPE);
      assertEquals(body.get("tableType").asText(), "OFFLINE",
          "DROP MATERIALIZED VIEW must always advertise OFFLINE.");
      // Delegate target: deleteTable(rawName, OFFLINE, null). The raw name (not the typed one)
      // is required so PinotHelixResourceManager.deleteTable re-derives the typed form via
      // TableNameBuilder; we explicitly check that signature so a refactor to pass the typed
      // name fails the test.
      verify(resource._pinotHelixResourceManager, times(1))
          .deleteTable(MATERIALIZED_VIEW_RAW, TableType.OFFLINE, null);
    }
  }

  /// Missing MV without IF EXISTS → 404, no delete call.
  @Test
  public void dropMaterializedViewMissingWithoutIfExistsReturns404()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(false);
    when(resource._pinotHelixResourceManager.getTableConfig(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(null);

    ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
        () -> resource.executeDdl(
            new DdlExecutionRequest("DROP MATERIALIZED VIEW " + MATERIALIZED_VIEW_RAW),
            false, mockHeaders(), mockRequest()));

    assertEquals(e.getResponse().getStatus(), Response.Status.NOT_FOUND.getStatusCode());
    assertTrue(e.getMessage().toLowerCase().contains("materialized view"),
        "404 must name the MV-specific form so callers see which DDL endpoint missed; got: "
            + e.getMessage());
    verify(resource._pinotHelixResourceManager, never())
        .deleteTable(anyString(), any(TableType.class), any());
  }

  /// Missing MV WITH IF EXISTS → 200 no-op, no delete call.
  @Test
  public void dropMaterializedViewMissingWithIfExistsIsNoOp()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(false);
    when(resource._pinotHelixResourceManager.getTableConfig(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(null);

    Response response = resource.executeDdl(
        new DdlExecutionRequest("DROP MATERIALIZED VIEW IF EXISTS " + MATERIALIZED_VIEW_RAW),
        false, mockHeaders(), mockRequest());

    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode(),
        "IF EXISTS on a missing MV must be a successful no-op (200) — matches the SQL-standard "
            + "DROP TABLE IF EXISTS contract.");
    JsonNode body = readBody(response);
    assertTrue(body.get("ifExists").asBoolean(),
        "Response must echo ifExists=true so the caller can distinguish from a real delete.");
    assertTrue(body.get("message").asText().toLowerCase().contains("if exists satisfied"),
        "Message must communicate idempotent no-op; got: " + body.get("message"));
    verify(resource._pinotHelixResourceManager, never())
        .deleteTable(anyString(), any(TableType.class), any());
  }

  /// Q2=B mirror: DROP MATERIALIZED VIEW handed a plain OFFLINE table must 400 — even under
  /// IF EXISTS. The opposite policy (silently treat a type-mismatched target as "absent")
  /// would let `DROP MATERIALIZED VIEW IF EXISTS foo` silently leave a real OFFLINE table
  /// named foo in place, surprising any operator whose script assumed the IF EXISTS clause
  /// converted "wrong kind of object" into "already gone". 400 forces them to inspect.
  @Test
  public void dropMaterializedViewOnPlainTableReturns400EvenUnderIfExists() {
    PinotDdlRestletResource resource = newResource();
    TableConfig plainTable = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MATERIALIZED_VIEW_WITH_TYPE)
        .build();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(true);
    when(resource._pinotHelixResourceManager.getTableConfig(MATERIALIZED_VIEW_WITH_TYPE))
        .thenReturn(plainTable);

    ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
        () -> resource.executeDdl(
            new DdlExecutionRequest(
                "DROP MATERIALIZED VIEW IF EXISTS " + MATERIALIZED_VIEW_RAW),
            false, mockHeaders(), mockRequest()));

    assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode(),
        "Type-mismatched target must 400 even under IF EXISTS — IF EXISTS handles absence, "
            + "not type confusion.");
    assertTrue(e.getMessage().contains("not a materialized view"), e.getMessage());
    assertTrue(e.getMessage().contains("DROP TABLE"),
        "Error must redirect to the correct DROP form; got: " + e.getMessage());
  }

  /// Q2=B primary: DROP TABLE on an MV table must 400 with a pointer at DROP MATERIALIZED
  /// VIEW. This closes the symmetry gap with SHOW CREATE TABLE / SHOW CREATE MATERIALIZED
  /// VIEW — all three DDL verbs (CREATE/DROP/SHOW CREATE) now strictly partition table vs
  /// MV at the REST boundary.
  @Test
  public void dropTableOnMaterializedViewReturns400() {
    PinotDdlRestletResource resource = newResource();
    seedStoredMaterializedView(resource);

    ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
        () -> resource.executeDdl(
            new DdlExecutionRequest("DROP TABLE " + MATERIALIZED_VIEW_RAW + " TYPE OFFLINE"),
            false, mockHeaders(), mockRequest()));

    assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    assertTrue(e.getMessage().contains("materialized view"),
        "Error must call out that the table is an MV; got: " + e.getMessage());
    assertTrue(e.getMessage().contains("DROP MATERIALIZED VIEW"),
        "Error must redirect to the MV-specific DROP form; got: " + e.getMessage());
  }

  /// Dry-run must validate (existence + Q2=B + active-task scan) but never call deleteTable.
  /// This pins the contract that an MV DROP dry-run can be safely re-issued in CI.
  @Test
  public void dropMaterializedViewDryRunNeverDeletes()
      throws Exception {
    PinotDdlRestletResource resource = newResource();
    seedStoredMaterializedView(resource);
    quietActiveTaskScan(resource);

    try (MockedStatic<ZKMetadataProvider> ignored = stubNoLogicalTableConfigs()) {
      Response response = resource.executeDdl(
          new DdlExecutionRequest("DROP MATERIALIZED VIEW " + MATERIALIZED_VIEW_RAW),
          true, mockHeaders(), mockRequest());

      assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
      JsonNode body = readBody(response);
      assertTrue(body.get("dryRun").asBoolean(),
          "Response must echo dryRun=true so the caller can distinguish from a live DROP.");
      assertTrue(body.get("message").asText().toLowerCase().startsWith("dry run"),
          "Dry-run message must lead with 'Dry run'; got: " + body.get("message"));
      verify(resource._pinotHelixResourceManager, never())
          .deleteTable(anyString(), any(TableType.class), any());
    }
  }

  /// Active running MV task → 400 with the pending-task list. Without this, an in-flight
  /// `MaterializedViewTask` would race the drop and orphan the partial output. Mirrors the
  /// same guard that DROP TABLE applies (and is enforced via the shared
  /// `assertNoActiveTasksBeforeDrop` helper, which makes this test the regression pin for
  /// the helper being invoked on the DROP MV path at all).
  @Test
  public void dropMaterializedViewBlockedByActiveTaskReturns400() {
    PinotDdlRestletResource resource = newResource();
    seedStoredMaterializedView(resource);

    PinotHelixTaskResourceManager.TaskCount runningCount =
        mock(PinotHelixTaskResourceManager.TaskCount.class);
    when(runningCount.getRunning()).thenReturn(1);
    when(resource._pinotHelixTaskResourceManager.getTaskStatesByTable(
        eq(MaterializedViewTask.TASK_TYPE), eq(MATERIALIZED_VIEW_WITH_TYPE)))
        .thenReturn(Map.of("Task_MaterializedViewTask_inflight", TaskState.IN_PROGRESS));
    when(resource._pinotHelixTaskResourceManager.getTaskCount("Task_MaterializedViewTask_inflight"))
        .thenReturn(runningCount);

    try (MockedStatic<ZKMetadataProvider> ignored = stubNoLogicalTableConfigs()) {
      ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
          () -> resource.executeDdl(
              new DdlExecutionRequest("DROP MATERIALIZED VIEW " + MATERIALIZED_VIEW_RAW),
              false, mockHeaders(), mockRequest()));

      assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      assertTrue(e.getMessage().contains("active running tasks"),
          "Error must call out the active-task block; got: " + e.getMessage());
      assertTrue(e.getMessage().contains("Task_MaterializedViewTask_inflight"),
          "Error must name the offending task so the operator can find it; got: "
              + e.getMessage());
    }
  }

  // -------------------------------------------------------------------------------------------
  // SHOW MATERIALIZED VIEWS
  //
  // The controller dispatches SHOW MATERIALIZED VIEWS to
  // `_pinotHelixResourceManager.getAllRawMaterializedViewNames(scopedDatabase)` and returns
  // the result on the shared `tableNames` field with the SHOW_MATERIALIZED_VIEWS operation
  // discriminator. The helper itself is exercised by
  // `PinotHelixResourceManagerMaterializedViewListingTest` — these tests pin the controller
  // contract (operation, scoping, message, auth) without re-asserting the filter logic.
  // -------------------------------------------------------------------------------------------

  @Test
  public void showMaterializedViewsHappyPath() {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.getAllRawMaterializedViewNames("default"))
        .thenReturn(java.util.Arrays.asList("mv_orders", "mv_clicks"));

    Response response = resource.executeDdl(new DdlExecutionRequest("SHOW MATERIALIZED VIEWS"),
        false, mockHeaders(), mockRequest());

    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    JsonNode body = readBody(response);
    assertEquals(body.get("operation").asText(), "SHOW_MATERIALIZED_VIEWS");
    // The scoped database must default to DEFAULT_DATABASE when neither SQL FROM nor the
    // Database header supplies one. Asserting on the wire field rather than the constant
    // because the field is what tooling actually reads.
    assertEquals(body.get("databaseName").asText(), "default");
    JsonNode names = body.get("tableNames");
    assertEquals(names.size(), 2);
    assertEquals(names.get(0).asText(), "mv_orders");
    assertEquals(names.get(1).asText(), "mv_clicks");
    // The message must use the MV-specific wording so an operator reading the response in
    // logs can tell SHOW MATERIALIZED VIEWS apart from SHOW TABLES at a glance.
    assertEquals(body.get("message").asText(), "Found 2 materialized view(s).");
  }

  @Test
  public void showMaterializedViewsEmptyCluster() {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.getAllRawMaterializedViewNames("default"))
        .thenReturn(Collections.emptyList());

    Response response = resource.executeDdl(new DdlExecutionRequest("SHOW MATERIALIZED VIEWS"),
        false, mockHeaders(), mockRequest());

    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    JsonNode body = readBody(response);
    assertEquals(body.get("operation").asText(), "SHOW_MATERIALIZED_VIEWS");
    // The wire field is present even when empty so clients can iterate without a null check.
    // Asserting `size()==0` rather than the field's absence pins this Jackson behaviour.
    assertEquals(body.get("tableNames").size(), 0);
    assertEquals(body.get("message").asText(), "Found 0 materialized view(s).");
  }

  /// `SHOW MATERIALIZED VIEWS FROM analytics` must propagate the explicit database into the
  /// helper call. Without this scoping the listing would silently default to the Database
  /// header (or DEFAULT_DATABASE) and leak MV names from the wrong database.
  @Test
  public void showMaterializedViewsExplicitFromDatabaseScopesListing() {
    PinotDdlRestletResource resource = newResource();
    when(resource._pinotHelixResourceManager.getAllRawMaterializedViewNames("analytics"))
        .thenReturn(java.util.Arrays.asList("mv_revenue"));

    Response response = resource.executeDdl(
        new DdlExecutionRequest("SHOW MATERIALIZED VIEWS FROM analytics"),
        false, mockHeaders(), mockRequest());

    assertEquals(response.getStatus(), Response.Status.OK.getStatusCode());
    JsonNode body = readBody(response);
    assertEquals(body.get("databaseName").asText(), "analytics");
    assertEquals(body.get("tableNames").get(0).asText(), "mv_revenue");
    verify(resource._pinotHelixResourceManager).getAllRawMaterializedViewNames("analytics");
    // Sanity: the default-database listing must NOT have been called — otherwise an explicit
    // FROM clause would still leak names from the caller's header-scoped database.
    verify(resource._pinotHelixResourceManager, never()).getAllRawMaterializedViewNames("default");
  }

  /// A custom AccessControl that denies the cluster-level GET_TABLE check must surface as a
  /// 403. The MV listing must use the SAME action pair (CLUSTER, GET_TABLE) as SHOW TABLES
  /// and the existing GET /materializedViews REST endpoint — using a different action would
  /// fork the listing auth surface for what is the same underlying read.
  @Test
  public void showMaterializedViewsPermissionDeniedReturns403() {
    PinotDdlRestletResource resource = newResource();
    when(resource._accessControlFactory.create()).thenReturn(new AccessControl() {
      @Override
      public boolean hasAccess(javax.ws.rs.core.HttpHeaders headers,
          org.apache.pinot.core.auth.TargetType targetType, String targetId, String action) {
        return false;
      }
    });

    ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
        () -> resource.executeDdl(new DdlExecutionRequest("SHOW MATERIALIZED VIEWS"),
            false, mockHeaders(), mockRequest()));

    assertEquals(e.getResponse().getStatus(), Response.Status.FORBIDDEN.getStatusCode());
    // The listing must NOT have been invoked when auth fails — otherwise we leak MV names
    // through the error path or accidentally hit ZK on every denied request.
    verify(resource._pinotHelixResourceManager, never()).getAllRawMaterializedViewNames(anyString());
  }

  // -------------------------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------------------------

  /// Builds a `CREATE MATERIALIZED VIEW` SQL string. When `withRefresh=true` the DDL emits
  /// `REFRESH EVERY '1d'`; when false the DDL omits the entire REFRESH clause so the test
  /// can pin the cluster-cron-fallback contract end to end.
  private static String buildCreateMaterializedViewSql(boolean withRefresh) {
    return "CREATE MATERIALIZED VIEW " + commonMaterializedViewBody(withRefresh);
  }

  /// Common MV CREATE body shared by the `CREATE MATERIALIZED VIEW` and `... IF NOT EXISTS`
  /// variants. Crafted to satisfy the compiler's MV consistency checks (timeColumnName is a
  /// DATETIME column, bucketTimePeriod is provided).
  ///
  /// The AS clause intentionally omits `LIMIT N`: the compiler's `extractDefinedSql` slices
  /// `originalSql` using `queryNode.getParserPosition()`, which Calcite reports as the
  /// LIMIT-only span when LIMIT is present at the top level; that quirk is a separate
  /// compiler-side bug to address later. These tests rely on the analyzer's
  /// auto-LIMIT-injection path.
  private static String commonMaterializedViewBody(boolean withRefresh) {
    String refreshClause = withRefresh ? "REFRESH EVERY '1d' " : "";
    return MATERIALIZED_VIEW_RAW + " ("
        + "  ts TIMESTAMP DATETIME FORMAT '1:MILLISECONDS:TIMESTAMP' GRANULARITY '1:MILLISECONDS',"
        + "  city STRING DIMENSION,"
        + "  cnt LONG METRIC"
        + ") "
        + refreshClause
        + "PROPERTIES ("
        + "  'timeColumnName' = 'ts',"
        + "  'bucketTimePeriod' = '1d',"
        + "  'replication' = '1'"
        + ") "
        + "AS SELECT ts, city, count(*) AS cnt FROM " + SOURCE + " GROUP BY ts, city";
  }

  /// Builds the schema the compiler will produce for [#commonMaterializedViewBody]. Used by
  /// the pre-existing-matching-schema and schema-race-recovery tests where the test mocks a
  /// "stored" schema that must be byte-for-byte compatible with the DDL-compiled one.
  private static Schema buildExpectedMaterializedViewSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(MATERIALIZED_VIEW_RAW)
        .addDateTime("ts", FieldSpec.DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS")
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .build();
  }

  /// Compiles a real `CREATE MATERIALIZED VIEW` and wires the resulting Schema + TableConfig
  /// (with the canonical `TableConfig#isMaterializedView` flag set, plus the
  /// `task.MaterializedViewTask.definedSQL` body the emitter renders) into the resource's
  /// mocked Helix manager. Mocking the identity directly via Mockito would be equivalent to
  /// asserting the contract on itself; compiling a real DDL keeps these tests honest if a
  /// future refactor changes how the compiler stamps a config as an MV.
  private static void seedStoredMaterializedView(PinotDdlRestletResource resource) {
    CompiledCreateMaterializedView compiled = (CompiledCreateMaterializedView) DdlCompiler.compile(
        "CREATE MATERIALIZED VIEW " + commonMaterializedViewBody(true));
    TableConfig mvConfig = compiled.getTableConfig();
    // The compiler stores the raw name; the Helix layer always reads/writes the typed name.
    // Rename in place so getTableConfig(...) and the controller's TableNameBuilder agree.
    mvConfig.setTableName(MATERIALIZED_VIEW_WITH_TYPE);
    Schema mvSchema = compiled.getSchema();
    when(resource._pinotHelixResourceManager.hasTable(MATERIALIZED_VIEW_WITH_TYPE)).thenReturn(true);
    when(resource._pinotHelixResourceManager.getTableConfig(MATERIALIZED_VIEW_WITH_TYPE))
        .thenReturn(mvConfig);
    when(resource._pinotHelixResourceManager.getTableSchema(MATERIALIZED_VIEW_WITH_TYPE))
        .thenReturn(mvSchema);
    // CREATE MV IF NOT EXISTS no-op path: the controller re-reads the persisted schema via
    // `getSchema(rawName)` to echo the actual (not client-attempted) state back. Without
    // this stub the helper would see null and silently leave the response.schema field as
    // the client-attempted body, defeating the "echo persisted" contract under test.
    when(resource._pinotHelixResourceManager.getSchema(MATERIALIZED_VIEW_RAW))
        .thenReturn(mvSchema);
  }

  /// Stubs the active-task scan to return no tasks for every task type. This is the default
  /// shape both `assertNoActiveTasksBeforeDrop` and `cleanupTableTasksBeforeDrop` expect; a
  /// raw mock would return `null` and NPE inside the helpers, masking the contract under
  /// test. Tests that want to exercise the active-task short-circuit override the relevant
  /// stub on a per-test basis after seeding (Mockito's last-stubbing-wins semantics).
  private static void quietActiveTaskScan(PinotDdlRestletResource resource) {
    when(resource._pinotHelixTaskResourceManager.getTaskStatesByTable(anyString(), anyString()))
        .thenReturn(Collections.emptyMap());
  }

  /// Returns an open [MockedStatic] that stubs
  /// [ZKMetadataProvider#getAllLogicalTableConfigs] to an empty list. `assertNoLogicalTable\
  /// References` calls into ZKMetadataProvider directly with the property store from
  /// `PinotHelixResourceManager#getPropertyStore`; in a unit test the property store is null
  /// and the call NPEs. Static-mocking the lookup is cheaper than wiring a real `ZkClient`
  /// stack and keeps these tests focused on controller logic. Callers wrap in
  /// try-with-resources to scope the override to a single test.
  private static MockedStatic<ZKMetadataProvider> stubNoLogicalTableConfigs() {
    MockedStatic<ZKMetadataProvider> stub = Mockito.mockStatic(ZKMetadataProvider.class);
    stub.when(() -> ZKMetadataProvider.getAllLogicalTableConfigs(any()))
        .thenReturn(Collections.emptyList());
    return stub;
  }

  /// Constructs a [PinotDdlRestletResource] with a permissive access control plugin and
  /// stub-only dependencies. Tests then layer their own behaviour onto the per-field mocks.
  private static PinotDdlRestletResource newResource() {
    PinotDdlRestletResource resource = new PinotDdlRestletResource();
    resource._pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    resource._pinotHelixTaskResourceManager = mock(PinotHelixTaskResourceManager.class);
    resource._pinotTaskManager = mock(PinotTaskManager.class);
    resource._controllerConf = mock(ControllerConf.class);
    resource._accessControlFactory = mock(AccessControlFactory.class);
    when(resource._accessControlFactory.create()).thenReturn(new AccessControl() {
    });
    return resource;
  }

  /// Builds an HttpHeaders mock that does not advertise a Database header — DDL paths
  /// then take the `database = null` (default) branch and skip translation conflicts.
  private static HttpHeaders mockHeaders() {
    return mock(HttpHeaders.class);
  }

  private static Request mockRequest() {
    Request request = mock(Request.class);
    when(request.getRequestURL()).thenReturn(new StringBuilder("http://localhost/sql/ddl"));
    return request;
  }

  private static JsonNode readBody(Response response) {
    Object entity = response.getEntity();
    assertTrue(entity instanceof org.apache.pinot.controller.api.resources.ddl.DdlExecutionResponse,
        "Expected a DdlExecutionResponse entity; got " + (entity == null ? "null" : entity.getClass()));
    try {
      return org.apache.pinot.spi.utils.JsonUtils.objectToJsonNode(entity);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /// Holds the three [MockedStatic] handles that quiet the controller's validation chain so
  /// the executor logic can be tested without `MaterializedViewTaskGenerator` on the
  /// classpath. Implements [AutoCloseable] so callers can use try-with-resources; close()
  /// closes each child mock individually and aggregates any failures so a misbehaving Mockito
  /// session never silently leaks into a sibling test.
  private static final class QuietValidationMocks implements AutoCloseable {
    private final MockedStatic<TableConfigValidationUtils> _validation;
    private final MockedStatic<PinotTableRestletResource> _tableResource;
    private final MockedStatic<org.apache.pinot.controller.tuner.TableConfigTunerUtils> _tuner;

    private QuietValidationMocks() {
      MockedStatic<TableConfigValidationUtils> validation = null;
      MockedStatic<PinotTableRestletResource> tableResource = null;
      MockedStatic<org.apache.pinot.controller.tuner.TableConfigTunerUtils> tuner = null;
      try {
        validation = Mockito.mockStatic(TableConfigValidationUtils.class);
        validation.when(() -> TableConfigValidationUtils.validateTableConfig(
            any(), any(), any(), any(), any(), any())).then(invocation -> null);

        tableResource = Mockito.mockStatic(PinotTableRestletResource.class);
        tableResource.when(() -> PinotTableRestletResource.tableTasksValidation(any(), any()))
            .then(invocation -> null);

        tuner = Mockito.mockStatic(org.apache.pinot.controller.tuner.TableConfigTunerUtils.class);
        tuner.when(() -> org.apache.pinot.controller.tuner.TableConfigTunerUtils.applyTunerConfigs(
            any(), any(), any(), any())).then(invocation -> null);

        _validation = validation;
        _tableResource = tableResource;
        _tuner = tuner;
        // Transfer ownership to fields — the catch block must not close them now.
        validation = null;
        tableResource = null;
        tuner = null;
      } catch (RuntimeException | Error e) {
        // Half-built mock session: close anything successfully opened so a follow-up test
        // does not blow up trying to mock the same static class a second time.
        closeQuietly(validation, e);
        closeQuietly(tableResource, e);
        closeQuietly(tuner, e);
        throw e;
      }
    }

    @Override
    public void close() {
      RuntimeException agg = null;
      agg = closeChained(_validation, agg);
      agg = closeChained(_tableResource, agg);
      agg = closeChained(_tuner, agg);
      if (agg != null) {
        throw agg;
      }
    }

    private static RuntimeException closeChained(MockedStatic<?> mock, RuntimeException carry) {
      try {
        mock.close();
        return carry;
      } catch (RuntimeException e) {
        if (carry == null) {
          return e;
        }
        carry.addSuppressed(e);
        return carry;
      }
    }

    private static void closeQuietly(MockedStatic<?> mock, Throwable carry) {
      if (mock == null) {
        return;
      }
      try {
        mock.close();
      } catch (RuntimeException e) {
        carry.addSuppressed(e);
      }
    }
  }
}
