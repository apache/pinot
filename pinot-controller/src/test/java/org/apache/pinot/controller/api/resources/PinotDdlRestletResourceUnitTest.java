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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.helix.task.TaskState;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.controller.api.access.AccessControl;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.api.resources.ddl.DdlExecutionRequest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.LogicalTableConfig;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.PhysicalTableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.glassfish.grizzly.http.server.Request;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Unit tests for [PinotDdlRestletResource#describeColumnShapeMismatch]. This is the
/// hybrid-table CREATE gate that decides whether a DDL-compiled schema is compatible with the
/// schema already stored in ZK for a hybrid pair's sibling variant. The comparator must accept
/// differences in schema-level metadata a DDL column list cannot express (primary keys, tags,
/// null-handling) and reject differences in per-column attributes that the DDL does control.
public class PinotDdlRestletResourceUnitTest {

  /// A dry-run DROP must fail on the same non-mutating logical-table reference guard as a live
  /// DROP. Otherwise dry-run can promise a deletion that the real request will later reject.
  @Test
  public void dropDryRunRejectsLogicalTableReferences() {
    PinotDdlRestletResource resource = new PinotDdlRestletResource();
    resource._pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    resource._accessControlFactory = mock(AccessControlFactory.class);
    when(resource._accessControlFactory.create()).thenReturn(new AccessControl() {
    });
    when(resource._pinotHelixResourceManager.hasTable("events_OFFLINE")).thenReturn(true);

    LogicalTableConfig logicalTableConfig = new LogicalTableConfig();
    logicalTableConfig.setTableName("logical_events");
    logicalTableConfig.setPhysicalTableConfigMap(
        Collections.singletonMap("events_OFFLINE", new PhysicalTableConfig()));

    Request request = mock(Request.class);
    when(request.getRequestURL()).thenReturn(new StringBuilder("http://localhost/sql/ddl"));

    try (MockedStatic<ZKMetadataProvider> metadataProvider = Mockito.mockStatic(ZKMetadataProvider.class)) {
      metadataProvider.when(() -> ZKMetadataProvider.getAllLogicalTableConfigs(Mockito.any()))
          .thenReturn(Collections.singletonList(logicalTableConfig));

      ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
          () -> resource.executeDdl(new DdlExecutionRequest("DROP TABLE events TYPE OFFLINE"), true,
              mock(HttpHeaders.class), request));
      assertEquals(e.getResponse().getStatus(), Response.Status.CONFLICT.getStatusCode());
      assertTrue(e.getMessage().contains("logical_events"), e.getMessage());
    }
  }

  /// If an active task appears after DROP's initial preflight, DDL must fail without first
  /// clearing the table task schedule. This guards the race that the shared JSON delete helper
  /// cannot prevent because it removes schedules before checking for active tasks.
  @Test
  public void dropDoesNotClearTaskSchedulesWhenTaskAppearsAfterPreflight()
      throws Exception {
    PinotDdlRestletResource resource = new PinotDdlRestletResource();
    resource._pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
    resource._pinotHelixTaskResourceManager = mock(PinotHelixTaskResourceManager.class);
    resource._accessControlFactory = mock(AccessControlFactory.class);
    when(resource._accessControlFactory.create()).thenReturn(new AccessControl() {
    });
    when(resource._pinotHelixResourceManager.hasTable("events_OFFLINE")).thenReturn(true);

    Map<String, Map<String, String>> taskConfigs = new HashMap<>();
    Map<String, String> refreshTaskConfig = new HashMap<>();
    refreshTaskConfig.put(PinotTaskManager.SCHEDULE_KEY, "0 0 * * * ?");
    taskConfigs.put("SegmentRefreshTask", refreshTaskConfig);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("events")
        .setTaskConfig(new TableTaskConfig(taskConfigs))
        .build();
    when(resource._pinotHelixResourceManager.getTableConfig("events_OFFLINE")).thenReturn(tableConfig);

    PinotHelixTaskResourceManager.TaskCount taskCount = mock(PinotHelixTaskResourceManager.TaskCount.class);
    when(taskCount.getRunning()).thenReturn(1);
    when(resource._pinotHelixTaskResourceManager.getTaskCount("task_0")).thenReturn(taskCount);
    when(resource._pinotHelixTaskResourceManager.getTaskStatesByTable("SegmentRefreshTask", "events_OFFLINE"))
        .thenReturn(Collections.emptyMap())
        .thenReturn(Collections.singletonMap("task_0", TaskState.IN_PROGRESS));

    Request request = mock(Request.class);
    when(request.getRequestURL()).thenReturn(new StringBuilder("http://localhost/sql/ddl"));

    try (MockedStatic<ZKMetadataProvider> metadataProvider = Mockito.mockStatic(ZKMetadataProvider.class)) {
      metadataProvider.when(() -> ZKMetadataProvider.getAllLogicalTableConfigs(Mockito.any()))
          .thenReturn(Collections.emptyList());

      ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
          () -> resource.executeDdl(new DdlExecutionRequest("DROP TABLE events TYPE OFFLINE"), false,
              mock(HttpHeaders.class), request));
      assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
      assertTrue(e.getMessage().contains("active running tasks"), e.getMessage());
    }

    verify(resource._pinotHelixResourceManager, never()).updateTableConfig(tableConfig);
    verify(resource._pinotHelixResourceManager, never()).deleteTable("events", TableType.OFFLINE, null);
    assertEquals(tableConfig.getTaskConfig().getTaskTypeConfigsMap()
        .get("SegmentRefreshTask").get(PinotTaskManager.SCHEDULE_KEY), "0 0 * * * ?");
  }

  /// SQL database qualifiers and the Database header must agree; conflicts are caller input
  /// errors, not controller failures.
  @Test
  public void conflictingSqlAndHeaderDatabaseReturnsBadRequest() {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(CommonConstants.DATABASE)).thenReturn("db2");

    assertDatabaseConflictReturnsBadRequest(
        "CREATE TABLE db1.events (id INT) TABLE_TYPE = OFFLINE", headers);
    assertDatabaseConflictReturnsBadRequest("DROP TABLE db1.events TYPE OFFLINE", headers);
    assertDatabaseConflictReturnsBadRequest("SHOW CREATE TABLE db1.events TYPE OFFLINE", headers);
    assertDatabaseConflictReturnsBadRequest("SHOW TABLES FROM db1", headers);
  }

  private static void assertDatabaseConflictReturnsBadRequest(String sql, HttpHeaders headers) {
    ControllerApplicationException e = expectThrows(ControllerApplicationException.class,
        () -> new PinotDdlRestletResource().executeDdl(new DdlExecutionRequest(sql), true, headers,
            mock(Request.class)));
    assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    assertTrue(e.getMessage().contains("does not match"), e.getMessage());
  }

  /// Compiled DDL with matching columns but no primary keys must accept a stored schema that has them.
  @Test
  public void acceptsMatchingColumnsWhenStoredHasExtraPrimaryKeyMetadata() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("id", DataType.INT, true));
    stored.setPrimaryKeyColumns(Collections.singletonList("id"));
    stored.setEnableColumnBasedNullHandling(true);

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.INT, true));

    assertNull(PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled),
        "schema-level metadata the DDL column list cannot express must not drive a mismatch");
  }

  /// Compiled DDL with an explicit matching PRIMARY KEY must accept a stored schema with the same
  /// primary-key contract.
  @Test
  public void acceptsExplicitPrimaryKeyWhenStoredSchemaMatches() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("id", DataType.INT, true));
    stored.setPrimaryKeyColumns(Collections.singletonList("id"));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.INT, true));
    compiled.setPrimaryKeyColumns(Collections.singletonList("id"));

    assertNull(PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled));
  }

  /// If the DDL explicitly supplies PRIMARY KEY, reusing a stored schema with a different key would
  /// silently persist the old key while the response advertises the new one. Reject that mismatch.
  @Test
  public void rejectsExplicitPrimaryKeyMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("id", DataType.INT, true));
    stored.addField(new DimensionFieldSpec("other", DataType.INT, true));
    stored.setPrimaryKeyColumns(Collections.singletonList("id"));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.INT, true));
    compiled.addField(new DimensionFieldSpec("other", DataType.INT, true));
    compiled.setPrimaryKeyColumns(Collections.singletonList("other"));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("PRIMARY KEY"), msg);
  }

  /// A missing or extra column must be rejected with a named column set in the message.
  @Test
  public void rejectsColumnSetDifference() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("id", DataType.INT, true));
    stored.addField(new DimensionFieldSpec("name", DataType.STRING, true));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.INT, true));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("column sets differ") && msg.contains("name"),
        "message should call out the offending column set difference: " + msg);
  }

  /// Different data type for the same column must be rejected.
  @Test
  public void rejectsDataTypeMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("id", DataType.INT, true));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.LONG, true));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("data type differs"), msg);
  }

  /// DIMENSION vs METRIC for the same column must be rejected.
  @Test
  public void rejectsFieldTypeMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("v", DataType.LONG, true));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new MetricFieldSpec("v", DataType.LONG));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("field type differs"), msg);
  }

  /// Single-value vs multi-value mismatch must be rejected.
  @Test
  public void rejectsSingleValuedFlagMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("tags", DataType.STRING, true));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("tags", DataType.STRING, false));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("single-valued flag"), msg);
  }

  /// NOT NULL flag mismatch must be rejected.
  @Test
  public void rejectsNotNullFlagMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    FieldSpec storedSpec = new DimensionFieldSpec("id", DataType.INT, true);
    storedSpec.setNotNull(true);
    stored.addField(storedSpec);

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.INT, true));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("NOT NULL flag"), msg);
  }

  /// BYTES columns produce a fresh byte[] on each getDefaultNullValue() call. Equality must be
  /// by content, not reference, otherwise every hybrid second-variant CREATE on a BYTES schema
  /// with a custom default would falsely trip the column-shape mismatch.
  @Test
  public void acceptsMatchingBytesDefaultNullValue() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    FieldSpec storedSpec = new DimensionFieldSpec("blob", DataType.BYTES, true);
    storedSpec.setDefaultNullValue("deadbeef");
    stored.addField(storedSpec);

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    FieldSpec compiledSpec = new DimensionFieldSpec("blob", DataType.BYTES, true);
    compiledSpec.setDefaultNullValue("deadbeef");
    compiled.addField(compiledSpec);

    assertNull(PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled),
        "matching BYTES defaults must not trip a content-vs-reference equality regression");
  }

  /// Regression: BigDecimal.equals is scale-sensitive (1 != 1.0); the comparator must use
  /// compareTo so the same numeric default arriving via different literal forms is treated
  /// as equivalent. Without this fix, a hybrid second-variant CREATE on a BIG_DECIMAL column
  /// whose stored default arrived as "1.0" but DDL re-states "1" would falsely 409 CONFLICT.
  @Test
  public void acceptsScaleShiftedBigDecimalDefault() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    MetricFieldSpec storedSpec = new MetricFieldSpec("amount", DataType.BIG_DECIMAL,
        new BigDecimal("1.0"));
    stored.addField(storedSpec);

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    MetricFieldSpec compiledSpec = new MetricFieldSpec("amount", DataType.BIG_DECIMAL,
        new BigDecimal("1"));
    compiled.addField(compiledSpec);

    assertNull(PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled),
        "scale-shifted BIG_DECIMAL defaults must compare equal via compareTo");
  }

  /// BYTES default mismatch must still be rejected with content-aware comparison.
  @Test
  public void rejectsBytesDefaultNullValueMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    FieldSpec storedSpec = new DimensionFieldSpec("blob", DataType.BYTES, true);
    storedSpec.setDefaultNullValue("deadbeef");
    stored.addField(storedSpec);

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    FieldSpec compiledSpec = new DimensionFieldSpec("blob", DataType.BYTES, true);
    compiledSpec.setDefaultNullValue("cafebabe");
    compiled.addField(compiledSpec);

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("default null value"), msg);
  }

  /// Default-null-value mismatch must be rejected.
  @Test
  public void rejectsDefaultNullValueMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    FieldSpec storedSpec = new DimensionFieldSpec("id", DataType.INT, true);
    storedSpec.setDefaultNullValue(-1);
    stored.addField(storedSpec);

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    FieldSpec compiledSpec = new DimensionFieldSpec("id", DataType.INT, true);
    compiledSpec.setDefaultNullValue(0);
    compiled.addField(compiledSpec);

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("default null value"), msg);
  }

  /// DATETIME format mismatch must be rejected.
  @Test
  public void rejectsDateTimeFormatMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:SECONDS:EPOCH", "1:MILLISECONDS"));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("DATETIME format"), msg);
  }

  /// DATETIME granularity mismatch must be rejected.
  @Test
  public void rejectsDateTimeGranularityMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:MILLISECONDS:EPOCH", "1:SECONDS"));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("DATETIME granularity"), msg);
  }

  /// Matching multi-column, multi-type schemas must be accepted.
  @Test
  public void acceptsMatchingMixedColumnSchema() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("id", DataType.INT, true));
    stored.addField(new MetricFieldSpec("value", DataType.DOUBLE));
    stored.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));
    stored.setPrimaryKeyColumns(Arrays.asList("id"));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.INT, true));
    compiled.addField(new MetricFieldSpec("value", DataType.DOUBLE));
    compiled.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));

    assertNull(PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled));
  }
}
