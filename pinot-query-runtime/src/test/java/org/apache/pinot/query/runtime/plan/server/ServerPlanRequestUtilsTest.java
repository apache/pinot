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
package org.apache.pinot.query.runtime.plan.server;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.core.data.manager.InstanceDataManager;
import org.apache.pinot.core.data.manager.LogicalTableContext;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.local.data.manager.TableDataManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotSame;


/// Tests that the leaf stage hands the optional segments of its worker to the requests it builds for the server.
public class ServerPlanRequestUtilsTest {
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("tbl")
      .addSingleValueDimension("col1", FieldSpec.DataType.STRING, "")
      .build();

  @Test
  public void testHybridTableRequestsCarryOptionalSegmentsPerTableType() {
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of());
    workerMetadata.setTableSegmentsMap(Map.of("OFFLINE", List.of("offlineSeg"), "REALTIME", List.of("realtimeSeg")));
    workerMetadata.setOptionalSegmentsMap(Map.of("REALTIME", List.of("newRealtimeSeg")));
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    for (TableType tableType : TableType.values()) {
      String tableNameWithType = "tbl_" + tableType;
      TableDataManager tableDataManager = mock(TableDataManager.class);
      when(tableDataManager.getTableName()).thenReturn(tableNameWithType);
      when(tableDataManager.getCachedTableConfigAndSchema()).thenReturn(Pair.of(tableConfig(tableType), SCHEMA));
      when(instanceDataManager.getTableDataManager(tableNameWithType)).thenReturn(tableDataManager);
    }

    List<InstanceRequest> instanceRequests = ServerPlanRequestUtils.constructServerQueryRequests(
        executionContext("tbl", workerMetadata), CalciteSqlParser.compileToPinotQuery("SELECT col1 FROM tbl"),
        instanceDataManager);

    assertEquals(instanceRequests.size(), 2);
    InstanceRequest offlineRequest = instanceRequests.get(0);
    assertEquals(offlineRequest.getSearchSegments(), List.of("offlineSeg"));
    assertFalse(offlineRequest.isSetOptionalSegments());
    InstanceRequest realtimeRequest = instanceRequests.get(1);
    assertEquals(realtimeRequest.getSearchSegments(), List.of("realtimeSeg"));
    assertEquals(realtimeRequest.getOptionalSegments(), List.of("newRealtimeSeg"));
    // The server may append to the list (e.g. the newly added segments of an upsert table), so it must be a copy.
    assertNotSame(realtimeRequest.getOptionalSegments(), workerMetadata.getOptionalSegmentsMap().get("REALTIME"));
  }

  @Test
  public void testLogicalTableRequestsCarryOptionalSegmentsPerPhysicalTable() {
    WorkerMetadata workerMetadata = new WorkerMetadata(0, Map.of());
    workerMetadata.setLogicalTableSegmentsMap(
        Map.of("t1_OFFLINE", List.of("t1Seg"), "t2_REALTIME", List.of("t2Seg")));
    workerMetadata.setOptionalSegmentsMap(Map.of("t2_REALTIME", List.of("t2NewSeg")));
    LogicalTableContext logicalTableContext = mock(LogicalTableContext.class);
    when(logicalTableContext.getRefOfflineTableConfig()).thenReturn(tableConfig(TableType.OFFLINE));
    when(logicalTableContext.getRefRealtimeTableConfig()).thenReturn(tableConfig(TableType.REALTIME));
    when(logicalTableContext.getLogicalTableSchema()).thenReturn(SCHEMA);
    InstanceDataManager instanceDataManager = mock(InstanceDataManager.class);
    when(instanceDataManager.getLogicalTableContext("tbl")).thenReturn(logicalTableContext);

    List<InstanceRequest> instanceRequests = ServerPlanRequestUtils.constructLogicalTableServerQueryRequests(
        executionContext("tbl", workerMetadata), CalciteSqlParser.compileToPinotQuery("SELECT col1 FROM tbl"),
        instanceDataManager);

    assertEquals(instanceRequests.size(), 2);
    InstanceRequest offlineRequest = instanceRequests.get(0);
    assertEquals(offlineRequest.getTableSegmentsInfoList().get(0).getSegments(), List.of("t1Seg"));
    assertFalse(offlineRequest.getTableSegmentsInfoList().get(0).isSetOptionalSegments());
    InstanceRequest realtimeRequest = instanceRequests.get(1);
    assertEquals(realtimeRequest.getTableSegmentsInfoList().get(0).getSegments(), List.of("t2Seg"));
    assertEquals(realtimeRequest.getTableSegmentsInfoList().get(0).getOptionalSegments(), List.of("t2NewSeg"));
  }

  private static TableConfig tableConfig(TableType tableType) {
    return new TableConfigBuilder(tableType).setTableName("tbl").build();
  }

  private static OpChainExecutionContext executionContext(String tableName, WorkerMetadata workerMetadata) {
    StageMetadata stageMetadata = new StageMetadata(1, List.of(workerMetadata),
        Map.of(DispatchablePlanFragment.TABLE_NAME_KEY, tableName));
    long activeDeadlineMs = System.currentTimeMillis() + 10_000;
    return new OpChainExecutionContext(mock(MailboxService.class), 1, "cid", activeDeadlineMs,
        activeDeadlineMs + 1_000, "broker", Map.of(), stageMetadata, workerMetadata, null, false, false);
  }
}
