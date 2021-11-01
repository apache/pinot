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
package org.apache.pinot.controller.helix.core.retention;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.lineage.LineageEntry;
import org.apache.pinot.common.lineage.LineageEntryState;
import org.apache.pinot.common.lineage.SegmentLineage;
import org.apache.pinot.common.lineage.SegmentLineageAccessHelper;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.ControllerTestUtils;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.controller.utils.SegmentMetadataMockUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentLineageCleanupTest {
  private static final int NUM_INSTANCES = 1;
  private static final long MAX_TIMEOUT_IN_MILLISECOND = 10_000L; // 10 seconds

  private static final String OFFLINE_TABLE_NAME = "segmentTable_OFFLINE";
  private static final String BROKER_TENANT_NAME = "brokerTenant";
  private static final String SERVER_TENANT_NAME = "serverTenant";
  private static final String RETENTION_TIME_UNIT = "DAYS";
  private static final String RETENTION_TIME_VALUE = "1";

  private RetentionManager _retentionManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    ControllerTestUtils.setupClusterAndValidate();

    // Create server tenant
    Tenant serverTenant = new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, NUM_INSTANCES, NUM_INSTANCES, 0);
    ControllerTestUtils.getHelixResourceManager().createServerTenant(serverTenant);

    // Create broker tenant
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, NUM_INSTANCES, 0, 0);
    PinotResourceManagerResponse response =
        ControllerTestUtils.getHelixResourceManager().createBrokerTenant(brokerTenant);
    Assert.assertTrue(response.isSuccessful());

    // Enable lead controller resource
    ControllerTestUtils.enableResourceConfigForLeadControllerResource(true);

    // Create the retention manager
    LeadControllerManager leadControllerManager = mock(LeadControllerManager.class);
    when(leadControllerManager.isLeaderForTable(anyString())).thenReturn(true);
    ControllerConf conf = new ControllerConf();
    ControllerMetrics controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    conf.setRetentionControllerFrequencyInSeconds(0);
    conf.setDeletedSegmentsRetentionInDays(0);
    _retentionManager = new RetentionManager(ControllerTestUtils.getHelixResourceManager(), leadControllerManager, conf,
        controllerMetrics);

    // Update table config
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(OFFLINE_TABLE_NAME)
        .setBrokerTenant(BROKER_TENANT_NAME)
        .setServerTenant(SERVER_TENANT_NAME)
        .setNumReplicas(1)
        .setRetentionTimeUnit(RETENTION_TIME_UNIT)
        .setRetentionTimeValue(RETENTION_TIME_VALUE)
        .build();
    ControllerTestUtils.getHelixResourceManager().addTable(tableConfig);
  }

  @Test
  public void testSegmentLineageCleanup()
      throws IOException, InterruptedException {
    // Create metadata for original segments
    for (int i = 0; i < 5; i++) {
      ControllerTestUtils.getHelixResourceManager().addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "segment_" + i), "downloadUrl");
    }

    // Create metadata for merged segments.
    for (int i = 0; i < 2; i++) {
      ControllerTestUtils.getHelixResourceManager().addNewSegment(OFFLINE_TABLE_NAME,
          SegmentMetadataMockUtils.mockSegmentMetadata(OFFLINE_TABLE_NAME, "merged_" + i), "downloadUrl");
    }
    Assert.assertEquals(ControllerTestUtils.getHelixResourceManager().getSegmentsFor(OFFLINE_TABLE_NAME).size(), 7);
    long currentTimeInMillis = System.currentTimeMillis();

    // Validate the case when the lineage entry state is 'IN_PROGRESS'
    SegmentLineage segmentLineage = new SegmentLineage(OFFLINE_TABLE_NAME);
    segmentLineage.addLineageEntry("0",
        new LineageEntry(Arrays.asList("segment_0", "segment_1"), Arrays.asList("merged_0"),
            LineageEntryState.IN_PROGRESS, currentTimeInMillis));
    SegmentLineageAccessHelper
        .writeSegmentLineage(ControllerTestUtils.getHelixResourceManager().getPropertyStore(), segmentLineage, -1);
    _retentionManager.processTable(OFFLINE_TABLE_NAME);
    waitForSegmentsToDelete(OFFLINE_TABLE_NAME, 7);
    List<String> segmentsForTable = ControllerTestUtils.getHelixResourceManager().getSegmentsFor(OFFLINE_TABLE_NAME);
    Assert.assertEquals(segmentsForTable.size(), 7);

    // Validate the case when the lineage entry state is 'COMPLETED'
    segmentLineage.updateLineageEntry("0",
        new LineageEntry(Arrays.asList("segment_0", "segment_1"), Arrays.asList("merged_0"),
            LineageEntryState.COMPLETED, currentTimeInMillis));
    SegmentLineageAccessHelper
        .writeSegmentLineage(ControllerTestUtils.getHelixResourceManager().getPropertyStore(), segmentLineage, -1);
    _retentionManager.processTable(OFFLINE_TABLE_NAME);
    waitForSegmentsToDelete(OFFLINE_TABLE_NAME, 5);
    segmentsForTable = ControllerTestUtils.getHelixResourceManager().getSegmentsFor(OFFLINE_TABLE_NAME);
    Assert.assertEquals(segmentsForTable.size(), 5);
    Assert.assertTrue(Collections.disjoint(segmentsForTable, Arrays.asList("segment_0", "segment_1")));

    // Validate the case when the lineage entry state is 'COMPLETED' and all segments are deleted
    ControllerTestUtils.getHelixResourceManager().deleteSegment(OFFLINE_TABLE_NAME, "merged_0");
    waitForSegmentsToDelete(OFFLINE_TABLE_NAME, 4);
    _retentionManager.processTable(OFFLINE_TABLE_NAME);
    waitForSegmentsToDelete(OFFLINE_TABLE_NAME, 4);
    segmentsForTable = ControllerTestUtils.getHelixResourceManager().getSegmentsFor(OFFLINE_TABLE_NAME);
    Assert.assertEquals(segmentsForTable.size(), 4);
    Assert.assertTrue(Collections.disjoint(segmentsForTable, Arrays.asList("segment_0", "segment_1", "merged_0")));
    segmentLineage =
        SegmentLineageAccessHelper
            .getSegmentLineage(ControllerTestUtils.getHelixResourceManager().getPropertyStore(), OFFLINE_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 0);

    // Validate the case when the lineage entry state is 'IN_PROGRESS' and timestamp is old
    LineageEntry lineageEntry =
        new LineageEntry(Arrays.asList("segment_2", "segment_3"), Arrays.asList("merged_1", "merged_2"),
            LineageEntryState.IN_PROGRESS, currentTimeInMillis - TimeUnit.DAYS.toMillis(2L));
    segmentLineage.addLineageEntry("1", lineageEntry);
    SegmentLineageAccessHelper
        .writeSegmentLineage(ControllerTestUtils.getHelixResourceManager().getPropertyStore(), segmentLineage, -1);
    _retentionManager.processTable(OFFLINE_TABLE_NAME);
    waitForSegmentsToDelete(OFFLINE_TABLE_NAME, 3);
    segmentsForTable = ControllerTestUtils.getHelixResourceManager().getSegmentsFor(OFFLINE_TABLE_NAME);
    Assert.assertEquals(segmentsForTable.size(), 3);
    Assert.assertTrue(Collections.disjoint(segmentsForTable, Arrays.asList("merged_1", "merged_2")));
    segmentLineage =
        SegmentLineageAccessHelper
            .getSegmentLineage(ControllerTestUtils.getHelixResourceManager().getPropertyStore(), OFFLINE_TABLE_NAME);
    Assert.assertEquals(segmentLineage.getLineageEntryIds().size(), 1);
  }

  private void waitForSegmentsToDelete(String tableNameWithType, int expectedNumSegmentsAfterDelete)
      throws InterruptedException {
    long endTimeMs = System.currentTimeMillis() + MAX_TIMEOUT_IN_MILLISECOND;
    do {
      if (ControllerTestUtils.getHelixResourceManager().getSegmentsFor(tableNameWithType).size()
          == expectedNumSegmentsAfterDelete) {
        return;
      } else {
        Thread.sleep(500L);
      }
    } while (System.currentTimeMillis() < endTimeMs);
    throw new RuntimeException("Timeout while waiting for segments to be deleted");
  }

  @AfterClass
  public void tearDown() {
    ControllerTestUtils.cleanup();
  }
}
