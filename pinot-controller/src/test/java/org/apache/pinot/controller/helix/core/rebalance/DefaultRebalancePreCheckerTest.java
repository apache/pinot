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
package org.apache.pinot.controller.helix.core.rebalance;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.restlet.resources.DiskUsageInfo;
import org.apache.pinot.common.restlet.resources.RebalanceConfig;
import org.apache.pinot.common.restlet.resources.RebalancePreCheckerResult;
import org.apache.pinot.common.restlet.resources.RebalancePreCheckerResult.PreCheckStatus;
import org.apache.pinot.controller.helix.core.rebalance.RebalancePreChecker.PreCheckContext;
import org.apache.pinot.controller.util.TableSizeReader;
import org.apache.pinot.controller.validation.ResourceUtilizationInfo;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Tests the consolidated disk utilization pre-check of [DefaultRebalancePreChecker].
///
/// All the tests share the same assignment: 4 segments of 100 bytes each, all on `Server_0`, out of which `segment_2`
/// and `segment_3` move to `Server_1`. Every server has 1000 bytes of total space and the threshold is 50% of it.
public class DefaultRebalancePreCheckerTest {
  private static final String SERVER_0 = "Server_0";
  private static final String SERVER_1 = "Server_1";
  private static final int NUM_SEGMENTS = 4;
  private static final long TOTAL_SPACE_BYTES = 1000L;
  private static final long TABLE_SIZE_PER_REPLICA_BYTES = 400L;
  private static final double THRESHOLD = 0.5;

  private final DefaultRebalancePreChecker _preChecker = new DefaultRebalancePreChecker();

  /// [ResourceUtilizationInfo] is a mutable static shared by everything running in the same JVM fork.
  @AfterClass
  public void tearDown() {
    ResourceUtilizationInfo.setDiskUsageInfo(Map.of());
  }

  @Test
  public void testWithinThresholdBothDuringAndAfterRebalance() {
    // Server_0 sheds 200 bytes and Server_1 gains them, neither ever goes over 500 bytes
    setDiskUsage(400L, 0L);
    RebalancePreCheckerResult result = checkDiskUtilization(new RebalanceConfig());
    assertEquals(result.getPreCheckStatus(), PreCheckStatus.PASS);
    assertEquals(result.getMessage(), "Within threshold (<50%)");
  }

  @Test
  public void testOverThresholdAfterRebalanceIsAnErrorWhateverTheRebalanceConfig() {
    // Server_1 ends up at 550 of its 1000 bytes, which no rebalance config can bring back under the threshold
    setDiskUsage(400L, 350L);
    for (RebalanceConfig rebalanceConfig : new RebalanceConfig[]{
        new RebalanceConfig(), lowDiskMode(), downtime()
    }) {
      RebalancePreCheckerResult result = checkDiskUtilization(rebalanceConfig);
      assertEquals(result.getPreCheckStatus(), PreCheckStatus.ERROR);
      assertEquals(result.getMessage(),
          "UNSAFE. Servers with unsafe disk utilization after rebalance (>50%): " + SERVER_1 + " (55%)");
    }
  }

  @Test
  public void testOverThresholdOnlyDuringRebalanceIsAnErrorWithoutLowDiskMode() {
    // Server_0 is at 550 of its 1000 bytes and only gets back under the threshold once it has shed its 200 bytes.
    // downtime does not order the drops before the adds, so it does not rule the transient peak out either.
    setDiskUsage(550L, 0L);
    for (RebalanceConfig rebalanceConfig : new RebalanceConfig[]{new RebalanceConfig(), downtime()}) {
      RebalancePreCheckerResult result = checkDiskUtilization(rebalanceConfig);
      assertEquals(result.getPreCheckStatus(), PreCheckStatus.ERROR);
      assertTrue(result.getMessage()
              .startsWith("UNSAFE. Servers with unsafe disk utilization during rebalance (>50%): " + SERVER_0
                  + " (55%)"), result.getMessage());
    }
  }

  @Test
  public void testOverThresholdOnlyDuringRebalanceIsSafeWithLowDiskMode() {
    setDiskUsage(550L, 0L);
    RebalancePreCheckerResult result = checkDiskUtilization(lowDiskMode());
    assertEquals(result.getPreCheckStatus(), PreCheckStatus.PASS);
    assertTrue(result.getMessage().startsWith("Within threshold (<50%) after rebalance"), result.getMessage());
  }

  @Test
  public void testDowntimeCancelsLowDiskModeOut() {
    // Downtime replaces the IdealState with the target assignment in one go, skipping the incremental path that is the
    // only one honoring lowDiskMode, so the transient peak stands and the message has to say so
    setDiskUsage(550L, 0L);
    RebalanceConfig rebalanceConfig = lowDiskMode();
    rebalanceConfig.setDowntime(true);

    RebalancePreCheckerResult result = checkDiskUtilization(rebalanceConfig);
    assertEquals(result.getPreCheckStatus(), PreCheckStatus.ERROR);
    assertEquals(result.getMessage(),
        "UNSAFE. Servers with unsafe disk utilization during rebalance (>50%): " + SERVER_0 + " (55%). lowDiskMode has "
            + "no effect while downtime is enabled, disable downtime for it to delete segments before adding the new "
            + "ones");
  }

  @Test
  public void testDiskUsageInfoNotAvailable() {
    ResourceUtilizationInfo.setDiskUsageInfo(Map.of());
    RebalancePreCheckerResult result = checkDiskUtilization(new RebalanceConfig());
    assertEquals(result.getPreCheckStatus(), PreCheckStatus.WARN);
    assertTrue(result.getMessage().startsWith("Disk usage info has not been updated"), result.getMessage());
  }

  private RebalancePreCheckerResult checkDiskUtilization(RebalanceConfig rebalanceConfig) {
    return _preChecker.checkDiskUtilization(getPreCheckContext(rebalanceConfig), THRESHOLD);
  }

  private static RebalanceConfig lowDiskMode() {
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setLowDiskMode(true);
    return rebalanceConfig;
  }

  private static RebalanceConfig downtime() {
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setDowntime(true);
    return rebalanceConfig;
  }

  private static void setDiskUsage(long usedSpaceBytesServer0, long usedSpaceBytesServer1) {
    long now = System.currentTimeMillis();
    ResourceUtilizationInfo.setDiskUsageInfo(
        Map.of(SERVER_0, new DiskUsageInfo(SERVER_0, "", TOTAL_SPACE_BYTES, usedSpaceBytesServer0, now), SERVER_1,
            new DiskUsageInfo(SERVER_1, "", TOTAL_SPACE_BYTES, usedSpaceBytesServer1, now)));
  }

  private static PreCheckContext getPreCheckContext(RebalanceConfig rebalanceConfig) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    return new PreCheckContext("jobId", tableConfig.getTableName(), tableConfig, getCurrentAssignment(),
        getTargetAssignment(), getTableSizeDetails(), rebalanceConfig, null, null);
  }

  private static Map<String, Map<String, String>> getCurrentAssignment() {
    Map<String, Map<String, String>> currentAssignment = new HashMap<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      currentAssignment.put("segment_" + i, Map.of(SERVER_0, ONLINE));
    }
    return currentAssignment;
  }

  private static Map<String, Map<String, String>> getTargetAssignment() {
    Map<String, Map<String, String>> targetAssignment = new HashMap<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      targetAssignment.put("segment_" + i, Map.of(i < NUM_SEGMENTS / 2 ? SERVER_0 : SERVER_1, ONLINE));
    }
    return targetAssignment;
  }

  private static TableSizeReader.TableSubTypeSizeDetails getTableSizeDetails() {
    TableSizeReader.TableSubTypeSizeDetails tableSubTypeSizeDetails = new TableSizeReader.TableSubTypeSizeDetails();
    tableSubTypeSizeDetails._reportedSizePerReplicaInBytes = TABLE_SIZE_PER_REPLICA_BYTES;
    return tableSubTypeSizeDetails;
  }
}
