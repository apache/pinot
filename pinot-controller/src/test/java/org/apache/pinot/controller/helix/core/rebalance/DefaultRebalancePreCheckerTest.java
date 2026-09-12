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
/// Unless stated otherwise, the tests share the same assignment: 4 segments of 100 bytes each, moving so that both
/// servers gain and shed some. `Server_0` gains 100 bytes and sheds 200, `Server_1` gains 200 and sheds 100. Every
/// server has 1000 bytes of total space and the threshold is 50% of it.
public class DefaultRebalancePreCheckerTest {
  private static final String SERVER_0 = "Server_0";
  private static final String SERVER_1 = "Server_1";
  private static final long TOTAL_SPACE_BYTES = 1000L;
  private static final long TABLE_SIZE_PER_REPLICA_BYTES = 400L;
  private static final double THRESHOLD = 0.5;

  private static final Map<String, Map<String, String>> CURRENT_ASSIGNMENT =
      Map.of("segment_0", Map.of(SERVER_0, ONLINE), "segment_1", Map.of(SERVER_0, ONLINE), "segment_2",
          Map.of(SERVER_0, ONLINE), "segment_3", Map.of(SERVER_1, ONLINE));
  private static final Map<String, Map<String, String>> TARGET_ASSIGNMENT =
      Map.of("segment_0", Map.of(SERVER_0, ONLINE), "segment_1", Map.of(SERVER_1, ONLINE), "segment_2",
          Map.of(SERVER_1, ONLINE), "segment_3", Map.of(SERVER_0, ONLINE));

  /// An assignment where `Server_0` only sheds segments: it gives its 2 remaining segments to `Server_1` and gains
  /// nothing.
  private static final Map<String, Map<String, String>> SHED_ONLY_CURRENT_ASSIGNMENT =
      Map.of("segment_0", Map.of(SERVER_0, ONLINE), "segment_1", Map.of(SERVER_0, ONLINE), "segment_2",
          Map.of(SERVER_0, ONLINE), "segment_3", Map.of(SERVER_0, ONLINE));
  private static final Map<String, Map<String, String>> SHED_ONLY_TARGET_ASSIGNMENT =
      Map.of("segment_0", Map.of(SERVER_0, ONLINE), "segment_1", Map.of(SERVER_0, ONLINE), "segment_2",
          Map.of(SERVER_1, ONLINE), "segment_3", Map.of(SERVER_1, ONLINE));

  private final DefaultRebalancePreChecker _preChecker = new DefaultRebalancePreChecker();

  /// [ResourceUtilizationInfo] is a mutable static shared by everything running in the same JVM fork.
  @AfterClass
  public void tearDown() {
    ResourceUtilizationInfo.setDiskUsageInfo(Map.of());
  }

  @Test
  public void testWithinThresholdBothDuringAndAfterRebalance() {
    // Server_0 peaks at 400 of its 1000 bytes and Server_1 at 300, neither reaches 500
    setDiskUsage(300L, 100L);
    RebalancePreCheckerResult result = checkDiskUtilization(new RebalanceConfig());
    assertEquals(result.getPreCheckStatus(), PreCheckStatus.PASS);
    assertEquals(result.getMessage(), "Within threshold (<50%)");
  }

  @Test
  public void testOverThresholdAfterRebalanceIsAnErrorWhateverTheRebalanceConfig() {
    // Server_1 ends up at 520 of its 1000 bytes, which no rebalance config can bring back under the threshold
    setDiskUsage(100L, 420L);
    for (RebalanceConfig rebalanceConfig : new RebalanceConfig[]{
        new RebalanceConfig(), lowDiskMode(), downtime(), bestEfforts()
    }) {
      RebalancePreCheckerResult result = checkDiskUtilization(rebalanceConfig);
      assertEquals(result.getPreCheckStatus(), PreCheckStatus.ERROR);
      assertEquals(result.getMessage(),
          "UNSAFE. Servers with unsafe disk utilization AFTER rebalance (>=50%): " + SERVER_1 + " (52%)");
    }
  }

  @Test
  public void testOverThresholdOnlyDuringRebalanceIsAnErrorWithoutLowDiskMode() {
    // Server_1 transiently holds the 200 bytes it gains on top of the 100 it is about to shed, peaking at 520
    setDiskUsage(100L, 320L);
    RebalancePreCheckerResult result = checkDiskUtilization(new RebalanceConfig());
    assertEquals(result.getPreCheckStatus(), PreCheckStatus.ERROR);
    assertEquals(result.getMessage(),
        "UNSAFE. Servers with unsafe disk utilization DURING rebalance (>=50%): " + SERVER_1 + " (52%). Enable "
            + "lowDiskMode to delete segments before adding the new ones");
  }

  @Test
  public void testOverThresholdOnlyDuringRebalanceIsSafeWithLowDiskMode() {
    setDiskUsage(100L, 320L);
    RebalancePreCheckerResult result = checkDiskUtilization(lowDiskMode());
    assertEquals(result.getPreCheckStatus(), PreCheckStatus.PASS);
    assertEquals(result.getMessage(),
        "Within threshold (<50%) AFTER rebalance. Servers that would go over it DURING the rebalance: " + SERVER_1
            + " (52%). lowDiskMode avoids that transient disk usage by deleting segments before adding the new ones");
  }

  @Test
  public void testDowntimeCancelsLowDiskModeOut() {
    // Downtime replaces the IdealState with the target assignment in one go, skipping the incremental path that is the
    // only one honoring lowDiskMode, so the transient peak stands whether or not lowDiskMode is set
    setDiskUsage(100L, 320L);
    RebalanceConfig lowDiskModeAndDowntime = lowDiskMode();
    lowDiskModeAndDowntime.setDowntime(true);

    for (RebalanceConfig rebalanceConfig : new RebalanceConfig[]{downtime(), lowDiskModeAndDowntime}) {
      RebalancePreCheckerResult result = checkDiskUtilization(rebalanceConfig);
      assertEquals(result.getPreCheckStatus(), PreCheckStatus.ERROR);
      assertEquals(result.getMessage(),
          "UNSAFE. Servers with unsafe disk utilization DURING rebalance (>=50%): " + SERVER_1 + " (52%). lowDiskMode, "
              + "which would delete segments before adding the new ones, has no effect while downtime is enabled");
    }
  }

  @Test
  public void testServerGainingNothingIsNotFlaggedDuringRebalance() {
    // Server_0 is already at 550 of its 1000 bytes and only sheds segments. The rebalance cannot push it any higher
    // and lowDiskMode would have nothing to delete first, so flagging it would blame the rebalance for a pre-existing
    // condition. It drops to 350 once done, so there is nothing to report at all.
    setDiskUsage(550L, 0L);
    RebalancePreCheckerResult result = checkDiskUtilization(new RebalanceConfig(), SHED_ONLY_CURRENT_ASSIGNMENT,
        SHED_ONLY_TARGET_ASSIGNMENT);
    assertEquals(result.getPreCheckStatus(), PreCheckStatus.PASS);
    assertEquals(result.getMessage(), "Within threshold (<50%)");
  }

  @Test
  public void testServerGainingNothingAndStayingOverThresholdIsStillFlagged() {
    // Server_0 shedding its 2 segments is not enough to bring it back under the threshold, which the AFTER estimate
    // catches even though the DURING one skips it
    setDiskUsage(750L, 0L);
    RebalancePreCheckerResult result = checkDiskUtilization(new RebalanceConfig(), SHED_ONLY_CURRENT_ASSIGNMENT,
        SHED_ONLY_TARGET_ASSIGNMENT);
    assertEquals(result.getPreCheckStatus(), PreCheckStatus.ERROR);
    assertEquals(result.getMessage(),
        "UNSAFE. Servers with unsafe disk utilization AFTER rebalance (>=50%): " + SERVER_0 + " (55%)");
  }

  @Test
  public void testDiskUsageInfoNotAvailable() {
    ResourceUtilizationInfo.setDiskUsageInfo(Map.of());
    RebalancePreCheckerResult result = checkDiskUtilization(new RebalanceConfig());
    assertEquals(result.getPreCheckStatus(), PreCheckStatus.WARN);
    assertTrue(result.getMessage().startsWith("Disk usage info has not been updated"), result.getMessage());
  }

  private RebalancePreCheckerResult checkDiskUtilization(RebalanceConfig rebalanceConfig) {
    return checkDiskUtilization(rebalanceConfig, CURRENT_ASSIGNMENT, TARGET_ASSIGNMENT);
  }

  private RebalancePreCheckerResult checkDiskUtilization(RebalanceConfig rebalanceConfig,
      Map<String, Map<String, String>> currentAssignment, Map<String, Map<String, String>> targetAssignment) {
    return _preChecker.checkDiskUtilization(getPreCheckContext(rebalanceConfig, currentAssignment, targetAssignment),
        THRESHOLD);
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

  private static RebalanceConfig bestEfforts() {
    RebalanceConfig rebalanceConfig = new RebalanceConfig();
    rebalanceConfig.setBestEfforts(true);
    return rebalanceConfig;
  }

  private static void setDiskUsage(long usedSpaceBytesServer0, long usedSpaceBytesServer1) {
    long now = System.currentTimeMillis();
    ResourceUtilizationInfo.setDiskUsageInfo(
        Map.of(SERVER_0, new DiskUsageInfo(SERVER_0, "", TOTAL_SPACE_BYTES, usedSpaceBytesServer0, now), SERVER_1,
            new DiskUsageInfo(SERVER_1, "", TOTAL_SPACE_BYTES, usedSpaceBytesServer1, now)));
  }

  private static PreCheckContext getPreCheckContext(RebalanceConfig rebalanceConfig,
      Map<String, Map<String, String>> currentAssignment, Map<String, Map<String, String>> targetAssignment) {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    return new PreCheckContext("jobId", tableConfig.getTableName(), tableConfig, currentAssignment, targetAssignment,
        getTableSizeDetails(), rebalanceConfig, null, null);
  }

  private static TableSizeReader.TableSubTypeSizeDetails getTableSizeDetails() {
    TableSizeReader.TableSubTypeSizeDetails tableSubTypeSizeDetails = new TableSizeReader.TableSubTypeSizeDetails();
    tableSubTypeSizeDetails._reportedSizePerReplicaInBytes = TABLE_SIZE_PER_REPLICA_BYTES;
    return tableSubTypeSizeDetails;
  }
}
