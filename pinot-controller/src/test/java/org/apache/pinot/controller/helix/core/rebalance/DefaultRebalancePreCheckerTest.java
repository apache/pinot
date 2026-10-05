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
import java.util.TreeMap;
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

  /// `lowDiskMode` is not a blanket guarantee: where the rebalance cannot progress at all within the disk the servers
  /// start with, it goes over rather than stalling. The pre-check replays the rebalance to find that out instead of
  /// assuming `lowDiskMode` always avoids the transient usage.
  ///
  /// Driven through the real [DefaultRebalancePreChecker] with an assignment that genuinely needs the budget given up,
  /// rather than by stubbing the replay, so that a wiring mistake between [PreCheckContext], the routing mode,
  /// batching, the segment sizes and the replay shows up here.
  @Test
  public void testOverThresholdDuringRebalanceIsAnErrorWhenLowDiskModeCannotAvoidIt() {
    // host05 starts on 2041 MiB and the target places 1523 MiB on it, so its ceiling is what it started with. It is
    // pinned with segments it cannot drop without going below three available replicas, and a step arrives where it is
    // the only place left to put a segment and has nothing spare
    String[][] groups = {
        {"host00,host01,host02,host03", "host02,host04,host05,host06", "0,1,2,3,4,5,6,7,8,9"},
        {"host00,host02,host06", "host00,host03,host05,host06", "10,11,12"},
        {"host00,host01,host03,host06", "host01,host02,host03,host05", "13,14,15"},
        {"host02,host04,host05,host06", "host00,host01,host03,host04", "16,17,18,19,20,21,22,23"},
        {"host01,host02,host03,host04", "host00,host02,host05,host06", "24,25,26,27,28,29,30,31,32"}
    };
    long[] sizesInMib = {
        31, 12, 17, 9, 16, 14, 431, 664, 23, 23, 23, 20, 12, 12, 28, 9, 29, 1162, 768, 14, 15, 31, 8, 14, 21, 9, 14,
        25, 15, 25, 18, 24, 28
    };
    Map<String, Map<String, String>> current = new HashMap<>();
    Map<String, Map<String, String>> target = new HashMap<>();
    TableSizeReader.TableSubTypeSizeDetails tableSizeDetails = new TableSizeReader.TableSubTypeSizeDetails();
    for (String[] group : groups) {
      for (String index : group[2].split(",")) {
        String segment = String.format("segment%03d", Integer.parseInt(index));
        current.put(segment, instanceStateMap(group[0].split(",")));
        target.put(segment, instanceStateMap(group[1].split(",")));
        TableSizeReader.SegmentSizeDetails segmentSizeDetails = new TableSizeReader.SegmentSizeDetails();
        segmentSizeDetails._maxReportedSizePerReplicaInBytes =
            sizesInMib[Integer.parseInt(index)] * 1024 * 1024;
        tableSizeDetails._segments.put(segment, segmentSizeDetails);
      }
    }

    // The size check works off the average segment size, which comes from the per-replica table size
    long mib = 1024L * 1024;
    long tableSizePerReplicaMib = 0;
    for (long sizeInMib : sizesInMib) {
      tableSizePerReplicaMib += sizeInMib;
    }
    tableSizeDetails._reportedSizePerReplicaInBytes = tableSizePerReplicaMib * mib;

    // host05 takes on 25 segments and sheds 8, at an average of 108 MiB each, so on 20 GiB of disk it crosses 50%
    // during the rebalance (7800 + 2700 = 52%) and comes back under it after (9636 = 48%). That is the branch the
    // replay guards. The others are given enough room never to be flagged
    long now = System.currentTimeMillis();
    Map<String, DiskUsageInfo> diskUsage = new HashMap<>();
    for (int i = 0; i <= 6; i++) {
      String server = String.format("host%02d", i);
      diskUsage.put(server, "host05".equals(server)
          ? new DiskUsageInfo(server, "", 20_000L * mib, 7_800L * mib, now)
          : new DiskUsageInfo(server, "", 400_000L * mib, 1_000L * mib, now));
    }
    ResourceUtilizationInfo.setDiskUsageInfo(diskUsage);

    RebalanceConfig rebalanceConfig = lowDiskMode();
    rebalanceConfig.setMinAvailableReplicas(3);
    rebalanceConfig.setBatchSizePerServer(1);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    RebalancePreCheckerResult result = _preChecker.checkDiskUtilization(
        new PreCheckContext("jobId", tableConfig.getTableName(), tableConfig, current, target, tableSizeDetails,
            rebalanceConfig, null, null), 0.5);

    System.out.println("diskUtilization: " + result.getPreCheckStatus() + " — " + result.getMessage());
    assertEquals(result.getPreCheckStatus(), PreCheckStatus.ERROR, result.getMessage());
    assertTrue(result.getMessage().contains("lowDiskMode cannot avoid it for this target assignment"),
        result.getMessage());
    assertTrue(result.getMessage().contains("host05"), result.getMessage());
  }

  private static Map<String, String> instanceStateMap(String... instances) {
    Map<String, String> instanceStateMap = new TreeMap<>();
    for (String instance : instances) {
      instanceStateMap.put(instance, ONLINE);
    }
    return instanceStateMap;
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
