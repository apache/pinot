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
package org.apache.pinot.controller.validation;

import com.google.common.collect.HashBiMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.model.ExternalView;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.restlet.resources.TableMetadataInfo;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.QuotaEnforcementMode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.utils.CommonConstants.Helix.StateModel.SegmentStateModel;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class PrimaryKeyQuotaCheckerTest {
  private static final String TABLE_NAME = "pkQuotaTestTable_REALTIME";
  private static final int NUM_REPLICAS = 2;

  private ServerSegmentMetadataReader _reader;
  private PinotHelixResourceManager _helix;
  private ControllerMetrics _controllerMetrics;
  private ControllerConf _controllerConf;

  @BeforeMethod
  public void setup()
      throws Exception {
    _reader = mock(ServerSegmentMetadataReader.class);
    _helix = mock(PinotHelixResourceManager.class);
    _controllerMetrics = new ControllerMetrics(PinotMetricUtils.getPinotMetricsRegistry());
    _controllerConf = mock(ControllerConf.class);
    when(_controllerConf.getServerAdminRequestTimeoutSeconds()).thenReturn(1);
    when(_controllerConf.getEnablePrimaryKeyQuotaCheck()).thenReturn(true);

    assignSegments("Server_0");
    when(_helix.getDataInstanceAdminEndpoints(any())).thenReturn(HashBiMap.create());
    when(_helix.getNumReplicas(any())).thenReturn(NUM_REPLICAS);
  }

  /// Assigns one segment per server in the IdealState and marks it ONLINE in the ExternalView. Returns the ExternalView
  /// so a test can change a segment's state.
  private ExternalView assignSegments(String... servers) {
    Map<String, List<String>> serverToSegments = new HashMap<>();
    ExternalView externalView = new ExternalView(TABLE_NAME);
    for (String server : servers) {
      String segment = segmentOf(server);
      serverToSegments.put(server, List.of(segment));
      externalView.setState(segment, server, SegmentStateModel.ONLINE);
    }
    when(_helix.getServerToSegmentsMap(TABLE_NAME)).thenReturn(serverToSegments);
    when(_helix.getTableExternalView(TABLE_NAME)).thenReturn(externalView);
    return externalView;
  }

  private static String segmentOf(String server) {
    return "segment_on_" + server;
  }

  private PrimaryKeyQuotaChecker newChecker() {
    return new PrimaryKeyQuotaChecker(_reader, _helix, _controllerMetrics, _controllerConf);
  }

  private static TableConfig upsertTableConfig(@Nullable Long cap, @Nullable QuotaEnforcementMode mode) {
    TableConfig tableConfig = mock(TableConfig.class);
    when(tableConfig.getTableName()).thenReturn(TABLE_NAME);
    when(tableConfig.isUpsertEnabled()).thenReturn(true);
    when(tableConfig.getQuotaConfig()).thenReturn(new QuotaConfig(null, null, null, null, cap, mode));
    return tableConfig;
  }

  private void mockServerCounts(Map<Integer, Map<String, Long>> partitionToServerCounts) {
    TableMetadataInfo info = mock(TableMetadataInfo.class);
    when(info.getPartitionToServerPrimaryKeyCountMap()).thenReturn(partitionToServerCounts);
    when(_reader.getAggregatedTableMetadataFromServer(anyString(), any(), anyList(), anyInt(), anyInt()))
        .thenReturn(info);
  }

  private long breachCount() {
    return _controllerMetrics.getMeteredTableValue(TABLE_NAME, ControllerMeter.PRIMARY_KEY_QUOTA_EXCEEDED).count();
  }

  @Test
  public void testDisabledWhenNotUpsert() {
    TableConfig tableConfig = upsertTableConfig(1000L, QuotaEnforcementMode.ENFORCED);
    when(tableConfig.isUpsertEnabled()).thenReturn(false);
    PrimaryKeyQuotaChecker checker = newChecker();
    assertEquals(checker.getEnforcementMode(tableConfig), QuotaEnforcementMode.DISABLED);
    // No server calls when disabled
    assertEquals(checker.checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.WITHIN_QUOTA);
    verify(_reader, never()).getAggregatedTableMetadataFromServer(anyString(), any(), anyList(), anyInt(), anyInt());
  }

  @Test
  public void testDisabledWhenNoCap() {
    TableConfig tableConfig = upsertTableConfig(null, QuotaEnforcementMode.ENFORCED);
    assertEquals(newChecker().getEnforcementMode(tableConfig), QuotaEnforcementMode.DISABLED);
  }

  @Test
  public void testDisabledWhenControllerFlagOff() {
    when(_controllerConf.getEnablePrimaryKeyQuotaCheck()).thenReturn(false);
    TableConfig tableConfig = upsertTableConfig(1000L, QuotaEnforcementMode.ENFORCED);
    assertEquals(newChecker().getEnforcementMode(tableConfig), QuotaEnforcementMode.DISABLED);
  }

  @Test
  public void testNullModeDefaultsToEnforced() {
    TableConfig tableConfig = upsertTableConfig(1000L, null);
    assertEquals(newChecker().getEnforcementMode(tableConfig), QuotaEnforcementMode.ENFORCED);
  }

  @Test
  public void testExplicitModeHonored() {
    TableConfig tableConfig = upsertTableConfig(1000L, QuotaEnforcementMode.SHADOW);
    assertEquals(newChecker().getEnforcementMode(tableConfig), QuotaEnforcementMode.SHADOW);
  }

  @Test
  public void testBreachEmitsMeterInShadowAndEnforced() {
    mockServerCounts(Collections.singletonMap(0, Collections.singletonMap("Server_0", 1500L)));
    for (QuotaEnforcementMode mode : new QuotaEnforcementMode[]{QuotaEnforcementMode.SHADOW,
        QuotaEnforcementMode.ENFORCED}) {
      TableConfig tableConfig = upsertTableConfig(1000L, mode);
      PrimaryKeyQuotaChecker checker = newChecker();
      long before = breachCount();
      assertEquals(checker.checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.EXCEEDED);
      assertEquals(breachCount(), before + 1, "breach meter should increment once in mode " + mode);
    }
  }

  @Test
  public void testDisabledMakesNoServerCallAndNoMeter() {
    TableConfig tableConfig = upsertTableConfig(1000L, QuotaEnforcementMode.DISABLED);
    PrimaryKeyQuotaChecker checker = newChecker();
    long before = breachCount();
    assertEquals(checker.checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.WITHIN_QUOTA);
    verify(_reader, never()).getAggregatedTableMetadataFromServer(anyString(), any(), anyList(), anyInt(), anyInt());
    assertEquals(breachCount(), before);
  }

  @Test
  public void testMaxAcrossReplicasNotSum() {
    // Two replicas of partition 0 at 60 and 70; sum (130) exceeds cap 100 but max (70) does not -> within quota
    assignSegments("Server_0", "Server_1");
    Map<String, Long> partition0 = new HashMap<>();
    partition0.put("Server_0", 60L);
    partition0.put("Server_1", 70L);
    mockServerCounts(Collections.singletonMap(0, partition0));
    TableConfig tableConfig = upsertTableConfig(100L, QuotaEnforcementMode.ENFORCED);
    assertEquals(newChecker().checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.WITHIN_QUOTA);
  }

  @Test
  public void testOnePartitionOverBreaches() {
    Map<Integer, Map<String, Long>> counts = new HashMap<>();
    counts.put(0, Collections.singletonMap("Server_0", 50L));
    counts.put(1, Collections.singletonMap("Server_1", 150L));
    mockServerCounts(counts);
    TableConfig tableConfig = upsertTableConfig(100L, QuotaEnforcementMode.ENFORCED);
    assertEquals(newChecker().checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.EXCEEDED);
  }

  @Test
  public void testExactlyAtCapIsWithinQuota() {
    mockServerCounts(Collections.singletonMap(0, Collections.singletonMap("Server_0", 100L)));
    TableConfig tableConfig = upsertTableConfig(100L, QuotaEnforcementMode.ENFORCED);
    assertEquals(newChecker().checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.WITHIN_QUOTA);
  }

  @Test
  public void testEmptyMapIsUndetermined() {
    mockServerCounts(Map.of());
    TableConfig tableConfig = upsertTableConfig(100L, QuotaEnforcementMode.ENFORCED);
    PrimaryKeyQuotaChecker checker = newChecker();
    assertEquals(checker.checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.UNDETERMINED);
    assertFalse(checker.isTablePrimaryKeyQuotaExceeded(tableConfig));
  }

  @Test
  public void testReaderExceptionIsUndetermined() {
    when(_reader.getAggregatedTableMetadataFromServer(anyString(), any(), anyList(), anyInt(), anyInt()))
        .thenThrow(new RuntimeException("servers unreachable"));
    TableConfig tableConfig = upsertTableConfig(100L, QuotaEnforcementMode.ENFORCED);
    PrimaryKeyQuotaChecker checker = newChecker();
    assertEquals(checker.checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.UNDETERMINED);
    assertFalse(checker.isTablePrimaryKeyQuotaExceeded(tableConfig));
  }

  @Test
  public void testPartialResponseWithoutBreachIsUndetermined() {
    // Server_1 hosts the table but did not respond; it may hold the partition that is over the cap
    assignSegments("Server_0", "Server_1");
    mockServerCounts(Map.of(0, Map.of("Server_0", 50L)));
    TableConfig tableConfig = upsertTableConfig(100L, QuotaEnforcementMode.ENFORCED);
    PrimaryKeyQuotaChecker checker = newChecker();
    long before = breachCount();
    assertEquals(checker.checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.UNDETERMINED);
    assertEquals(breachCount(), before);
  }

  @Test
  public void testPartialResponseWithBreachIsExceeded() {
    // A missing server can only hide a breach, so one seen in a partial response is real
    assignSegments("Server_0", "Server_1");
    mockServerCounts(Map.of(0, Map.of("Server_0", 150L)));
    TableConfig tableConfig = upsertTableConfig(100L, QuotaEnforcementMode.ENFORCED);
    assertEquals(newChecker().checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.EXCEEDED);
  }

  @DataProvider
  public Object[][] notLoadedSegmentStates() {
    // null: the segment is not in the ExternalView for the server yet, as right after a restart
    return new Object[][]{{null}, {SegmentStateModel.OFFLINE}, {SegmentStateModel.ERROR}};
  }

  @Test(dataProvider = "notLoadedSegmentStates")
  public void testServerStillLoadingIsUndetermined(@Nullable String segmentState) {
    // RF=1 server restarting: it already answers with the partitions loaded so far, so its count (50) can be below the
    // real one. It must not count as complete until all of its segments are loaded.
    ExternalView externalView = assignSegments("Server_0");
    externalView.getStateMap(segmentOf("Server_0")).remove("Server_0");
    if (segmentState != null) {
      externalView.setState(segmentOf("Server_0"), "Server_0", segmentState);
    }
    mockServerCounts(Map.of(0, Map.of("Server_0", 50L)));
    TableConfig tableConfig = upsertTableConfig(100L, QuotaEnforcementMode.ENFORCED);
    PrimaryKeyQuotaChecker checker = newChecker();
    long before = breachCount();
    assertEquals(checker.checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.UNDETERMINED);
    assertEquals(breachCount(), before);
  }

  @Test
  public void testServerStillLoadingWithBreachIsExceeded() {
    // A breach is visible even before the server finishes loading
    ExternalView externalView = assignSegments("Server_0");
    externalView.setState(segmentOf("Server_0"), "Server_0", SegmentStateModel.OFFLINE);
    mockServerCounts(Map.of(0, Map.of("Server_0", 150L)));
    TableConfig tableConfig = upsertTableConfig(100L, QuotaEnforcementMode.ENFORCED);
    assertEquals(newChecker().checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.EXCEEDED);
  }

  @Test
  public void testConsumingSegmentCountsAsLoaded() {
    ExternalView externalView = assignSegments("Server_0");
    externalView.setState(segmentOf("Server_0"), "Server_0", SegmentStateModel.CONSUMING);
    mockServerCounts(Map.of(0, Map.of("Server_0", 50L)));
    TableConfig tableConfig = upsertTableConfig(100L, QuotaEnforcementMode.ENFORCED);
    assertEquals(newChecker().checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.WITHIN_QUOTA);
  }

  @Test
  public void testMissingExternalViewIsUndetermined() {
    when(_helix.getTableExternalView(TABLE_NAME)).thenReturn(null);
    mockServerCounts(Map.of(0, Map.of("Server_0", 50L)));
    TableConfig tableConfig = upsertTableConfig(100L, QuotaEnforcementMode.ENFORCED);
    assertEquals(newChecker().checkPrimaryKeyQuota(tableConfig), PrimaryKeyQuotaChecker.Status.UNDETERMINED);
  }

  @Test
  public void testIsTablePrimaryKeyQuotaExceededWrapper() {
    mockServerCounts(Collections.singletonMap(0, Collections.singletonMap("Server_0", 1500L)));
    TableConfig tableConfig = upsertTableConfig(1000L, QuotaEnforcementMode.ENFORCED);
    assertTrue(newChecker().isTablePrimaryKeyQuotaExceeded(tableConfig));
  }
}
