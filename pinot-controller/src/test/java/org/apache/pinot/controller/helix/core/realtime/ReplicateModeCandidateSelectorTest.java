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
package org.apache.pinot.controller.helix.core.realtime;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.constants.InstanceConstants.InstanceOperation;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.PartitionAssignmentUtils;
import org.apache.pinot.common.assignment.ReplicateModeAssignmentUtils;
import org.apache.pinot.common.restlet.resources.DiskUsageInfo;
import org.apache.pinot.common.utils.helix.HelixHelper;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeCandidateSelector.CandidateContext;
import org.apache.pinot.controller.helix.core.realtime.ReplicateModeCandidateSelector.Selection;
import org.apache.pinot.controller.validation.ResourceUtilizationInfo;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig.PartitionSelector;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Verifies read-only follower eligibility, deterministic selection and cached disk admission.
@Test(singleThreaded = true)
public class ReplicateModeCandidateSelectorTest {
  private static final String TABLE = "testTable_REALTIME";
  private static final String SERVER_A = "Server_a_1234";
  private static final String SERVER_B = "Server_b_1234";
  private static final String SERVER_C = "Server_c_1234";
  private static final String SERVER_TAG = "tenant_REALTIME";
  private static final List<String> SERVERS = List.of(SERVER_A, SERVER_B, SERVER_C);
  private static final List<String> SEGMENTS = List.of("segment0", "segment1");
  private static final long NOW_MS = 1_000_000L;

  private IdealState _idealState;
  private InstancePartitions _instancePartitions;
  private Map<String, InstanceConfig> _instanceConfigs;
  private Set<String> _liveInstances;
  private Map<String, DiskUsageInfo> _previousDiskUsage;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setUp()
      throws ReflectiveOperationException {
    _idealState = new IdealState(TABLE);
    _idealState.enable(true);
    ReplicateModeAssignmentUtils.setAssignment(_idealState, 0, SERVER_A, SERVERS);
    _instancePartitions = new InstancePartitions("testTable_CONSUMING");
    _instancePartitions.setInstances(0, 0, SERVERS);
    _instanceConfigs = new HashMap<>();
    for (String server : SERVERS) {
      InstanceConfig config = new InstanceConfig(server);
      config.addTag(SERVER_TAG);
      _instanceConfigs.put(server, config);
    }
    _liveInstances = new HashSet<>(SERVERS);
    Field cache = ResourceUtilizationInfo.class.getDeclaredField("_instanceDiskUsageInfo");
    cache.setAccessible(true);
    _previousDiskUsage = (Map<String, DiskUsageInfo>) cache.get(null);
    ResourceUtilizationInfo.setDiskUsageInfo(Map.of());
  }

  @AfterMethod
  public void tearDown() {
    ResourceUtilizationInfo.setDiskUsageInfo(_previousDiskUsage);
  }

  @Test
  public void testSelectsLeastLoadedFollowerWithoutChangingInputs() {
    IdealState before = HelixHelper.cloneIdealState(_idealState);
    String placementBefore = _instancePartitions.toJsonString();
    Map<String, Integer> counts = Map.of(SERVER_B, 5, SERVER_C, 1);

    Selection selection = selector().select(context(table()), 0, SEGMENTS, Set.of(SERVER_A), counts, null, NOW_MS);

    assertEquals(selection.eligibleServers(), List.of(SERVER_B, SERVER_C));
    assertEquals(selection.selectedServer(), SERVER_C);
    assertEquals(selection.rejectedServers().keySet(), Set.of(SERVER_A));
    assertEquals(_idealState, before);
    assertEquals(_instancePartitions.toJsonString(), placementBefore);
    assertEquals(counts, Map.of(SERVER_B, 5, SERVER_C, 1));
    assertThrows(UnsupportedOperationException.class, () -> selection.eligibleServers().add(SERVER_A));
    assertThrows(UnsupportedOperationException.class, () -> selection.rejectedServers().put(SERVER_B, "rejected"));
  }

  @Test
  public void testPreferredServerMustStillBeEligible() {
    Map<String, Integer> counts = Map.of(SERVER_B, 100, SERVER_C, 0);
    Selection preferred = selector().select(context(table()), 0, SEGMENTS, Set.of(SERVER_A), counts, SERVER_B,
        NOW_MS);
    assertEquals(preferred.selectedServer(), SERVER_B);

    Selection excluded = selector().select(context(table()), 0, SEGMENTS, Set.of(SERVER_A, SERVER_B), counts,
        SERVER_B, NOW_MS);
    assertEquals(excluded.selectedServer(), SERVER_C);
  }

  @Test
  public void testTieBreakUsesEncodedPartitionIdAndSortedEligibleServers() {
    ReplicateModeAssignmentUtils.setAssignment(_idealState, 10_000, SERVER_A, List.of(SERVER_C, SERVER_A, SERVER_B));
    ReplicateModeAssignmentUtils.setAssignment(_idealState, 10_001, SERVER_A, List.of(SERVER_C, SERVER_A, SERVER_B));

    Selection even = selector().select(context(table()), 10_000, SEGMENTS, Set.of(SERVER_A), Map.of(), null, NOW_MS);
    Selection odd = selector().select(context(table()), 10_001, SEGMENTS, Set.of(SERVER_A), Map.of(), null, NOW_MS);

    assertEquals(even.selectedServer(), SERVER_B);
    assertEquals(odd.selectedServer(), SERVER_C);
    assertEquals(odd.eligibleServers(), List.of(SERVER_B, SERVER_C));
  }

  @Test
  public void testNoRecordedOrEligibleCandidates() {
    Selection absent = selector().select(context(table()), 99, SEGMENTS, Set.of(), Map.of(), null, NOW_MS);
    assertTrue(absent.eligibleServers().isEmpty());
    assertTrue(absent.rejectedServers().isEmpty());
    assertNull(absent.selectedServer());

    _liveInstances.clear();
    Selection unavailable = selector().select(context(table()), 0, SEGMENTS, Set.of(SERVER_A), Map.of(), null,
        NOW_MS);
    assertTrue(unavailable.eligibleServers().isEmpty());
    assertEquals(unavailable.rejectedServers().keySet(), Set.copyOf(SERVERS));
    assertNull(unavailable.selectedServer());
  }

  @DataProvider
  public Object[][] eligibilityFailures() {
    return new Object[][]{{"notLive"}, {"missingConfig"}, {"disabled"}, {"wrongTag"}, {"queriesDisabled"},
        {"shutdown"}, {"outsidePlacement"}, {"partitionDisabled"}, {"excluded"}};
  }

  @Test(dataProvider = "eligibilityFailures")
  public void testExcludesUnavailableCandidates(String failure) {
    Set<String> excluded = Set.of(SERVER_A);
    InstanceConfig config = _instanceConfigs.get(SERVER_B);
    switch (failure) {
      case "notLive" -> _liveInstances.remove(SERVER_B);
      case "missingConfig" -> _instanceConfigs.remove(SERVER_B);
      case "disabled" -> config.setInstanceOperation(InstanceOperation.DISABLE);
      case "wrongTag" -> config.removeTag(SERVER_TAG);
      case "queriesDisabled" -> config.getRecord().setBooleanField(Helix.QUERIES_DISABLED, true);
      case "shutdown" -> config.getRecord().setBooleanField(Helix.IS_SHUTDOWN_IN_PROGRESS, true);
      case "outsidePlacement" -> _instancePartitions.setInstances(0, 0, List.of(SERVER_A, SERVER_C));
      case "partitionDisabled" -> config.setInstanceEnabledForPartition(TABLE, SEGMENTS.get(1), false);
      case "excluded" -> excluded = Set.of(SERVER_A, SERVER_B);
      default -> throw new IllegalArgumentException(failure);
    }

    Selection selection = selector().select(context(table()), 0, SEGMENTS, excluded, Map.of(), null, NOW_MS);

    assertEquals(selection.eligibleServers(), List.of(SERVER_C));
    assertEquals(selection.selectedServer(), SERVER_C);
    assertEquals(selection.rejectedServers().keySet(), Set.of(SERVER_A, SERVER_B));
    assertFalse(selection.rejectedServers().get(SERVER_B).isEmpty());
  }

  @Test
  public void testDisabledSegmentInAnotherTableDoesNotExcludeCandidate() {
    _instanceConfigs.get(SERVER_B).setInstanceEnabledForPartition("anotherTable_REALTIME", SEGMENTS.get(0), false);

    Selection selection = selector().select(context(table()), 0, SEGMENTS, Set.of(SERVER_A),
        Map.of(SERVER_B, 0, SERVER_C, 1), null, NOW_MS);

    assertEquals(selection.eligibleServers(), List.of(SERVER_B, SERVER_C));
    assertEquals(selection.selectedServer(), SERVER_B);
  }

  @Test
  public void testTenantConsumingTagOverride() {
    String consumingTag = "ingestion_REALTIME";
    TableConfig tableConfig = tableBuilder().setTagOverrideConfig(new TagOverrideConfig(consumingTag, null)).build();
    _instanceConfigs.get(SERVER_B).addTag(consumingTag);

    Selection selection = selector().select(context(tableConfig), 0, SEGMENTS, Set.of(SERVER_A), Map.of(), null,
        NOW_MS);

    assertEquals(selection.eligibleServers(), List.of(SERVER_B));
  }

  @Test
  public void testExplicitConsumingTagTakesPrecedenceOverTenantTag() {
    String customTag = "custom_REALTIME";
    TableConfig tableConfig = tableBuilder().setTagOverrideConfig(new TagOverrideConfig("ingestion_REALTIME", null))
        .setInstanceAssignmentConfigMap(Map.of(InstancePartitionsType.CONSUMING.name(), assignment(customTag, null)))
        .build();
    _instanceConfigs.get(SERVER_B).addTag(customTag);

    Selection selection = selector().select(context(tableConfig), 0, SEGMENTS, Set.of(SERVER_A), Map.of(), null,
        NOW_MS);

    assertEquals(selection.eligibleServers(), List.of(SERVER_B));
  }

  @Test
  public void testPreconfiguredPlacementDoesNotRequireTenantTag() {
    TableConfig tableConfig = tableBuilder()
        .setInstancePartitionsMap(Map.of(InstancePartitionsType.CONSUMING, "shared_CONSUMING")).build();
    _instanceConfigs.values().forEach(config -> config.removeTag(SERVER_TAG));

    Selection selection = selector().select(context(tableConfig), 0, SEGMENTS, Set.of(SERVER_A), Map.of(), null,
        NOW_MS);

    assertEquals(selection.eligibleServers(), List.of(SERVER_B, SERVER_C));
  }

  @Test
  public void testMirrorServerSetStillRequiresConfiguredTag() {
    String mirrorTag = "mirror_REALTIME";
    TableConfig tableConfig = tableBuilder()
        .setInstancePartitionsMap(Map.of(InstancePartitionsType.CONSUMING, "shared_CONSUMING"))
        .setInstanceAssignmentConfigMap(Map.of(InstancePartitionsType.CONSUMING.name(),
            assignment(mirrorTag, PartitionSelector.MIRROR_SERVER_SET_PARTITION_SELECTOR.name())))
        .build();
    _instanceConfigs.get(SERVER_B).addTag(mirrorTag);

    Selection selection = selector().select(context(tableConfig), 0, SEGMENTS, Set.of(SERVER_A), Map.of(), null,
        NOW_MS);

    assertEquals(selection.eligibleServers(), List.of(SERVER_B));
  }

  @Test
  public void testPinnedAssignmentRemainsAuthoritativeUntilRebalance() {
    PartitionAssignmentUtils.setInstances(_idealState, 0, List.of(SERVER_A, SERVER_B));
    _instancePartitions.setInstances(0, 0, List.of(SERVER_C));
    TableConfig pinned = tableBuilder().setRoutingConfig(
        new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, null)).build();

    Selection pinnedSelection = selector().select(context(pinned), 0, SEGMENTS, Set.of(SERVER_A), Map.of(), null,
        NOW_MS);
    Selection unpinnedSelection = selector().select(context(table()), 0, SEGMENTS, Set.of(SERVER_A), Map.of(), null,
        NOW_MS);

    assertEquals(pinnedSelection.eligibleServers(), List.of(SERVER_B));
    assertEquals(unpinnedSelection.eligibleServers(), List.of(SERVER_C));
  }

  @DataProvider
  public Object[][] diskSamples() {
    return new Object[][]{{100L, 81L, NOW_MS, true}, {100L, 80L, NOW_MS, false},
        {100L, 99L, NOW_MS - 10_001L, false}, {0L, 99L, NOW_MS, false}, {100L, -1L, NOW_MS, false},
        {100L, 99L, -1L, false}, {100L, 99L, NOW_MS + 1L, false}};
  }

  @Test(dataProvider = "diskSamples")
  public void testOnlyFreshValidDiskBreachRejectsCandidate(long total, long used, long lastUpdated,
      boolean rejected) {
    ResourceUtilizationInfo.setDiskUsageInfo(Map.of(SERVER_B,
        new DiskUsageInfo(SERVER_B, "/", total, used, lastUpdated)));
    ReplicateModeCandidateSelector selector = new ReplicateModeCandidateSelector(diskConfig(true, true, false));

    Selection selection = selector.select(context(table()), 0, SEGMENTS, Set.of(SERVER_A), Map.of(), null, NOW_MS);

    assertEquals(selection.eligibleServers().contains(SERVER_B), !rejected);
    assertTrue(selection.eligibleServers().contains(SERVER_C));
    assertEquals(selection.rejectedServers().containsKey(SERVER_B), rejected);
  }

  @DataProvider
  public Object[][] diskCheckGates() {
    return new Object[][]{{false, true, true, false}, {true, false, false, false}, {true, true, false, true},
        {true, false, true, true}};
  }

  @Test(dataProvider = "diskCheckGates")
  public void testDiskVetoHonorsExistingCheckerFlags(boolean resourceEnabled, boolean allCheckersEnabled,
      boolean diskCheckerEnabled, boolean rejected) {
    ResourceUtilizationInfo.setDiskUsageInfo(Map.of(SERVER_B,
        new DiskUsageInfo(SERVER_B, "/", 100L, 99L, NOW_MS)));
    ReplicateModeCandidateSelector selector = new ReplicateModeCandidateSelector(
        diskConfig(resourceEnabled, allCheckersEnabled, diskCheckerEnabled));

    Selection selection = selector.select(context(table()), 0, SEGMENTS, Set.of(SERVER_A), Map.of(), null, NOW_MS);

    assertEquals(selection.rejectedServers().containsKey(SERVER_B), rejected);
    assertTrue(selection.eligibleServers().contains(SERVER_C));
  }

  @Test
  public void testUnknownDiskSampleDoesNotBlockSelection() {
    ResourceUtilizationInfo.setDiskUsageInfo(Map.of(SERVER_B, new DiskUsageInfo(SERVER_B)));
    ReplicateModeCandidateSelector selector = new ReplicateModeCandidateSelector(diskConfig(true, true, false));

    Selection selection = selector.select(context(table()), 0, SEGMENTS, Set.of(SERVER_A), Map.of(), null, NOW_MS);

    assertEquals(selection.eligibleServers(), List.of(SERVER_B, SERVER_C));
  }

  private ReplicateModeCandidateSelector selector() {
    return new ReplicateModeCandidateSelector(new ControllerConf());
  }

  private CandidateContext context(TableConfig tableConfig) {
    return new CandidateContext(tableConfig, _idealState, _instancePartitions, _instanceConfigs, _liveInstances);
  }

  private static TableConfig table() {
    return tableBuilder().build();
  }

  private static TableConfigBuilder tableBuilder() {
    return new TableConfigBuilder(TableType.REALTIME).setTableName("testTable").setServerTenant("tenant");
  }

  private static InstanceAssignmentConfig assignment(String tag, String partitionSelector) {
    return new InstanceAssignmentConfig(new InstanceTagPoolConfig(tag, false, 0, null), null,
        new InstanceReplicaGroupPartitionConfig(false, 0, 0, 0, 0, 0, false, null), partitionSelector, false);
  }

  private static ControllerConf diskConfig(boolean resourceEnabled, boolean allCheckersEnabled,
      boolean diskCheckerEnabled) {
    return new ControllerConf(Map.of(
        ControllerConf.ENABLE_RESOURCE_UTILIZATION_CHECK, resourceEnabled,
        ControllerConf.ENABLE_ALL_RESOURCE_UTILIZATION_CHECKERS, allCheckersEnabled,
        ControllerConf.ENABLE_DISK_UTILIZATION_CHECKER, diskCheckerEnabled,
        ControllerConf.RESOURCE_UTILIZATION_CHECKER_FREQUENCY, 10L,
        ControllerConf.DISK_UTILIZATION_THRESHOLD, 0.8));
  }
}
