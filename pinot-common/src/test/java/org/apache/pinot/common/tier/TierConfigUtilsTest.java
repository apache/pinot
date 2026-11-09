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
package org.apache.pinot.common.tier;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.assignment.InstancePartitionsUtils;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/// Tests the utility methods for TierConfig
public class TierConfigUtilsTest {

  @Test
  public void testShouldRelocateToTiers() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    Assert.assertFalse(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTierConfigList(null).build();
    Assert.assertFalse(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTierConfigList(List.of())
            .build();
    Assert.assertFalse(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTierConfigList(
        Lists.newArrayList(new TierConfig("myTier", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "10d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tag_OFFLINE", null, null))).build();
    Assert.assertTrue(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTierConfigList(
        Lists.newArrayList(
            new TierConfig("myTier", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, "10d", Lists.newArrayList("seg0", "seg1"),
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tag_OFFLINE", null, null))).build();
    Assert.assertTrue(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTierConfigList(
        Lists.newArrayList(new TierConfig("myTier", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null, null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tag_OFFLINE", null, null))).build();
    Assert.assertTrue(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("myTable").build();
    Assert.assertFalse(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("myTable").setTierConfigList(null).build();
    Assert.assertFalse(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("myTable").setTierConfigList(List.of())
            .build();
    Assert.assertFalse(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("myTable").setTierConfigList(
        Lists.newArrayList(new TierConfig("myTier", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "10d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tag_OFFLINE", null, null))).build();
    Assert.assertTrue(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig = new TableConfigBuilder(TableType.REALTIME).setTableName("myTable").setTierConfigList(
        Lists.newArrayList(
            new TierConfig("myTier", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, null, Lists.newArrayList("seg0", "seg1"),
                TierFactory.PINOT_SERVER_STORAGE_TYPE, "tag_OFFLINE", null, null))).build();
    Assert.assertTrue(TierConfigUtils.shouldRelocateToTiers(tableConfig));
  }

  /// Tests conversion from `TierConfig` to `Tier` in the `TierFactory`
  @Test
  public void testGetTier() {
    TierConfig tierConfig = new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
        TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null);
    Tier tier = TierFactory.getTier(tierConfig);
    Assert.assertEquals(tier.getName(), "tier1");
    Assert.assertTrue(tier.getSegmentSelector() instanceof TimeBasedTierSegmentSelector);
    Assert.assertEquals(tier.getSegmentSelector().getType(), TierFactory.TIME_SEGMENT_SELECTOR_TYPE);
    Assert.assertEquals(((TimeBasedTierSegmentSelector) tier.getSegmentSelector()).getSegmentAgeMillis(),
        30 * 24 * 60 * 60 * 1000L);
    Assert.assertTrue(tier.getStorage() instanceof PinotServerTierStorage);
    Assert.assertEquals(tier.getStorage().getType(), TierFactory.PINOT_SERVER_STORAGE_TYPE);
    Assert.assertEquals(((PinotServerTierStorage) tier.getStorage()).getServerTag(), "tier1_tag_OFFLINE");

    // With provided segments, the time base selector is overwritten by a fixed selector.
    tier = TierFactory.getTier(tierConfig, Set.of("segment1", "segment2"));
    Assert.assertEquals(tier.getName(), "tier1");
    Assert.assertTrue(tier.getSegmentSelector() instanceof FixedTierSegmentSelector);
    Assert.assertEquals(tier.getSegmentSelector().getType(), TierFactory.FIXED_SEGMENT_SELECTOR_TYPE);
    Assert.assertEquals(((FixedTierSegmentSelector) tier.getSegmentSelector()).getSegmentsToSelect(),
        Set.of("segment1", "segment2"));

    tierConfig = new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null,
        Lists.newArrayList("segment1", "segment2", "segment3"), TierFactory.PINOT_SERVER_STORAGE_TYPE,
        "tier1_tag_OFFLINE", null, null);
    tier = TierFactory.getTier(tierConfig);
    Assert.assertEquals(tier.getName(), "tier1");
    Assert.assertTrue(tier.getSegmentSelector() instanceof FixedTierSegmentSelector);
    Assert.assertEquals(tier.getSegmentSelector().getType(), TierFactory.FIXED_SEGMENT_SELECTOR_TYPE);
    Assert.assertEquals(((FixedTierSegmentSelector) tier.getSegmentSelector()).getSegmentsToSelect(),
        Set.of("segment1", "segment2", "segment3"));

    // With provided segments, the fixed selector can be overwritten with different set of segments.
    tier = TierFactory.getTier(tierConfig, Set.of("segment1a", "segment2b"));
    Assert.assertEquals(tier.getName(), "tier1");
    Assert.assertTrue(tier.getSegmentSelector() instanceof FixedTierSegmentSelector);
    Assert.assertEquals(tier.getSegmentSelector().getType(), TierFactory.FIXED_SEGMENT_SELECTOR_TYPE);
    Assert.assertEquals(((FixedTierSegmentSelector) tier.getSegmentSelector()).getSegmentsToSelect(),
        Set.of("segment1a", "segment2b"));

    tierConfig = new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null, null,
        TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null);
    tier = TierFactory.getTier(tierConfig);
    Assert.assertEquals(tier.getName(), "tier1");
    Assert.assertTrue(tier.getSegmentSelector() instanceof FixedTierSegmentSelector);
    Assert.assertEquals(tier.getSegmentSelector().getType(), TierFactory.FIXED_SEGMENT_SELECTOR_TYPE);
    Assert.assertTrue(((FixedTierSegmentSelector) tier.getSegmentSelector()).getSegmentsToSelect().isEmpty());

    tierConfig =
        new TierConfig("tier1", "unknown", "30d", null, TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE",
            null, null);
    try {
      TierFactory.getTier(tierConfig);
      Assert.fail("Should have failed due to unsupported segmentSelectorType");
    } catch (IllegalStateException e) {
      // expected
    }

    tierConfig =
        new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null, "unknown", "tier1_tag_OFFLINE",
            null, null);
    try {
      TierFactory.getTier(tierConfig);
      Assert.fail("Should've failed due to unsupported storageType");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  /// Tests the custom comparator for tiers
  @Test
  public void testTierComparator() {
    Comparator<Tier> tierComparator = TierConfigUtils.getTierComparator();

    Tier tier1 = new Tier("tier1", new TimeBasedTierSegmentSelector("30d"), new PinotServerTierStorage(
            "tag_OFFLINE", null, null));
    Tier tier2 = new Tier("tier2", new TimeBasedTierSegmentSelector("1000d"),
            new PinotServerTierStorage("tag_OFFLINE", null, null));
    Tier tier3 = new Tier("tier3", new TimeBasedTierSegmentSelector("24h"),
            new PinotServerTierStorage("tag_OFFLINE", null, null));
    Tier tier4 = new Tier("tier4", new TimeBasedTierSegmentSelector("10m"),
            new PinotServerTierStorage("tag_OFFLINE", null, null));
    Tier tier5 = new Tier("tier5", new TimeBasedTierSegmentSelector("1d"),
            new PinotServerTierStorage("tag_OFFLINE", null, null));

    Tier tier6 = new Tier("tier6", new FixedTierSegmentSelector(Set.of("seg0")),
        new PinotServerTierStorage("tag_OFFLINE", null, null));
    Tier tier7 = new Tier("tier6", new FixedTierSegmentSelector(Set.of("seg1")),
        new PinotServerTierStorage("tag_OFFLINE", null, null));

    Assert.assertEquals(tierComparator.compare(tier1, tier2), 1);
    Assert.assertEquals(tierComparator.compare(tier1, tier3), -1);
    Assert.assertEquals(tierComparator.compare(tier1, tier4), -1);
    Assert.assertEquals(tierComparator.compare(tier4, tier2), 1);
    Assert.assertEquals(tierComparator.compare(tier3, tier2), 1);
    Assert.assertEquals(tierComparator.compare(tier3, tier4), -1);
    Assert.assertEquals(tierComparator.compare(tier1, tier1), 0);
    Assert.assertEquals(tierComparator.compare(tier3, tier5), 0);

    Assert.assertEquals(tierComparator.compare(tier6, tier7), 0);
    Assert.assertEquals(tierComparator.compare(tier6, tier5), -1);
    Assert.assertEquals(tierComparator.compare(tier4, tier7), 1);
  }

  @Test
  public void testGetDataDirForTier() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    String dataDir = TierConfigUtils.getDataDirForTier(tableConfig, "tier1");
    Assert.assertNull(dataDir);
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTierConfigList(Lists
        .newArrayList(new TierConfig("myTier", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "10d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tag_OFFLINE", null, null))).build();
    dataDir = TierConfigUtils.getDataDirForTier(tableConfig, "tier1");
    Assert.assertNull(dataDir);
    dataDir = TierConfigUtils.getDataDirForTier(tableConfig, "myTier");
    Assert.assertNull(dataDir);
    // Provide instance tierConfigs for the tier.
    Map<String, Map<String, String>> instanceTierConfigs = new HashMap<>();
    Map<String, String> tierCfgMap = new HashMap<>();
    tierCfgMap.put("datadir", "/abc/xyz");
    instanceTierConfigs.put("myTier", tierCfgMap);
    dataDir = TierConfigUtils.getDataDirForTier(tableConfig, "myTier", instanceTierConfigs);
    Assert.assertEquals(dataDir, "/abc/xyz");
    // Table tierConfigs overwrite those from instance tierConfigs.
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTierConfigList(Lists
        .newArrayList(new TierConfig("myTier", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "10d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, "tag_OFFLINE", null,
            Map.of("dataDir", "/foo/bar")))).build();
    dataDir = TierConfigUtils.getDataDirForTier(tableConfig, "myTier", instanceTierConfigs);
    Assert.assertEquals(dataDir, "/foo/bar");
  }

  /// A tier whose instance partitions have been assigned and persisted must be placed on exactly those instances.
  /// Recomputing a default placement here would send segments added after the assignment to different instances than
  /// the ones the assignment already placed the existing segments on.
  @Test
  public void testGetTieredInstancePartitionsUsesPersistedAssignment() {
    String rawTableName = "myTable";
    String tableNameWithType = "myTable_OFFLINE";
    String tierName = "myTier";
    String serverTag = "tierTag_OFFLINE";
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).build();
    Tier tier = TierFactory.getTier(
        new TierConfig(tierName, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "10d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, serverTag, null, null));

    // Two replica-groups over two partitions, which is what instance assignment persists for the tier and which no
    // default placement can produce.
    InstancePartitions persisted = new InstancePartitions(
        InstancePartitionsUtils.getInstancePartitionsNameForTier(tableNameWithType, tierName));
    persisted.setInstances(0, 0, List.of("tierServer_0"));
    persisted.setInstances(0, 1, List.of("tierServer_2"));
    persisted.setInstances(1, 0, List.of("tierServer_1"));
    persisted.setInstances(1, 1, List.of("tierServer_3"));

    HelixManager helixManager =
        mockHelixManager(serverTag, List.of("tierServer_0", "tierServer_1", "tierServer_2", "tierServer_3"), persisted);

    InstancePartitions instancePartitions =
        TierConfigUtils.getTieredInstancePartitions(tableConfig, tier, helixManager);
    Assert.assertEquals(instancePartitions.getNumPartitions(), 2);
    Assert.assertEquals(instancePartitions.getNumReplicaGroups(), 2);
    Assert.assertEquals(instancePartitions.getInstances(0, 0), List.of("tierServer_0"));
    Assert.assertEquals(instancePartitions.getInstances(1, 1), List.of("tierServer_3"));
    Assert.assertEquals(instancePartitions.getInstancePartitionsName(), persisted.getInstancePartitionsName());

    // The same assignment must be reached through the per-segment entry point, which is what segment assignment uses.
    SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata("mySegment");
    segmentZKMetadata.setEndTime(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30));
    segmentZKMetadata.setTimeUnit(TimeUnit.MILLISECONDS);
    InstancePartitions forSegment =
        TierConfigUtils.getTieredInstancePartitionsForSegment(tableConfig, segmentZKMetadata, List.of(tier),
            helixManager);
    Assert.assertNotNull(forSegment);
    Assert.assertEquals(forSegment.getPartitionToInstancesMap(), persisted.getPartitionToInstancesMap());
  }

  /// Without a persisted assignment the tier still falls back to a default placement over its tagged instances.
  @Test
  public void testGetTieredInstancePartitionsFallsBackToDefault() {
    String rawTableName = "myTable";
    String tierName = "myTier";
    String serverTag = "tierTag_OFFLINE";
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(rawTableName).build();
    Tier tier = TierFactory.getTier(
        new TierConfig(tierName, TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "10d", null,
            TierFactory.PINOT_SERVER_STORAGE_TYPE, serverTag, null, null));

    HelixManager helixManager =
        mockHelixManager(serverTag, List.of("tierServer_0", "tierServer_1", "tierServer_2", "tierServer_3"), null);

    InstancePartitions instancePartitions =
        TierConfigUtils.getTieredInstancePartitions(tableConfig, tier, helixManager);
    Assert.assertEquals(instancePartitions.getNumPartitions(), 1);
    Assert.assertEquals(instancePartitions.getNumReplicaGroups(), 1);
    Assert.assertEquals(Set.copyOf(instancePartitions.getInstances(0, 0)),
        Set.of("tierServer_0", "tierServer_1", "tierServer_2", "tierServer_3"));
  }

  /// Mocks a Helix manager holding the given tagged instances, and the given instance partitions if not null.
  private HelixManager mockHelixManager(String serverTag, List<String> taggedInstances,
      InstancePartitions instancePartitions) {
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    for (String instance : taggedInstances) {
      InstanceConfig instanceConfig = new InstanceConfig(instance);
      instanceConfig.addTag(serverTag);
      instanceConfigs.add(instanceConfig);
    }
    HelixDataAccessor helixDataAccessor = mock(HelixDataAccessor.class);
    when(helixDataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder("testCluster"));
    when(helixDataAccessor.getChildValues(any(PropertyKey.class), anyBoolean())).thenAnswer(
        invocation -> instanceConfigs);

    @SuppressWarnings("unchecked")
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    String path = instancePartitions != null ? ZKMetadataProvider.constructPropertyStorePathForInstancePartitions(
        instancePartitions.getInstancePartitionsName()) : null;
    when(propertyStore.get(anyString(), eq(null), eq(AccessOption.PERSISTENT))).thenAnswer(
        invocation -> invocation.getArgument(0, String.class).equals(path) ? instancePartitions.toZNRecord() : null);

    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.getHelixDataAccessor()).thenReturn(helixDataAccessor);
    when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);
    return helixManager;
  }
}
