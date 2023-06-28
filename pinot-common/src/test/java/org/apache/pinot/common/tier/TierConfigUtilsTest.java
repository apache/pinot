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
import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.utils.config.TierConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests the utility methods for TierConfig
 */
public class TierConfigUtilsTest {

  @Test
  public void testShouldRelocateToTiers() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    Assert.assertFalse(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTierConfigList(null).build();
    Assert.assertFalse(TierConfigUtils.shouldRelocateToTiers(tableConfig));

    tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setTierConfigList(Collections.emptyList())
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
        new TableConfigBuilder(TableType.REALTIME).setTableName("myTable").setTierConfigList(Collections.emptyList())
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

  /**
   * Tests conversion from {@code TierConfig} to {@code Tier} in the {@code TierFactory}
   */
  @Test
  public void testGetTier() {
    TierConfig tierConfig = new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
        TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE", null, null);
    Tier tier = TierFactory.getTier(tierConfig, null);
    Assert.assertEquals(tier.getName(), "tier1");
    Assert.assertTrue(tier.getSegmentSelector() instanceof TimeBasedTierSegmentSelector);
    Assert.assertEquals(tier.getSegmentSelector().getType(), TierFactory.TIME_SEGMENT_SELECTOR_TYPE);
    Assert.assertEquals(((TimeBasedTierSegmentSelector) tier.getSegmentSelector()).getSegmentAgeMillis(),
        30 * 24 * 60 * 60 * 1000L);
    Assert.assertTrue(tier.getStorage() instanceof PinotServerTierStorage);
    Assert.assertEquals(tier.getStorage().getType(), TierFactory.PINOT_SERVER_STORAGE_TYPE);
    Assert.assertEquals(((PinotServerTierStorage) tier.getStorage()).getServerTag(), "tier1_tag_OFFLINE");

    tierConfig = new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null,
        Lists.newArrayList("segment1", "segment2", "segment3"), TierFactory.PINOT_SERVER_STORAGE_TYPE,
        "tier1_tag_OFFLINE", null, null);
    tier = TierFactory.getTier(tierConfig, null);
    Assert.assertEquals(tier.getName(), "tier1");
    Assert.assertTrue(tier.getSegmentSelector() instanceof FixedTierSegmentSelector);
    Assert.assertEquals(tier.getSegmentSelector().getType(), TierFactory.FIXED_SEGMENT_SELECTOR_TYPE);
    Assert.assertEquals(((FixedTierSegmentSelector) tier.getSegmentSelector()).getSegmentsToSelect(),
        Sets.newHashSet("segment1", "segment2", "segment3"));

    tierConfig = new TierConfig("tier1", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null,
        null, TierFactory.PINOT_SERVER_STORAGE_TYPE,
        "tier1_tag_OFFLINE", null, null);
    tier = TierFactory.getTier(tierConfig, null);
    Assert.assertEquals(tier.getName(), "tier1");
    Assert.assertTrue(tier.getSegmentSelector() instanceof FixedTierSegmentSelector);
    Assert.assertEquals(tier.getSegmentSelector().getType(), TierFactory.FIXED_SEGMENT_SELECTOR_TYPE);
    Assert.assertTrue(((FixedTierSegmentSelector) tier.getSegmentSelector()).getSegmentsToSelect().isEmpty());

    tierConfig =
        new TierConfig("tier1", "unknown", "30d", null, TierFactory.PINOT_SERVER_STORAGE_TYPE, "tier1_tag_OFFLINE",
            null, null);
    try {
      TierFactory.getTier(tierConfig, null);
      Assert.fail("Should have failed due to unsupported segmentSelectorType");
    } catch (IllegalStateException e) {
      // expected
    }

    tierConfig =
        new TierConfig("tier1", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null, "unknown", "tier1_tag_OFFLINE",
            null, null);
    try {
      TierFactory.getTier(tierConfig, null);
      Assert.fail("Should've failed due to unsupported storageType");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  /**
   * Tests the custom comparator for tiers
   */
  @Test
  public void testTierComparator() {
    Comparator<Tier> tierComparator = TierConfigUtils.getTierComparator();

    Tier tier1 =
        new Tier("tier1", new TimeBasedTierSegmentSelector(null, "30d"), new PinotServerTierStorage(
            "tag_OFFLINE", null, null));
    Tier tier2 =
        new Tier("tier2", new TimeBasedTierSegmentSelector(null, "1000d"),
            new PinotServerTierStorage("tag_OFFLINE", null, null));
    Tier tier3 =
        new Tier("tier3", new TimeBasedTierSegmentSelector(null, "24h"),
            new PinotServerTierStorage("tag_OFFLINE", null, null));
    Tier tier4 =
        new Tier("tier4", new TimeBasedTierSegmentSelector(null, "10m"),
            new PinotServerTierStorage("tag_OFFLINE", null, null));
    Tier tier5 =
        new Tier("tier5", new TimeBasedTierSegmentSelector(null, "1d"),
            new PinotServerTierStorage("tag_OFFLINE", null, null));

    Tier tier6 = new Tier("tier6", new FixedTierSegmentSelector(null, Sets.newHashSet("seg0")),
        new PinotServerTierStorage("tag_OFFLINE", null, null));
    Tier tier7 = new Tier("tier6", new FixedTierSegmentSelector(null, Sets.newHashSet("seg1")),
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
            Collections.singletonMap("dataDir", "/foo/bar")))).build();
    dataDir = TierConfigUtils.getDataDirForTier(tableConfig, "myTier", instanceTierConfigs);
    Assert.assertEquals(dataDir, "/foo/bar");
  }
}
