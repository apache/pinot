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
package org.apache.pinot.spi.utils;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for helper methods in {@link IngestionConfigUtils}
 */
public class IngestionConfigUtilsTest {

  @Test
  public void testGetStreamConfigMap() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    try {
      IngestionConfigUtils.getStreamConfigMap(tableConfig);
      Assert.fail("Should fail for OFFLINE table");
    } catch (IllegalStateException e) {
      // expected
    }

    tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName("myTable").setTimeColumnName("timeColumn").build();

    // get from ingestion config (when not present in indexing config)
    Map<String, String> streamConfigMap = Collections.singletonMap("streamType", "kafka");
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setStreamIngestionConfig(new StreamIngestionConfig(Collections.singletonList(streamConfigMap)));
    tableConfig.setIngestionConfig(ingestionConfig);
    Map<String, String> actualStreamConfigsMap = IngestionConfigUtils.getStreamConfigMap(tableConfig);
    Assert.assertEquals(actualStreamConfigsMap.size(), 1);
    Assert.assertEquals(actualStreamConfigsMap.get("streamType"), "kafka");

    // get from ingestion config (even if present in indexing config)
    Map<String, String> deprecatedStreamConfigMap = new HashMap<>();
    deprecatedStreamConfigMap.put("streamType", "foo");
    deprecatedStreamConfigMap.put("customProp", "foo");
    IndexingConfig indexingConfig = new IndexingConfig();
    indexingConfig.setStreamConfigs(deprecatedStreamConfigMap);
    tableConfig.setIndexingConfig(indexingConfig);
    actualStreamConfigsMap = IngestionConfigUtils.getStreamConfigMap(tableConfig);
    Assert.assertEquals(actualStreamConfigsMap.size(), 1);
    Assert.assertEquals(actualStreamConfigsMap.get("streamType"), "kafka");

    // fail if multiple found
    ingestionConfig.setStreamIngestionConfig(
        new StreamIngestionConfig(Arrays.asList(streamConfigMap, deprecatedStreamConfigMap)));
    try {
      IngestionConfigUtils.getStreamConfigMap(tableConfig);
      Assert.fail("Should fail for multiple stream configs");
    } catch (IllegalStateException e) {
      // expected
    }

    // get from indexing config
    tableConfig.setIngestionConfig(null);
    actualStreamConfigsMap = IngestionConfigUtils.getStreamConfigMap(tableConfig);
    Assert.assertEquals(actualStreamConfigsMap.size(), 2);
    Assert.assertEquals(actualStreamConfigsMap.get("streamType"), "foo");

    // fail if found nowhere
    tableConfig.setIndexingConfig(null);
    try {
      IngestionConfigUtils.getStreamConfigMap(tableConfig);
      Assert.fail("Should fail for no stream config found");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test
  public void testGetPushFrequency() {
    // get from ingestion config, when not present in segmentsConfig
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "APPEND", "HOURLY"));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setIngestionConfig(ingestionConfig).build();
    Assert.assertEquals(IngestionConfigUtils.getBatchSegmentIngestionFrequency(tableConfig), "HOURLY");

    // get from ingestion config, even if present in segmentsConfig
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    segmentsValidationAndRetentionConfig.setSegmentPushFrequency("DAILY");
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    Assert.assertEquals(IngestionConfigUtils.getBatchSegmentIngestionFrequency(tableConfig), "HOURLY");

    // get from segmentsConfig
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    Assert.assertEquals(IngestionConfigUtils.getBatchSegmentIngestionFrequency(tableConfig), "DAILY");

    // present nowhere
    segmentsValidationAndRetentionConfig.setSegmentPushFrequency(null);
    Assert.assertNull(IngestionConfigUtils.getBatchSegmentIngestionFrequency(tableConfig));
  }

  @Test
  public void testGetPushType() {
    // get from ingestion config, when not present in segmentsConfig
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "APPEND", "HOURLY"));
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").setIngestionConfig(ingestionConfig).build();
    Assert.assertEquals(IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig), "APPEND");

    // get from ingestion config, even if present in segmentsConfig
    SegmentsValidationAndRetentionConfig segmentsValidationAndRetentionConfig =
        new SegmentsValidationAndRetentionConfig();
    segmentsValidationAndRetentionConfig.setSegmentPushType("REFRESH");
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    Assert.assertEquals(IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig), "APPEND");

    // get from segmentsConfig
    tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setValidationConfig(segmentsValidationAndRetentionConfig);
    Assert.assertEquals(IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig), "REFRESH");

    // present nowhere, then should return APPEND which is the default
    segmentsValidationAndRetentionConfig.setSegmentPushType(null);
    Assert.assertEquals(IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig), "APPEND");
  }

  @Test
  public void testGetConfigMapWithPrefix() {
    Map<String, String> testMap = ImmutableMap.of("k1", "v1", "k1.k2", "v2", "k1.k3", "v3", "k4", "v4");
    Assert.assertEquals(2, IngestionConfigUtils.getConfigMapWithPrefix(testMap, "k1").size());
    Assert.assertEquals(2, IngestionConfigUtils.getConfigMapWithPrefix(testMap, "k1.").size());
  }
}
