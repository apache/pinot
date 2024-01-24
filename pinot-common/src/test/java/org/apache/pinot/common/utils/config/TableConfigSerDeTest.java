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
package org.apache.pinot.common.utils.config;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.tier.TierFactory;
import org.apache.pinot.spi.config.table.CompletionConfig;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.QueryConfig;
import org.apache.pinot.spi.config.table.QuotaConfig;
import org.apache.pinot.spi.config.table.ReplicaGroupStrategyConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.TunerConfig;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceConstraintConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.config.table.ingestion.AggregationConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ComplexTypeConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TableConfigSerDeTest {
  private static final double NO_DICTIONARY_THRESHOLD_RATIO = 0.72;
  @Test
  public void testSerDe()
      throws IOException {
    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable")
        .setNoDictionarySizeRatioThreshold(NO_DICTIONARY_THRESHOLD_RATIO).setOptimizeDictionaryForMetrics(true);
    {
      // Default table config
      TableConfig tableConfig = tableConfigBuilder.build();

      checkDefaultTableConfig(tableConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkDefaultTableConfig(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkDefaultTableConfig(tableConfigToCompare);

      // Backward-compatible for raw table name and lower case table type
      ObjectNode tableConfigJson = (ObjectNode) tableConfigBuilder.build().toJsonNode();
      tableConfigJson.put(TableConfig.TABLE_NAME_KEY, "testTable");
      tableConfigJson.put(TableConfig.TABLE_TYPE_KEY, "offline");
      tableConfigToCompare = JsonUtils.jsonNodeToObject(tableConfigJson, TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkDefaultTableConfig(tableConfigToCompare);
    }
    {
      // With quota config
      QuotaConfig quotaConfig = new QuotaConfig("30g", "100.00");
      TableConfig tableConfig = tableConfigBuilder.setQuotaConfig(quotaConfig).build();

      checkQuotaConfig(tableConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkQuotaConfig(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkQuotaConfig(tableConfigToCompare);
    }
    {
      // With tenant config
      TableConfig tableConfig =
          tableConfigBuilder.setServerTenant("aServerTenant").setBrokerTenant("aBrokerTenant").build();

      checkTenantConfigWithoutTagOverride(tableConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkTenantConfigWithoutTagOverride(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkTenantConfigWithoutTagOverride(tableConfigToCompare);

      // With tag override config
      TagOverrideConfig tagOverrideConfig = new TagOverrideConfig("aRTConsumingTag_REALTIME", null);
      tableConfig = tableConfigBuilder.setTagOverrideConfig(tagOverrideConfig).build();

      checkTenantConfigWithTagOverride(tableConfig);

      // Serialize then de-serialize
      tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkTenantConfigWithTagOverride(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkTenantConfigWithTagOverride(tableConfigToCompare);
    }
    {
      // With SegmentAssignmentStrategyConfig
      ReplicaGroupStrategyConfig replicaGroupStrategyConfig = new ReplicaGroupStrategyConfig("memberId", 5);
      TableConfig tableConfig = tableConfigBuilder.setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy")
          .setReplicaGroupStrategyConfig(replicaGroupStrategyConfig).build();

      checkSegmentAssignmentStrategyConfig(tableConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkSegmentAssignmentStrategyConfig(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkSegmentAssignmentStrategyConfig(tableConfigToCompare);
    }
    {
      // With completion config
      CompletionConfig completionConfig = new CompletionConfig("DEFAULT");
      TableConfig tableConfig = tableConfigBuilder.setCompletionConfig(completionConfig).build();

      checkCompletionConfig(tableConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkCompletionConfig(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkCompletionConfig(tableConfigToCompare);
    }
    {
      // With routing config
      RoutingConfig routingConfig =
          new RoutingConfig("builder", Arrays.asList("pruner0", "pruner1", "pruner2"), "selector", false);
      TableConfig tableConfig = tableConfigBuilder.setRoutingConfig(routingConfig).build();

      checkRoutingConfig(tableConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkRoutingConfig(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkRoutingConfig(tableConfigToCompare);
    }
    {
      // With query config
      QueryConfig queryConfig = new QueryConfig(1000L, true, true, Collections.singletonMap("func(a)", "b"), null,
          null);
      TableConfig tableConfig = tableConfigBuilder.setQueryConfig(queryConfig).build();

      checkQueryConfig(tableConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkQueryConfig(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkQueryConfig(tableConfigToCompare);
    }
    {
      // With instance assignment config
      InstanceAssignmentConfig instanceAssignmentConfig =
          new InstanceAssignmentConfig(new InstanceTagPoolConfig("tenant_OFFLINE", true, 3, null),
              new InstanceConstraintConfig(Arrays.asList("constraint1", "constraint2")),
              new InstanceReplicaGroupPartitionConfig(true, 0, 3, 5, 0, 0, false, null));
      TableConfig tableConfig = tableConfigBuilder.setInstanceAssignmentConfigMap(
          Collections.singletonMap(InstancePartitionsType.OFFLINE.toString(), instanceAssignmentConfig)).build();

      checkInstanceAssignmentConfig(tableConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkInstanceAssignmentConfig(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkInstanceAssignmentConfig(tableConfigToCompare);
    }
    {
      // With field config
      Map<String, String> properties = new HashMap<>();
      properties.put("foo", "bar");
      properties.put("foobar", "potato");
      List<FieldConfig> fieldConfigList = Arrays.asList(new FieldConfig("column1", FieldConfig.EncodingType.DICTIONARY,
              Lists.newArrayList(FieldConfig.IndexType.INVERTED, FieldConfig.IndexType.RANGE), null, properties),
          new FieldConfig("column2", null, Collections.emptyList(), null, null),
          new FieldConfig("column3", FieldConfig.EncodingType.RAW, Collections.emptyList(),
              FieldConfig.CompressionCodec.SNAPPY, null));
      TableConfig tableConfig = tableConfigBuilder.setFieldConfigList(fieldConfigList).build();

      checkFieldConfig(tableConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkFieldConfig(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkFieldConfig(tableConfigToCompare);
    }
    {
      // with upsert config
      TableConfig tableConfig = tableConfigBuilder.setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL)).build();

      // Serialize then de-serialize
      checkTableConfigWithUpsertConfig(JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class));
      checkTableConfigWithUpsertConfig(TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig)));
    }
    {
      // with dedup config
      DedupConfig dedupConfig = new DedupConfig(true, HashFunction.MD5);
      TableConfig tableConfig = tableConfigBuilder.setDedupConfig(dedupConfig).build();
      // Serialize then de-serialize
      checkTableConfigWithDedupConfig(JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class));
      checkTableConfigWithDedupConfig(TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig)));
    }
    {
      // with SegmentsValidationAndRetentionConfig
      TableConfig tableConfig = tableConfigBuilder.setPeerSegmentDownloadScheme(CommonConstants.HTTP_PROTOCOL).build();
      checkSegmentsValidationAndRetentionConfig(
          JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class));
      checkSegmentsValidationAndRetentionConfig(
          TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig)));
    }
    {
      // With ingestion config
      IngestionConfig ingestionConfig = new IngestionConfig();
      ingestionConfig.setBatchIngestionConfig(
          new BatchIngestionConfig(Collections.singletonList(Collections.singletonMap("batchType", "s3")), "APPEND",
              "HOURLY"));
      ingestionConfig.setStreamIngestionConfig(
          new StreamIngestionConfig(Collections.singletonList(Collections.singletonMap("streamType", "kafka"))));
      ingestionConfig.setFilterConfig(new FilterConfig("filterFunc(foo)"));
      ingestionConfig.setTransformConfigs(
          Arrays.asList(new TransformConfig("bar", "func(moo)"), new TransformConfig("zoo", "myfunc()")));
      ingestionConfig.setComplexTypeConfig(new ComplexTypeConfig(Arrays.asList("c1", "c2"), ".",
          ComplexTypeConfig.CollectionNotUnnestedToJson.NON_PRIMITIVE, Collections.emptyMap()));
      ingestionConfig.setAggregationConfigs(
          Arrays.asList(new AggregationConfig("SUM__bar", "SUM(bar)"), new AggregationConfig("MIN_foo", "MIN(foo)")));

      TableConfig tableConfig = tableConfigBuilder.setIngestionConfig(ingestionConfig).build();
      checkIngestionConfig(tableConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkIngestionConfig(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkIngestionConfig(tableConfigToCompare);
    }
    {
      // With tier config
      List<TierConfig> tierConfigList = Lists.newArrayList(
          new TierConfig("tierA", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "10d", null,
              TierFactory.PINOT_SERVER_STORAGE_TYPE, "tierA_tag_OFFLINE", null, null),
          new TierConfig("tierB", TierFactory.TIME_SEGMENT_SELECTOR_TYPE, "30d", null,
              TierFactory.PINOT_SERVER_STORAGE_TYPE, "tierB_tag_OFFLINE", null, null),
          new TierConfig("tier0", TierFactory.FIXED_SEGMENT_SELECTOR_TYPE, null, Lists.newArrayList("seg0"),
              TierFactory.PINOT_SERVER_STORAGE_TYPE, "tierB_tag_OFFLINE", null, null));
      TableConfig tableConfig = tableConfigBuilder.setTierConfigList(tierConfigList).build();

      checkTierConfigList(tableConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      checkTierConfigList(tableConfigToCompare);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      checkTierConfigList(tableConfigToCompare);
    }
    {
      // With tuner config
      String name = "testTuner";
      Map<String, String> props = new HashMap<>();
      props.put("key", "value");
      TunerConfig tunerConfig = new TunerConfig(name, props);
      TableConfig tableConfig = tableConfigBuilder.setTunerConfigList(Lists.newArrayList(tunerConfig)).build();

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = JsonUtils.stringToObject(tableConfig.toJsonString(), TableConfig.class);
      assertEquals(tableConfigToCompare, tableConfig);
      TunerConfig tunerConfigToCompare = tableConfigToCompare.getTunerConfigsList().get(0);

      assertEquals(tunerConfigToCompare.getName(), name);
      assertEquals(tunerConfigToCompare.getTunerProperties(), props);

      tableConfigToCompare = TableConfigUtils.fromZNRecord(TableConfigUtils.toZNRecord(tableConfig));
      assertEquals(tableConfigToCompare, tableConfig);
      tunerConfigToCompare = tableConfigToCompare.getTunerConfigsList().get(0);
      assertEquals(tunerConfigToCompare.getName(), name);
      assertEquals(tunerConfigToCompare.getTunerProperties(), props);
    }
  }

  private void checkSegmentsValidationAndRetentionConfig(TableConfig tableConfig) {
    // TODO validate other fields of SegmentsValidationAndRetentionConfig.
    assertEquals(tableConfig.getValidationConfig().getPeerSegmentDownloadScheme(), CommonConstants.HTTP_PROTOCOL);
  }

  private void checkDefaultTableConfig(TableConfig tableConfig) {
    // Check mandatory fields
    assertEquals(tableConfig.getTableName(), "testTable_OFFLINE");
    assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
    assertNotNull(tableConfig.getValidationConfig());
    assertNotNull(tableConfig.getTenantConfig());
    assertNotNull(tableConfig.getIndexingConfig());
    assertNotNull(tableConfig.getCustomConfig());

    // Check optional fields
    assertNull(tableConfig.getQuotaConfig());
    assertNull(tableConfig.getRoutingConfig());
    assertNull(tableConfig.getQueryConfig());
    assertNull(tableConfig.getInstanceAssignmentConfigMap());
    assertNull(tableConfig.getSegmentAssignmentConfigMap());
    assertNull(tableConfig.getFieldConfigList());
    assertEquals(tableConfig.getIndexingConfig().isOptimizeDictionaryForMetrics(), true);
    assertEquals(tableConfig.getIndexingConfig().getNoDictionarySizeRatioThreshold(), NO_DICTIONARY_THRESHOLD_RATIO);

    // Serialize
    ObjectNode tableConfigJson = (ObjectNode) tableConfig.toJsonNode();
    assertEquals(tableConfigJson.get(TableConfig.TABLE_NAME_KEY).asText(), "testTable_OFFLINE");
    assertEquals(tableConfigJson.get(TableConfig.TABLE_TYPE_KEY).asText(), "OFFLINE");
    assertTrue(tableConfigJson.get(TableConfig.VALIDATION_CONFIG_KEY) instanceof ObjectNode);
    assertTrue(tableConfigJson.get(TableConfig.TENANT_CONFIG_KEY) instanceof ObjectNode);
    assertTrue(tableConfigJson.get(TableConfig.INDEXING_CONFIG_KEY) instanceof ObjectNode);
    assertTrue(tableConfigJson.get(TableConfig.CUSTOM_CONFIG_KEY) instanceof ObjectNode);
    assertFalse(tableConfigJson.has(TableConfig.QUOTA_CONFIG_KEY));
    assertFalse(tableConfigJson.has(TableConfig.TASK_CONFIG_KEY));
    assertFalse(tableConfigJson.has(TableConfig.ROUTING_CONFIG_KEY));
    assertFalse(tableConfigJson.has(TableConfig.QUERY_CONFIG_KEY));
    assertFalse(tableConfigJson.has(TableConfig.INSTANCE_ASSIGNMENT_CONFIG_MAP_KEY));
    assertFalse(tableConfigJson.has(TableConfig.SEGMENT_ASSIGNMENT_CONFIG_MAP_KEY));
    assertFalse(tableConfigJson.has(TableConfig.FIELD_CONFIG_LIST_KEY));
  }

  private void checkQuotaConfig(TableConfig tableConfig) {
    QuotaConfig quotaConfig = tableConfig.getQuotaConfig();
    assertNotNull(quotaConfig);
    assertEquals(quotaConfig.getStorage(), "30G");
    assertEquals(quotaConfig.getMaxQueriesPerSecond(), "100.0");
  }

  private void checkTenantConfigWithoutTagOverride(TableConfig tableConfig) {
    TenantConfig tenantConfig = tableConfig.getTenantConfig();
    assertNotNull(tenantConfig);
    assertEquals(tenantConfig.getServer(), "aServerTenant");
    assertEquals(tenantConfig.getBroker(), "aBrokerTenant");
    assertNull(tenantConfig.getTagOverrideConfig());
  }

  private void checkTenantConfigWithTagOverride(TableConfig tableConfig) {
    TenantConfig tenantConfig = tableConfig.getTenantConfig();
    assertNotNull(tenantConfig);
    assertEquals(tenantConfig.getServer(), "aServerTenant");
    assertEquals(tenantConfig.getBroker(), "aBrokerTenant");
    assertNotNull(tenantConfig.getTagOverrideConfig());
    assertEquals(tenantConfig.getTagOverrideConfig().getRealtimeConsuming(), "aRTConsumingTag_REALTIME");
    assertNull(tenantConfig.getTagOverrideConfig().getRealtimeCompleted());
  }

  private void checkSegmentAssignmentStrategyConfig(TableConfig tableConfig) {
    SegmentsValidationAndRetentionConfig validationConfig = tableConfig.getValidationConfig();
    assertEquals(validationConfig.getSegmentAssignmentStrategy(), "ReplicaGroupSegmentAssignmentStrategy");
    ReplicaGroupStrategyConfig replicaGroupStrategyConfig = validationConfig.getReplicaGroupStrategyConfig();
    assertNotNull(replicaGroupStrategyConfig);
    assertEquals(replicaGroupStrategyConfig.getPartitionColumn(), "memberId");
    assertEquals(replicaGroupStrategyConfig.getNumInstancesPerPartition(), 5);
  }

  private void checkCompletionConfig(TableConfig tableConfig) {
    CompletionConfig completionConfig = tableConfig.getValidationConfig().getCompletionConfig();
    assertNotNull(completionConfig);
    assertEquals(completionConfig.getCompletionMode(), "DEFAULT");
  }

  private void checkRoutingConfig(TableConfig tableConfig) {
    RoutingConfig routingConfig = tableConfig.getRoutingConfig();
    assertNotNull(routingConfig);
    assertEquals(routingConfig.getRoutingTableBuilderName(), "builder");
    assertEquals(routingConfig.getSegmentPrunerTypes(), Arrays.asList("pruner0", "pruner1", "pruner2"));
    assertEquals(routingConfig.getInstanceSelectorType(), "selector");
  }

  private void checkQueryConfig(TableConfig tableConfig) {
    QueryConfig queryConfig = tableConfig.getQueryConfig();
    assertNotNull(queryConfig);
    assertEquals(queryConfig.getTimeoutMs(), Long.valueOf(1000L));
    assertEquals(queryConfig.getDisableGroovy(), Boolean.TRUE);
    assertEquals(queryConfig.getExpressionOverrideMap(), Collections.singletonMap("func(a)", "b"));
  }

  private void checkIngestionConfig(TableConfig tableConfig) {
    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    assertNotNull(ingestionConfig);
    assertNotNull(ingestionConfig.getFilterConfig());
    assertEquals(ingestionConfig.getFilterConfig().getFilterFunction(), "filterFunc(foo)");
    List<TransformConfig> transformConfigs = ingestionConfig.getTransformConfigs();
    assertNotNull(transformConfigs);
    assertEquals(transformConfigs.size(), 2);
    assertEquals(transformConfigs.get(0).getColumnName(), "bar");
    assertEquals(transformConfigs.get(0).getTransformFunction(), "func(moo)");
    assertEquals(transformConfigs.get(1).getColumnName(), "zoo");
    assertEquals(transformConfigs.get(1).getTransformFunction(), "myfunc()");
    assertNotNull(ingestionConfig.getBatchIngestionConfig());
    assertNotNull(ingestionConfig.getBatchIngestionConfig().getBatchConfigMaps());
    assertEquals(ingestionConfig.getBatchIngestionConfig().getBatchConfigMaps().size(), 1);
    assertEquals(ingestionConfig.getBatchIngestionConfig().getBatchConfigMaps().get(0).get("batchType"), "s3");
    assertEquals(ingestionConfig.getBatchIngestionConfig().getSegmentIngestionType(), "APPEND");
    assertEquals(ingestionConfig.getBatchIngestionConfig().getSegmentIngestionFrequency(), "HOURLY");
    assertNotNull(ingestionConfig.getStreamIngestionConfig());
    assertNotNull(ingestionConfig.getStreamIngestionConfig().getStreamConfigMaps());
    assertEquals(ingestionConfig.getStreamIngestionConfig().getStreamConfigMaps().size(), 1);
    assertEquals(ingestionConfig.getStreamIngestionConfig().getStreamConfigMaps().get(0).get("streamType"), "kafka");
  }

  private void checkTierConfigList(TableConfig tableConfig) {
    List<TierConfig> tierConfigsList = tableConfig.getTierConfigsList();
    assertNotNull(tierConfigsList);
    assertEquals(tierConfigsList.size(), 3);
    assertEquals(tierConfigsList.get(0).getName(), "tierA");
    assertEquals(tierConfigsList.get(0).getSegmentSelectorType(), TierFactory.TIME_SEGMENT_SELECTOR_TYPE);
    assertEquals(tierConfigsList.get(0).getSegmentAge(), "10d");
    assertEquals(tierConfigsList.get(0).getStorageType(), TierFactory.PINOT_SERVER_STORAGE_TYPE);
    assertEquals(tierConfigsList.get(0).getServerTag(), "tierA_tag_OFFLINE");
    assertEquals(tierConfigsList.get(1).getName(), "tierB");
    assertEquals(tierConfigsList.get(1).getSegmentSelectorType(), TierFactory.TIME_SEGMENT_SELECTOR_TYPE);
    assertEquals(tierConfigsList.get(1).getSegmentAge(), "30d");
    assertEquals(tierConfigsList.get(1).getStorageType(), TierFactory.PINOT_SERVER_STORAGE_TYPE);
    assertEquals(tierConfigsList.get(1).getServerTag(), "tierB_tag_OFFLINE");
    assertEquals(tierConfigsList.get(2).getName(), "tier0");
    assertEquals(tierConfigsList.get(2).getSegmentSelectorType(), TierFactory.FIXED_SEGMENT_SELECTOR_TYPE);
    assertNull(tierConfigsList.get(2).getSegmentAge());
    assertEquals(tierConfigsList.get(2).getSegmentList(), Lists.newArrayList("seg0"));
    assertEquals(tierConfigsList.get(2).getStorageType(), TierFactory.PINOT_SERVER_STORAGE_TYPE);
    assertEquals(tierConfigsList.get(2).getServerTag(), "tierB_tag_OFFLINE");
  }

  private void checkInstanceAssignmentConfig(TableConfig tableConfig) {
    Map<String, InstanceAssignmentConfig> instanceAssignmentConfigMap = tableConfig.getInstanceAssignmentConfigMap();
    assertNotNull(instanceAssignmentConfigMap);
    assertEquals(instanceAssignmentConfigMap.size(), 1);
    assertTrue(instanceAssignmentConfigMap.containsKey(InstancePartitionsType.OFFLINE.toString()));
    InstanceAssignmentConfig instanceAssignmentConfig =
        instanceAssignmentConfigMap.get(InstancePartitionsType.OFFLINE.toString());

    InstanceTagPoolConfig tagPoolConfig = instanceAssignmentConfig.getTagPoolConfig();
    assertEquals(tagPoolConfig.getTag(), "tenant_OFFLINE");
    assertTrue(tagPoolConfig.isPoolBased());
    assertEquals(tagPoolConfig.getNumPools(), 3);
    assertNull(tagPoolConfig.getPools());

    InstanceConstraintConfig constraintConfig = instanceAssignmentConfig.getConstraintConfig();
    assertNotNull(constraintConfig);
    assertEquals(constraintConfig.getConstraints(), Arrays.asList("constraint1", "constraint2"));

    InstanceReplicaGroupPartitionConfig replicaGroupPartitionConfig =
        instanceAssignmentConfig.getReplicaGroupPartitionConfig();
    assertTrue(replicaGroupPartitionConfig.isReplicaGroupBased());
    assertEquals(replicaGroupPartitionConfig.getNumInstances(), 0);
    assertEquals(replicaGroupPartitionConfig.getNumReplicaGroups(), 3);
    assertEquals(replicaGroupPartitionConfig.getNumInstancesPerReplicaGroup(), 5);
    assertEquals(replicaGroupPartitionConfig.getNumPartitions(), 0);
    assertEquals(replicaGroupPartitionConfig.getNumInstancesPerPartition(), 0);
  }

  private void checkFieldConfig(TableConfig tableConfig) {
    List<FieldConfig> fieldConfigList = tableConfig.getFieldConfigList();
    assertNotNull(fieldConfigList);
    assertEquals(fieldConfigList.size(), 3);

    FieldConfig firstFieldConfig = fieldConfigList.get(0);
    assertEquals(firstFieldConfig.getName(), "column1");
    assertEquals(firstFieldConfig.getEncodingType(), FieldConfig.EncodingType.DICTIONARY);
    assertEquals(firstFieldConfig.getIndexTypes().get(0), FieldConfig.IndexType.INVERTED);
    assertEquals(firstFieldConfig.getIndexTypes().get(1), FieldConfig.IndexType.RANGE);
    Map<String, String> expectedProperties = new HashMap<>();
    expectedProperties.put("foo", "bar");
    expectedProperties.put("foobar", "potato");
    assertEquals(firstFieldConfig.getProperties(), expectedProperties);

    FieldConfig secondFieldConfig = fieldConfigList.get(1);
    assertEquals(secondFieldConfig.getName(), "column2");
    assertNull(secondFieldConfig.getEncodingType());
    assertNull(secondFieldConfig.getIndexType());
    assertEquals(secondFieldConfig.getIndexTypes().size(), 0);
    assertNull(secondFieldConfig.getProperties());

    FieldConfig thirdFieldConfig = fieldConfigList.get(2);
    assertEquals(thirdFieldConfig.getName(), "column3");
    assertEquals(thirdFieldConfig.getEncodingType(), FieldConfig.EncodingType.RAW);
    assertNull(thirdFieldConfig.getIndexType());
    assertEquals(thirdFieldConfig.getCompressionCodec(), FieldConfig.CompressionCodec.SNAPPY);
    assertNull(thirdFieldConfig.getProperties());
  }

  private void checkTableConfigWithUpsertConfig(TableConfig tableConfig) {
    UpsertConfig upsertConfig = tableConfig.getUpsertConfig();
    assertNotNull(upsertConfig);

    assertEquals(upsertConfig.getMode(), UpsertConfig.Mode.FULL);
  }

  private void checkTableConfigWithDedupConfig(TableConfig tableConfig) {
    DedupConfig dedupConfig = tableConfig.getDedupConfig();
    assertNotNull(dedupConfig);

    assertTrue(dedupConfig.isDedupEnabled());
    assertEquals(dedupConfig.getHashFunction(), HashFunction.MD5);
  }
}
