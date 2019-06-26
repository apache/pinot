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
package org.apache.pinot.common.config;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.common.data.StarTreeIndexSpec;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.startree.hll.HllConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class TableConfigTest {

  @Test
  public void testSerializeMandatoryFields()
      throws Exception {
    TableConfig tableConfig = new TableConfig.Builder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setTableName(null);
    testSerializeMandatoryFields(tableConfig, "Table name");

    tableConfig = new TableConfig.Builder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setTableType(null);
    testSerializeMandatoryFields(tableConfig, "Table type");

    tableConfig = new TableConfig.Builder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setValidationConfig(null);
    testSerializeMandatoryFields(tableConfig, "Validation config");

    tableConfig = new TableConfig.Builder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setTenantConfig(null);
    testSerializeMandatoryFields(tableConfig, "Tenant config");

    tableConfig = new TableConfig.Builder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setIndexingConfig(null);
    testSerializeMandatoryFields(tableConfig, "Indexing config");

    tableConfig = new TableConfig.Builder(TableType.OFFLINE).setTableName("myTable").build();
    tableConfig.setCustomConfig(null);
    testSerializeMandatoryFields(tableConfig, "Custom config");
  }

  private void testSerializeMandatoryFields(TableConfig tableConfig, String expectedMessage)
      throws Exception {
    try {
      tableConfig.toJsonConfig();
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(expectedMessage));
    }
    try {
      tableConfig.toZNRecord();
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(expectedMessage));
    }
  }

  @Test
  public void testDeserializeMandatoryFields()
      throws Exception {
    TableConfig tableConfig = new TableConfig.Builder(TableType.OFFLINE).setTableName("myTable").build();
    ObjectNode jsonTableConfig = tableConfig.toJsonConfig();
    TableConfig.fromJsonConfig(jsonTableConfig);

    testDeserializeMandatoryFields(jsonTableConfig.deepCopy(), TableConfig.TABLE_TYPE_KEY);

    testDeserializeMandatoryFields(jsonTableConfig.deepCopy(), TableConfig.TABLE_NAME_KEY);

    testDeserializeMandatoryFields(jsonTableConfig.deepCopy(), TableConfig.VALIDATION_CONFIG_KEY);

    testDeserializeMandatoryFields(jsonTableConfig.deepCopy(), TableConfig.TENANT_CONFIG_KEY);

    testDeserializeMandatoryFields(jsonTableConfig.deepCopy(), TableConfig.INDEXING_CONFIG_KEY);

    testDeserializeMandatoryFields(jsonTableConfig.deepCopy(), TableConfig.CUSTOM_CONFIG_KEY);
  }

  private void testDeserializeMandatoryFields(ObjectNode jsonTableConfig, String mandatoryFieldKey)
      throws Exception {
    jsonTableConfig.remove(mandatoryFieldKey);
    try {
      TableConfig.fromJsonConfig(jsonTableConfig);
      fail();
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(mandatoryFieldKey));
    }
  }

  @Test
  public void testSerializeDeserialize()
      throws Exception {
    TableConfig.Builder tableConfigBuilder = new TableConfig.Builder(TableType.OFFLINE).setTableName("myTable");
    {
      // No quota config
      TableConfig tableConfig = tableConfigBuilder.build();

      assertEquals(tableConfig.getTableName(), "myTable_OFFLINE");
      assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
      assertEquals(tableConfig.getIndexingConfig().getLoadMode(), "HEAP");
      assertNull(tableConfig.getQuotaConfig());

      // Serialize
      ObjectNode jsonTableConfig = tableConfig.toJsonConfig();
      // All nested configs should be json objects instead of serialized strings
      assertTrue(jsonTableConfig.get(TableConfig.VALIDATION_CONFIG_KEY) instanceof ObjectNode);
      assertTrue(jsonTableConfig.get(TableConfig.TENANT_CONFIG_KEY) instanceof ObjectNode);
      assertTrue(jsonTableConfig.get(TableConfig.INDEXING_CONFIG_KEY) instanceof ObjectNode);
      assertTrue(jsonTableConfig.get(TableConfig.CUSTOM_CONFIG_KEY) instanceof ObjectNode);

      // De-serialize
      TableConfig tableConfigToCompare = TableConfig.fromJsonConfig(jsonTableConfig);
      assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      assertNull(tableConfigToCompare.getQuotaConfig());
      assertNull(tableConfigToCompare.getValidationConfig().getReplicaGroupStrategyConfig());
      assertNull(tableConfigToCompare.getValidationConfig().getHllConfig());

      tableConfigToCompare = TableConfig.fromZnRecord(tableConfig.toZNRecord());
      assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      assertNull(tableConfigToCompare.getQuotaConfig());
      assertNull(tableConfig.getValidationConfig().getReplicaGroupStrategyConfig());
      assertNull(tableConfigToCompare.getValidationConfig().getHllConfig());
    }
    {
      // With quota config
      QuotaConfig quotaConfig = new QuotaConfig();
      quotaConfig.setStorage("30G");
      TableConfig tableConfig = tableConfigBuilder.setQuotaConfig(quotaConfig).build();

      assertEquals(tableConfig.getTableName(), "myTable_OFFLINE");
      assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
      assertEquals(tableConfig.getIndexingConfig().getLoadMode(), "HEAP");
      assertNotNull(tableConfig.getQuotaConfig());
      assertEquals(tableConfig.getQuotaConfig().getStorage(), "30G");
      assertNull(tableConfig.getQuotaConfig().getMaxQueriesPerSecond());

      // With qps quota
      quotaConfig.setMaxQueriesPerSecond("100.00");
      tableConfig = tableConfigBuilder.setQuotaConfig(quotaConfig).build();
      assertNotNull(tableConfig.getQuotaConfig());
      assertNotNull(tableConfig.getQuotaConfig().getMaxQueriesPerSecond());
      assertEquals(tableConfig.getQuotaConfig().getMaxQueriesPerSecond(), "100.00");

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = TableConfig.fromJsonConfig(tableConfig.toJsonConfig());
      assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      assertNotNull(tableConfigToCompare.getQuotaConfig());
      assertEquals(tableConfigToCompare.getQuotaConfig().getStorage(), tableConfig.getQuotaConfig().getStorage());

      tableConfigToCompare = TableConfig.fromZnRecord(tableConfig.toZNRecord());
      assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      assertNotNull(tableConfigToCompare.getQuotaConfig());
      assertEquals(tableConfigToCompare.getQuotaConfig().getStorage(), tableConfig.getQuotaConfig().getStorage());
    }
    {
      // With tenant config
      TableConfig tableConfig =
          tableConfigBuilder.setServerTenant("aServerTenant").setBrokerTenant("aBrokerTenant").build();

      assertEquals(tableConfig.getTableName(), "myTable_OFFLINE");
      assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
      assertEquals(tableConfig.getIndexingConfig().getLoadMode(), "HEAP");
      assertNotNull(tableConfig.getTenantConfig());
      assertEquals(tableConfig.getTenantConfig().getServer(), "aServerTenant");
      assertEquals(tableConfig.getTenantConfig().getBroker(), "aBrokerTenant");
      assertNull(tableConfig.getTenantConfig().getTagOverrideConfig());

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = TableConfig.fromJsonConfig(tableConfig.toJsonConfig());
      assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      assertNotNull(tableConfigToCompare.getTenantConfig());
      assertEquals(tableConfigToCompare.getTenantConfig().getServer(), tableConfig.getTenantConfig().getServer());
      assertEquals(tableConfigToCompare.getTenantConfig().getBroker(), tableConfig.getTenantConfig().getBroker());
      assertNull(tableConfig.getTenantConfig().getTagOverrideConfig());

      tableConfigToCompare = TableConfig.fromZnRecord(tableConfig.toZNRecord());
      assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      assertNotNull(tableConfigToCompare.getTenantConfig());
      assertEquals(tableConfigToCompare.getTenantConfig().getServer(), tableConfig.getTenantConfig().getServer());
      assertEquals(tableConfigToCompare.getTenantConfig().getBroker(), tableConfig.getTenantConfig().getBroker());
      assertNull(tableConfig.getTenantConfig().getTagOverrideConfig());

      TagOverrideConfig tagOverrideConfig = new TagOverrideConfig();
      tagOverrideConfig.setRealtimeConsuming("aRTConsumingTag_REALTIME");
      tableConfig = tableConfigBuilder.setTagOverrideConfig(tagOverrideConfig).build();

      assertEquals(tableConfig.getTableName(), "myTable_OFFLINE");
      assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
      assertNotNull(tableConfig.getTenantConfig());
      assertEquals(tableConfig.getTenantConfig().getServer(), "aServerTenant");
      assertEquals(tableConfig.getTenantConfig().getBroker(), "aBrokerTenant");
      assertNotNull(tableConfig.getTenantConfig().getTagOverrideConfig());
      assertEquals(tableConfig.getTenantConfig().getTagOverrideConfig().getRealtimeConsuming(),
          "aRTConsumingTag_REALTIME");
      assertNull(tableConfig.getTenantConfig().getTagOverrideConfig().getRealtimeCompleted());

      // Serialize then de-serialize
      tableConfigToCompare = TableConfig.fromJsonConfig(tableConfig.toJsonConfig());
      assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      assertNotNull(tableConfigToCompare.getTenantConfig());
      assertEquals(tableConfigToCompare.getTenantConfig().getServer(), tableConfig.getTenantConfig().getServer());
      assertEquals(tableConfigToCompare.getTenantConfig().getBroker(), tableConfig.getTenantConfig().getBroker());
      assertNotNull(tableConfigToCompare.getTenantConfig().getTagOverrideConfig());
      assertEquals(tableConfig.getTenantConfig().getTagOverrideConfig(),
          tableConfigToCompare.getTenantConfig().getTagOverrideConfig());

      tableConfigToCompare = TableConfig.fromZnRecord(tableConfig.toZNRecord());
      assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      assertNotNull(tableConfigToCompare.getTenantConfig());
      assertEquals(tableConfigToCompare.getTenantConfig().getServer(), tableConfig.getTenantConfig().getServer());
      assertEquals(tableConfigToCompare.getTenantConfig().getBroker(), tableConfig.getTenantConfig().getBroker());
      assertNotNull(tableConfigToCompare.getTenantConfig().getTagOverrideConfig());
      assertEquals(tableConfig.getTenantConfig().getTagOverrideConfig(),
          tableConfigToCompare.getTenantConfig().getTagOverrideConfig());
    }
    {
      // With SegmentAssignmentStrategyConfig
      ReplicaGroupStrategyConfig replicaGroupConfig = new ReplicaGroupStrategyConfig();
      replicaGroupConfig.setNumInstancesPerPartition(5);
      replicaGroupConfig.setMirrorAssignmentAcrossReplicaGroups(true);
      replicaGroupConfig.setPartitionColumn("memberId");

      TableConfig tableConfig =
          tableConfigBuilder.setSegmentAssignmentStrategy("ReplicaGroupSegmentAssignmentStrategy").build();
      tableConfig.getValidationConfig().setReplicaGroupStrategyConfig(replicaGroupConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = TableConfig.fromJsonConfig(tableConfig.toJsonConfig());
      checkTableConfigWithAssignmentConfig(tableConfig, tableConfigToCompare);

      tableConfigToCompare = TableConfig.fromZnRecord(tableConfig.toZNRecord());
      checkTableConfigWithAssignmentConfig(tableConfig, tableConfigToCompare);
    }
    {
      CompletionConfig completionConfig = new CompletionConfig();
      completionConfig.setCompletionMode("DEFAULT");

      TableConfig tableConfig =
          tableConfigBuilder.build();
      tableConfig.getValidationConfig().setCompletionConfig(completionConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = TableConfig.fromJsonConfig(tableConfig.toJsonConfig());
      checkTableConfigWithCompletionConfig(tableConfig, tableConfigToCompare);

      tableConfigToCompare = TableConfig.fromZnRecord(tableConfig.toZNRecord());
      checkTableConfigWithCompletionConfig(tableConfig, tableConfigToCompare);
    }
    {
      // With default StreamConsumptionConfig
      TableConfig tableConfig = tableConfigBuilder.build();
      assertEquals(tableConfig.getIndexingConfig().getStreamConsumptionConfig().getStreamPartitionAssignmentStrategy(),
          "UniformStreamPartitionAssignment");

      // with streamConsumptionConfig set
      tableConfig =
          tableConfigBuilder.setStreamPartitionAssignmentStrategy("BalancedStreamPartitionAssignment").build();
      assertEquals(tableConfig.getIndexingConfig().getStreamConsumptionConfig().getStreamPartitionAssignmentStrategy(),
          "BalancedStreamPartitionAssignment");

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = TableConfig.fromJsonConfig(tableConfig.toJsonConfig());
      assertEquals(
          tableConfigToCompare.getIndexingConfig().getStreamConsumptionConfig().getStreamPartitionAssignmentStrategy(),
          "BalancedStreamPartitionAssignment");

      tableConfigToCompare = TableConfig.fromZnRecord(tableConfig.toZNRecord());
      assertEquals(
          tableConfigToCompare.getIndexingConfig().getStreamConsumptionConfig().getStreamPartitionAssignmentStrategy(),
          "BalancedStreamPartitionAssignment");
    }
    {
      // With star tree config
      StarTreeIndexSpec starTreeIndexSpec = new StarTreeIndexSpec();
      Set<String> dims = new HashSet<>();
      dims.add("dims");
      starTreeIndexSpec.setDimensionsSplitOrder(Collections.singletonList("dim"));
      starTreeIndexSpec.setMaxLeafRecords(5);
      starTreeIndexSpec.setSkipMaterializationCardinalityThreshold(1);
      starTreeIndexSpec.setSkipMaterializationForDimensions(dims);
      starTreeIndexSpec.setSkipStarNodeCreationForDimensions(dims);

      TableConfig tableConfig = tableConfigBuilder.build();
      tableConfig.getIndexingConfig().setStarTreeIndexSpec(starTreeIndexSpec);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = TableConfig.fromJsonConfig(tableConfig.toJsonConfig());
      checkTableConfigWithStarTreeConfig(tableConfig, tableConfigToCompare);

      tableConfigToCompare = TableConfig.fromZnRecord(tableConfig.toZNRecord());
      checkTableConfigWithStarTreeConfig(tableConfig, tableConfigToCompare);
    }
    {
      // With HllConfig
      HllConfig hllConfig = new HllConfig();
      Set<String> columns = new HashSet<>();
      columns.add("column");
      columns.add("column2");

      hllConfig.setColumnsToDeriveHllFields(columns);
      hllConfig.setHllLog2m(9);
      hllConfig.setHllDeriveColumnSuffix("suffix");

      String hllConfigJson = hllConfig.toJsonString();
      HllConfig newHllConfig = HllConfig.fromJsonString(hllConfigJson);
      assertEquals(hllConfig.getColumnsToDeriveHllFields(), newHllConfig.getColumnsToDeriveHllFields());
      assertEquals(hllConfig.getHllLog2m(), newHllConfig.getHllLog2m());
      assertEquals(hllConfig.getHllDeriveColumnSuffix(), newHllConfig.getHllDeriveColumnSuffix());

      TableConfig tableConfig = tableConfigBuilder.build();
      tableConfig.getValidationConfig().setHllConfig(hllConfig);

      // Serialize then de-serialize
      TableConfig tableConfigToCompare = TableConfig.fromJsonConfig(tableConfig.toJsonConfig());
      checkTableConfigWithHllConfig(tableConfig, tableConfigToCompare);

      tableConfigToCompare = TableConfig.fromZnRecord(tableConfig.toZNRecord());
      checkTableConfigWithHllConfig(tableConfig, tableConfigToCompare);
    }
  }

  private void checkTableConfigWithAssignmentConfig(TableConfig tableConfig, TableConfig tableConfigToCompare) {
    // Check that the segment assignment configuration does exist.
    assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
    assertNotNull(tableConfigToCompare.getValidationConfig().getReplicaGroupStrategyConfig());
    assertEquals(tableConfigToCompare.getValidationConfig().getReplicaGroupStrategyConfig(),
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig());

    // Check that the configurations are correct.
    ReplicaGroupStrategyConfig strategyConfig =
        tableConfigToCompare.getValidationConfig().getReplicaGroupStrategyConfig();
    assertTrue(strategyConfig.getMirrorAssignmentAcrossReplicaGroups());
    assertEquals(strategyConfig.getNumInstancesPerPartition(), 5);
    assertEquals(strategyConfig.getPartitionColumn(), "memberId");
  }

  private void checkTableConfigWithCompletionConfig(TableConfig tableConfig, TableConfig tableConfigToCompare) {
    // Check that the segment assignment configuration does exist.
    assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
    assertNotNull(tableConfigToCompare.getValidationConfig().getCompletionConfig());
    assertEquals(tableConfigToCompare.getValidationConfig().getCompletionConfig(),
        tableConfig.getValidationConfig().getCompletionConfig());

    // Check that the configurations are correct.
    CompletionConfig completionConfig =
        tableConfigToCompare.getValidationConfig().getCompletionConfig();
    assertEquals(completionConfig.getCompletionMode(), "DEFAULT");
  }

  private void checkTableConfigWithStarTreeConfig(TableConfig tableConfig, TableConfig tableConfigToCompare)
      throws Exception {
    // Check that the segment assignment configuration does exist.
    assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
    assertNotNull(tableConfigToCompare.getIndexingConfig().getStarTreeIndexSpec());

    // Check that the configurations are correct.
    StarTreeIndexSpec starTreeIndexSpec = tableConfigToCompare.getIndexingConfig().getStarTreeIndexSpec();

    Set<String> dims = new HashSet<>();
    dims.add("dims");

    assertEquals(starTreeIndexSpec.getDimensionsSplitOrder(), Collections.singletonList("dim"));
    assertEquals(starTreeIndexSpec.getMaxLeafRecords(), 5);
    assertEquals(starTreeIndexSpec.getSkipMaterializationCardinalityThreshold(), 1);
    assertEquals(starTreeIndexSpec.getSkipMaterializationForDimensions(), dims);
    assertEquals(starTreeIndexSpec.getSkipStarNodeCreationForDimensions(), dims);

    starTreeIndexSpec = StarTreeIndexSpec.fromJsonString(starTreeIndexSpec.toJsonString());
    assertEquals(starTreeIndexSpec.getDimensionsSplitOrder(), Collections.singletonList("dim"));
    assertEquals(starTreeIndexSpec.getMaxLeafRecords(), 5);
    assertEquals(starTreeIndexSpec.getSkipMaterializationCardinalityThreshold(), 1);
    assertEquals(starTreeIndexSpec.getSkipMaterializationForDimensions(), dims);
    assertEquals(starTreeIndexSpec.getSkipStarNodeCreationForDimensions(), dims);
  }

  private void checkTableConfigWithHllConfig(TableConfig tableConfig, TableConfig tableConfigToCompare) {
    // Check that the segment assignment configuration does exist.
    assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
    assertNotNull(tableConfigToCompare.getValidationConfig().getHllConfig());

    // Check that the configurations are correct.
    HllConfig hllConfig = tableConfigToCompare.getValidationConfig().getHllConfig();

    Set<String> columns = new HashSet<>();
    columns.add("column");
    columns.add("column2");

    assertEquals(hllConfig.getColumnsToDeriveHllFields(), columns);
    assertEquals(hllConfig.getHllLog2m(), 9);
    assertEquals(hllConfig.getHllDeriveColumnSuffix(), "suffix");
  }
}
