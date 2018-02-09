/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.common.config;

import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.TableType;
import com.linkedin.pinot.startree.hll.HllConfig;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.helix.ZNRecord;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TableConfigTest {

  @Test
  public void testSerializeDeserialize() throws Exception {
    TableConfig.Builder tableConfigBuilder = new TableConfig.Builder(TableType.OFFLINE).setTableName("myTable");
    {
      // No quota config
      TableConfig tableConfig = tableConfigBuilder.build();

      Assert.assertEquals(tableConfig.getTableName(), "myTable_OFFLINE");
      Assert.assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
      Assert.assertEquals(tableConfig.getIndexingConfig().getLoadMode(), "HEAP");
      Assert.assertNull(tableConfig.getQuotaConfig());

      // Serialize then de-serialize
      JSONObject jsonConfig = TableConfig.toJSONConfig(tableConfig);
      TableConfig tableConfigToCompare = TableConfig.fromJSONConfig(jsonConfig);
      Assert.assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      Assert.assertNull(tableConfigToCompare.getQuotaConfig());
      Assert.assertNull(tableConfigToCompare.getValidationConfig().getReplicaGroupStrategyConfig());
      Assert.assertNull(tableConfigToCompare.getValidationConfig().getHllConfig());
      Assert.assertNull(tableConfigToCompare.getValidationConfig().getStarTreeConfig());

      ZNRecord znRecord = TableConfig.toZnRecord(tableConfig);
      tableConfigToCompare = TableConfig.fromZnRecord(znRecord);
      Assert.assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      Assert.assertNull(tableConfigToCompare.getQuotaConfig());
      Assert.assertNull(tableConfig.getValidationConfig().getReplicaGroupStrategyConfig());
      Assert.assertNull(tableConfigToCompare.getValidationConfig().getHllConfig());
      Assert.assertNull(tableConfigToCompare.getValidationConfig().getStarTreeConfig());
    }
    {
      // With quota config
      QuotaConfig quotaConfig = new QuotaConfig();
      quotaConfig.setStorage("30G");
      TableConfig tableConfig = tableConfigBuilder.setQuotaConfig(quotaConfig).build();

      Assert.assertEquals(tableConfig.getTableName(), "myTable_OFFLINE");
      Assert.assertEquals(tableConfig.getTableType(), TableType.OFFLINE);
      Assert.assertEquals(tableConfig.getIndexingConfig().getLoadMode(), "HEAP");
      Assert.assertNotNull(tableConfig.getQuotaConfig());
      Assert.assertEquals(tableConfig.getQuotaConfig().getStorage(), "30G");

      // Serialize then de-serialize
      JSONObject jsonConfig = TableConfig.toJSONConfig(tableConfig);
      TableConfig tableConfigToCompare = TableConfig.fromJSONConfig(jsonConfig);
      Assert.assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      Assert.assertNotNull(tableConfigToCompare.getQuotaConfig());
      Assert.assertEquals(tableConfigToCompare.getQuotaConfig().getStorage(),
          tableConfig.getQuotaConfig().getStorage());

      ZNRecord znRecord = TableConfig.toZnRecord(tableConfig);
      tableConfigToCompare = TableConfig.fromZnRecord(znRecord);
      Assert.assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
      Assert.assertNotNull(tableConfigToCompare.getQuotaConfig());
      Assert.assertEquals(tableConfigToCompare.getQuotaConfig().getStorage(),
          tableConfig.getQuotaConfig().getStorage());
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
      JSONObject jsonConfig = TableConfig.toJSONConfig(tableConfig);
      TableConfig tableConfigToCompare = TableConfig.fromJSONConfig(jsonConfig);
      checkTableConfigWithAssignmentConfig(tableConfig, tableConfigToCompare);

      ZNRecord znRecord = TableConfig.toZnRecord(tableConfig);
      tableConfigToCompare = TableConfig.fromZnRecord(znRecord);
      checkTableConfigWithAssignmentConfig(tableConfig, tableConfigToCompare);
    }
    {
      // With default StreamConsumptionConfig
      TableConfig tableConfig = tableConfigBuilder.build();
      Assert.assertEquals(
          tableConfig.getIndexingConfig().getStreamConsumptionConfig()
              .getStreamPartitionAssignmentStrategy(), "UniformStreamPartitionAssignment");

      // with streamConsumptionConfig set
      tableConfig =
          tableConfigBuilder.setStreamPartitionAssignmentStrategy("BalancedStreamPartitionAssignment").build();
      Assert.assertEquals(
          tableConfig.getIndexingConfig().getStreamConsumptionConfig()
              .getStreamPartitionAssignmentStrategy(), "BalancedStreamPartitionAssignment");

      // Serialize then de-serialize
      JSONObject jsonConfig = TableConfig.toJSONConfig(tableConfig);
      TableConfig tableConfigToCompare = TableConfig.fromJSONConfig(jsonConfig);
      Assert.assertEquals(
          tableConfigToCompare.getIndexingConfig().getStreamConsumptionConfig()
              .getStreamPartitionAssignmentStrategy(), "BalancedStreamPartitionAssignment");

      ZNRecord znRecord = TableConfig.toZnRecord(tableConfig);
      tableConfigToCompare = TableConfig.fromZnRecord(znRecord);
      Assert.assertEquals(
          tableConfigToCompare.getIndexingConfig().getStreamConsumptionConfig()
              .getStreamPartitionAssignmentStrategy(), "BalancedStreamPartitionAssignment");
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
      tableConfig.getValidationConfig().setStarTreeConfig(starTreeIndexSpec);

      // Serialize then de-serialize
      JSONObject jsonConfig = TableConfig.toJSONConfig(tableConfig);
      TableConfig tableConfigToCompare = TableConfig.fromJSONConfig(jsonConfig);
      checkTableConfigWithStarTreeConfig(tableConfig, tableConfigToCompare);

      ZNRecord znRecord = TableConfig.toZnRecord(tableConfig);
      tableConfigToCompare = TableConfig.fromZnRecord(znRecord);
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
      Assert.assertEquals(hllConfig.getColumnsToDeriveHllFields(), newHllConfig.getColumnsToDeriveHllFields());
      Assert.assertEquals(hllConfig.getHllLog2m(), newHllConfig.getHllLog2m());
      Assert.assertEquals(hllConfig.getHllDeriveColumnSuffix(), newHllConfig.getHllDeriveColumnSuffix());

      TableConfig tableConfig = tableConfigBuilder.build();
      tableConfig.getValidationConfig().setHllConfig(hllConfig);

      // Serialize then de-serialize
      JSONObject jsonConfig = TableConfig.toJSONConfig(tableConfig);
      TableConfig tableConfigToCompare = TableConfig.fromJSONConfig(jsonConfig);
      checkTableConfigWithHllConfig(tableConfig, tableConfigToCompare);

      ZNRecord znRecord = TableConfig.toZnRecord(tableConfig);
      tableConfigToCompare = TableConfig.fromZnRecord(znRecord);
      checkTableConfigWithHllConfig(tableConfig, tableConfigToCompare);
    }
  }

  private void checkTableConfigWithAssignmentConfig(TableConfig tableConfig, TableConfig tableConfigToCompare) {
    // Check that the segment assignment configuration does exist.
    Assert.assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
    Assert.assertNotNull(tableConfigToCompare.getValidationConfig().getReplicaGroupStrategyConfig());
    Assert.assertEquals(tableConfigToCompare.getValidationConfig().getReplicaGroupStrategyConfig(),
        tableConfig.getValidationConfig().getReplicaGroupStrategyConfig());

    // Check that the configurations are correct.
    ReplicaGroupStrategyConfig strategyConfig =
        tableConfigToCompare.getValidationConfig().getReplicaGroupStrategyConfig();
    Assert.assertEquals(strategyConfig.getMirrorAssignmentAcrossReplicaGroups(), true);
    Assert.assertEquals(strategyConfig.getNumInstancesPerPartition(), 5);
    Assert.assertEquals(strategyConfig.getPartitionColumn(), "memberId");
  }

  private void checkTableConfigWithStarTreeConfig(TableConfig tableConfig, TableConfig tableConfigToCompare)
      throws Exception {
    // Check that the segment assignment configuration does exist.
    Assert.assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
    Assert.assertNotNull(tableConfigToCompare.getValidationConfig().getStarTreeConfig());

    // Check that the configurations are correct.
    StarTreeIndexSpec starTreeIndexSpec = tableConfigToCompare.getValidationConfig().getStarTreeConfig();

    Set<String> dims = new HashSet<>();
    dims.add("dims");

    Assert.assertEquals(starTreeIndexSpec.getDimensionsSplitOrder(), Collections.singletonList("dim"));
    Assert.assertEquals(starTreeIndexSpec.getMaxLeafRecords(), 5);
    Assert.assertEquals(starTreeIndexSpec.getSkipMaterializationCardinalityThreshold(), 1);
    Assert.assertEquals(starTreeIndexSpec.getSkipMaterializationForDimensions(), dims);
    Assert.assertEquals(starTreeIndexSpec.getSkipStarNodeCreationForDimensions(), dims);

    starTreeIndexSpec = StarTreeIndexSpec.fromJsonString(starTreeIndexSpec.toJsonString());
    Assert.assertEquals(starTreeIndexSpec.getDimensionsSplitOrder(), Collections.singletonList("dim"));
    Assert.assertEquals(starTreeIndexSpec.getMaxLeafRecords(), 5);
    Assert.assertEquals(starTreeIndexSpec.getSkipMaterializationCardinalityThreshold(), 1);
    Assert.assertEquals(starTreeIndexSpec.getSkipMaterializationForDimensions(), dims);
    Assert.assertEquals(starTreeIndexSpec.getSkipStarNodeCreationForDimensions(), dims);
  }

  private void checkTableConfigWithHllConfig(TableConfig tableConfig, TableConfig tableConfigToCompare) {
    // Check that the segment assignment configuration does exist.
    Assert.assertEquals(tableConfigToCompare.getTableName(), tableConfig.getTableName());
    Assert.assertNotNull(tableConfigToCompare.getValidationConfig().getHllConfig());

    // Check that the configurations are correct.
    HllConfig hllConfig = tableConfigToCompare.getValidationConfig().getHllConfig();

    Set<String> columns = new HashSet<>();
    columns.add("column");
    columns.add("column2");

    Assert.assertTrue(hllConfig.getColumnsToDeriveHllFields().equals(columns));
    Assert.assertEquals(hllConfig.getHllLog2m(), 9);
    Assert.assertEquals(hllConfig.getHllDeriveColumnSuffix(), "suffix");
  }
}
