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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryWorkloadConfigUtilsTest {

  @Test(dataProvider = "fromZNRecordDataProvider")
  public void testFromZNRecord(ZNRecord znRecord, QueryWorkloadConfig expectedQueryWorkloadConfig, boolean shouldFail) {
    try {
      QueryWorkloadConfig actualQueryWorkloadConfig = QueryWorkloadConfigUtils.fromZNRecord(znRecord);
      if (shouldFail) {
        Assert.fail("Expected an exception but none was thrown");
      }
      Assert.assertEquals(actualQueryWorkloadConfig, expectedQueryWorkloadConfig);
    } catch (Exception e) {
      if (!shouldFail) {
        Assert.fail("Caught unexpected exception: " + e.getMessage(), e);
      }
    }
  }

  @DataProvider(name = "fromZNRecordDataProvider")
  public Object[][] fromZNRecordDataProvider() throws JsonProcessingException {
    List<Object[]> data = new ArrayList<>();

    // Shared, valid configuration
    EnforcementProfile validEnforcementProfile = new EnforcementProfile(100, 100 ,100L);

    // Leaf node
    PropagationScheme leafPropagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE,
        List.of("value1", "value2"));
    NodeConfig leafNodeConfig = new NodeConfig(validEnforcementProfile, leafPropagationScheme);

    // Non-leaf node
    PropagationScheme nonLeafPropagationScheme = new PropagationScheme(PropagationScheme.Type.TENANT,
        List.of("value3", "value4"));
    NodeConfig nonLeafNodeConfig = new NodeConfig(validEnforcementProfile, nonLeafPropagationScheme);

    Map<NodeConfig.Type, NodeConfig> nodeConfigs = Map.of(NodeConfig.Type.LEAF_NODE, leafNodeConfig,
        NodeConfig.Type.NON_LEAF_NODE, nonLeafNodeConfig);
    QueryWorkloadConfig validQueryWorkloadConfig = new QueryWorkloadConfig("workloadId", nodeConfigs);

    // Valid scenario: NODE_CONFIGS field is a JSON array string
    ZNRecord validZnRecord = new ZNRecord("workloadId");
    validZnRecord.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "workloadId");
    validZnRecord.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, JsonUtils.objectToString(nodeConfigs));
    data.add(new Object[] { validZnRecord, validQueryWorkloadConfig, false });

    // Null propagation scheme
    NodeConfig nodeConfigWithoutPropagationScheme = new NodeConfig(validEnforcementProfile, null);
    Map<NodeConfig.Type, NodeConfig> nodeConfigsWithoutPropagation = Map.of(NodeConfig.Type.LEAF_NODE,
        nodeConfigWithoutPropagationScheme);
    ZNRecord znRecordNullPropagation = new ZNRecord("workloadId");
    znRecordNullPropagation.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "workloadId");
    znRecordNullPropagation.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS,
        JsonUtils.objectToString(nodeConfigsWithoutPropagation));
    QueryWorkloadConfig expectedQueryWorkloadConfigNullPropagation = new QueryWorkloadConfig("workloadId",
        nodeConfigsWithoutPropagation);
    data.add(new Object[] { znRecordNullPropagation, expectedQueryWorkloadConfigNullPropagation, false });

    // Missing NODE_CONFIGS field
    ZNRecord missingNodeConfigsZnRecord = new ZNRecord("workloadId");
    missingNodeConfigsZnRecord.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "workloadId");
    data.add(new Object[] { missingNodeConfigsZnRecord, null, true });

    // Invalid JSON in NODE_CONFIGS field
    ZNRecord invalidJsonZnRecord = new ZNRecord("workloadId");
    invalidJsonZnRecord.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "workloadId");
    invalidJsonZnRecord.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, "{invalidJsonField: }");
    data.add(new Object[] { invalidJsonZnRecord, null, true });

    return data.toArray(new Object[0][]);
  }

  @Test(dataProvider = "updateZNRecordDataProvider")
  public void testUpdateZNRecordWithWorkloadConfig(QueryWorkloadConfig queryWorkloadConfig, ZNRecord znRecord,
      ZNRecord expectedZnRecord, boolean shouldFail) {
    try {
      QueryWorkloadConfigUtils.updateZNRecordWithWorkloadConfig(znRecord, queryWorkloadConfig);
      if (shouldFail) {
        Assert.fail("Expected an exception but none was thrown");
      }
      Assert.assertEquals(znRecord, expectedZnRecord);
    } catch (Exception e) {
      if (!shouldFail) {
        Assert.fail("Caught unexpected exception: " + e.getMessage(), e);
      }
    }
  }

  @DataProvider(name = "updateZNRecordDataProvider")
  public Object[][] updateZNRecordDataProvider() throws JsonProcessingException {
    List<Object[]> data = new ArrayList<>();

    EnforcementProfile validEnforcementProfile = new EnforcementProfile(100, 100, 100L);
    // Leaf node
    PropagationScheme leafPropagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE,
        List.of("value1", "value2"));
    NodeConfig leafNodeConfig = new NodeConfig(validEnforcementProfile, leafPropagationScheme);
    // Non-leaf node
    PropagationScheme nonLeafPropagationScheme = new PropagationScheme(PropagationScheme.Type.TENANT,
        List.of("value3", "value4"));
    NodeConfig nonLeafNodeConfig = new NodeConfig(validEnforcementProfile, nonLeafPropagationScheme);
    Map<NodeConfig.Type, NodeConfig> nodeConfigs = Map.of(NodeConfig.Type.LEAF_NODE, leafNodeConfig,
        NodeConfig.Type.NON_LEAF_NODE, nonLeafNodeConfig);
    QueryWorkloadConfig validQueryWorkloadConfig = new QueryWorkloadConfig("workloadId", nodeConfigs);

    // 1) Valid scenario
    ZNRecord validZnRecord = new ZNRecord("validId");
    ZNRecord expectedValidZnRecord = new ZNRecord("validId");
    validZnRecord.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "workloadId");
    expectedValidZnRecord.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "workloadId");
    String nodeConfigsJson = JsonUtils.objectToString(nodeConfigs);
    validZnRecord.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, nodeConfigsJson);
    expectedValidZnRecord.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, nodeConfigsJson);
    data.add(new Object[] { validQueryWorkloadConfig, validZnRecord, expectedValidZnRecord, false });

    // 2) Null propagation scheme in both nodes
    NodeConfig nodeConfigWithoutPropagation = new NodeConfig(validEnforcementProfile, null);
    Map<NodeConfig.Type, NodeConfig> nodeConfigsWithoutPropagation = Map.of(NodeConfig.Type.LEAF_NODE,
        nodeConfigWithoutPropagation, NodeConfig.Type.NON_LEAF_NODE, nodeConfigWithoutPropagation);
    QueryWorkloadConfig configWithoutPropagation = new QueryWorkloadConfig("noPropagation",
        nodeConfigsWithoutPropagation);

    String nodeConfigsNoPropagationJson = JsonUtils.objectToString(nodeConfigsWithoutPropagation);

    ZNRecord znRecordNoPropagation = new ZNRecord("noPropagationId");
    ZNRecord expectedZnRecordNoPropagation = new ZNRecord("noPropagationId");
    znRecordNoPropagation.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "noPropagation");
    znRecordNoPropagation.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, nodeConfigsNoPropagationJson);

    expectedZnRecordNoPropagation.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "noPropagation");
    expectedZnRecordNoPropagation.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, nodeConfigsNoPropagationJson);
    data.add(new Object[] { configWithoutPropagation, znRecordNoPropagation, expectedZnRecordNoPropagation, false });

    // 3) Null leaf node in QueryWorkloadConfig
    Map<NodeConfig.Type, NodeConfig> nodeConfigsWithNullLeaf = Map.of(NodeConfig.Type.NON_LEAF_NODE, nonLeafNodeConfig);
    QueryWorkloadConfig nullLeafNodeConfig = new QueryWorkloadConfig("nullLeaf", nodeConfigsWithNullLeaf);
    ZNRecord znRecordNullLeaf = new ZNRecord("nullLeafId");
    ZNRecord expectedZnRecordNullLeaf = new ZNRecord("nullLeafId");
    znRecordNullLeaf.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "nullLeaf");
    expectedZnRecordNullLeaf.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "nullLeaf");
    String nodeConfigsWithNullLeafJson = JsonUtils.objectToString(nodeConfigsWithNullLeaf);
    znRecordNullLeaf.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, nodeConfigsWithNullLeafJson);
    expectedZnRecordNullLeaf.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, nodeConfigsWithNullLeafJson);
    data.add(new Object[] { nullLeafNodeConfig, znRecordNullLeaf, expectedZnRecordNullLeaf, false });

    // 4) Null QueryWorkloadConfig -> should fail
    ZNRecord znRecordNullConfig = new ZNRecord("nullConfigId");
    data.add(new Object[] { null, znRecordNullConfig, null, true });

    // 5) Null ZNRecord -> should fail
    data.add(new Object[] { validQueryWorkloadConfig, null, null, true });

    // 6) Behavior with empty ZNRecord ID
    ZNRecord emptyIdZnRecord = new ZNRecord("");
    ZNRecord expectedEmptyIdZnRecord = new ZNRecord("");
    emptyIdZnRecord.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "workloadId");
    expectedEmptyIdZnRecord.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "workloadId");
    String emptyNodeConfigsJson = JsonUtils.objectToString(nodeConfigs);
    emptyIdZnRecord.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, emptyNodeConfigsJson);
    expectedEmptyIdZnRecord.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, emptyNodeConfigsJson);
    data.add(new Object[] { validQueryWorkloadConfig, emptyIdZnRecord, expectedEmptyIdZnRecord, false });

    return data.toArray(new Object[0][]);
  }
}
