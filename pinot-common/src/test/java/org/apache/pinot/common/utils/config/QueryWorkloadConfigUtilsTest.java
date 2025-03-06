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

import java.util.ArrayList;
import java.util.List;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryWorkloadConfigUtilsTest {

  @Test(dataProvider = "fromZNRecordDataProvider")
  public void testFromZNRecord(ZNRecord znRecord, QueryWorkloadConfig expectedQueryWorkloadConfig, boolean shouldFail) {
    try {
      QueryWorkloadConfig actualQueryWorkloadConfig = WorkloadConfigUtils.fromZNRecord(znRecord);
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
  public Object[][] fromZNRecordDataProvider() {
    List<Object[]> data = new ArrayList<>();

    // Shared, valid configurations
    EnforcementProfile validEnforcementProfile = new EnforcementProfile(100, 100, 100);

    // Leaf node
    PropagationScheme leafPropagationScheme = new PropagationScheme("TABLE", List.of("value1", "value2"));
    NodeConfig leafNodeConfig = new NodeConfig(validEnforcementProfile, leafPropagationScheme);

    // Non-leaf node
    PropagationScheme nonLeafPropagationScheme = new PropagationScheme("TENANT", List.of("value3", "value4"));
    NodeConfig nonLeafNodeConfig = new NodeConfig(validEnforcementProfile, nonLeafPropagationScheme);

    // A fully valid QueryWorkloadConfig
    QueryWorkloadConfig validQueryWorkloadConfig = new QueryWorkloadConfig("workloadId",
        leafNodeConfig, nonLeafNodeConfig);

    // Valid scenario
    ZNRecord validZnRecord = new ZNRecord("workloadId");
    validZnRecord.setSimpleField(QueryWorkloadConfig.LEAF_NODE, leafNodeConfig.toJsonString());
    validZnRecord.setSimpleField(QueryWorkloadConfig.NON_LEAF_NODE, nonLeafNodeConfig.toJsonString());
    data.add(new Object[] { validZnRecord, validQueryWorkloadConfig, false });

    // Null propagation scheme
    NodeConfig nodeConfigWithoutPropagationScheme = new NodeConfig(validEnforcementProfile, null);
    ZNRecord znRecord = new ZNRecord("workloadId");
    znRecord.setSimpleField(QueryWorkloadConfig.LEAF_NODE, nodeConfigWithoutPropagationScheme.toJsonString());
    znRecord.setSimpleField(QueryWorkloadConfig.NON_LEAF_NODE, nodeConfigWithoutPropagationScheme.toJsonString());
    QueryWorkloadConfig expectedQueryWorkloadConfig = new QueryWorkloadConfig("workloadId",
        nodeConfigWithoutPropagationScheme, nodeConfigWithoutPropagationScheme);
    data.add(new Object[] { znRecord, expectedQueryWorkloadConfig, false });

    // Null propagation scheme in leaf node
    NodeConfig leafNodeConfigWithoutPropagationScheme = new NodeConfig(validEnforcementProfile, null);
    ZNRecord znRecordWithNullLeafNode = new ZNRecord("workloadId");
    znRecordWithNullLeafNode.setSimpleField(QueryWorkloadConfig.LEAF_NODE,
        leafNodeConfigWithoutPropagationScheme.toJsonString());
    znRecordWithNullLeafNode.setSimpleField(QueryWorkloadConfig.NON_LEAF_NODE, nonLeafNodeConfig.toJsonString());
    QueryWorkloadConfig expectedQueryWorkloadConfigWithNullLeafNode = new QueryWorkloadConfig("workloadId",
        leafNodeConfigWithoutPropagationScheme, nonLeafNodeConfig);
    data.add(new Object[] { znRecordWithNullLeafNode, expectedQueryWorkloadConfigWithNullLeafNode, false });

    // Missing LEAF_NODE
    ZNRecord missingLeafNodeZnRecord = new ZNRecord("workloadId");
    missingLeafNodeZnRecord.setSimpleField(QueryWorkloadConfig.NON_LEAF_NODE, nonLeafNodeConfig.toJsonString());
    data.add(new Object[] { missingLeafNodeZnRecord, null, true });

    // 3) Missing NON_LEAF_NODE
    ZNRecord missingNonLeafNodeZnRecord = new ZNRecord("workloadId");
    missingNonLeafNodeZnRecord.setSimpleField(QueryWorkloadConfig.LEAF_NODE, leafNodeConfig.toJsonString());
    data.add(new Object[] { missingNonLeafNodeZnRecord, null, true });

    // Invalid JSON in LEAF_NODE
    ZNRecord invalidLeafNodeZnRecord = new ZNRecord("workloadId");
    invalidLeafNodeZnRecord.setSimpleField(QueryWorkloadConfig.LEAF_NODE, "{invalidJsonField: }");
    invalidLeafNodeZnRecord.setSimpleField(QueryWorkloadConfig.NON_LEAF_NODE, nonLeafNodeConfig.toJsonString());
    data.add(new Object[] { invalidLeafNodeZnRecord, null, true });

    // Invalid JSON in NON_LEAF_NODE
    ZNRecord invalidNonLeafNodeZnRecord = new ZNRecord("workloadId");
    invalidNonLeafNodeZnRecord.setSimpleField(QueryWorkloadConfig.LEAF_NODE, leafNodeConfig.toJsonString());
    invalidNonLeafNodeZnRecord.setSimpleField(QueryWorkloadConfig.NON_LEAF_NODE, "notEvenJson");
    data.add(new Object[] { invalidNonLeafNodeZnRecord, null, true });

    return data.toArray(new Object[0][]);
  }

  @Test(dataProvider = "updateZNRecordDataProvider")
  public void testUpdateZNRecordWithWorkloadConfig(QueryWorkloadConfig queryWorkloadConfig, ZNRecord znRecord,
      ZNRecord expectedZnRecord, boolean shouldFail) {
    try {
      WorkloadConfigUtils.updateZNRecordWithWorkloadConfig(znRecord, queryWorkloadConfig);
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
  public Object[][] updateZNRecordDataProvider() {
    List<Object[]> data = new ArrayList<>();

    EnforcementProfile validEnforcementProfile = new EnforcementProfile(100, 100, 100);
    // Leaf node
    PropagationScheme leafPropagationScheme = new PropagationScheme("TABLE", List.of("value1", "value2"));
    NodeConfig leafNodeConfig = new NodeConfig(validEnforcementProfile, leafPropagationScheme);
    // Non-leaf node
    PropagationScheme nonLeafPropagationScheme = new PropagationScheme("TENANT", List.of("value3", "value4"));
    NodeConfig nonLeafNodeConfig = new NodeConfig(validEnforcementProfile, nonLeafPropagationScheme);
    // A fully valid QueryWorkloadConfig
    QueryWorkloadConfig validQueryWorkloadConfig =
        new QueryWorkloadConfig("workloadId", leafNodeConfig, nonLeafNodeConfig);

    // 1) Valid scenario
    ZNRecord validZnRecord = new ZNRecord("validId");
    ZNRecord expectedValidZnRecord = new ZNRecord("validId");
    expectedValidZnRecord.setSimpleField(QueryWorkloadConfig.LEAF_NODE, leafNodeConfig.toJsonString());
    expectedValidZnRecord.setSimpleField(QueryWorkloadConfig.NON_LEAF_NODE, nonLeafNodeConfig.toJsonString());
    data.add(new Object[] { validQueryWorkloadConfig, validZnRecord, expectedValidZnRecord, false });

    // 2) Null propagation scheme in both leaf and non-leaf
    NodeConfig nodeConfigWithoutPropagation = new NodeConfig(validEnforcementProfile, null);
    QueryWorkloadConfig configWithoutPropagation =
        new QueryWorkloadConfig("noPropagation", nodeConfigWithoutPropagation, nodeConfigWithoutPropagation);
    ZNRecord znRecordNoPropagation = new ZNRecord("noPropagationId");
    ZNRecord expectedZnRecordNoPropagation = new ZNRecord("noPropagationId");
    expectedZnRecordNoPropagation.setSimpleField(QueryWorkloadConfig.LEAF_NODE,
        nodeConfigWithoutPropagation.toJsonString());
    expectedZnRecordNoPropagation.setSimpleField(QueryWorkloadConfig.NON_LEAF_NODE,
        nodeConfigWithoutPropagation.toJsonString());
    data.add(new Object[] { configWithoutPropagation, znRecordNoPropagation, expectedZnRecordNoPropagation, false });

    // 3) Null leaf node
    QueryWorkloadConfig nullLeafNodeConfig =
        new QueryWorkloadConfig("nullLeaf", null, nonLeafNodeConfig);
    ZNRecord znRecordNullLeaf = new ZNRecord("nullLeafId");
    data.add(new Object[] { nullLeafNodeConfig, znRecordNullLeaf, null, true });

    // 4) Null non-leaf node
    QueryWorkloadConfig nullNonLeafNodeConfig =
        new QueryWorkloadConfig("nullNonLeaf", leafNodeConfig, null);
    ZNRecord znRecordNullNonLeaf = new ZNRecord("nullNonLeafId");
    data.add(new Object[] { nullNonLeafNodeConfig, znRecordNullNonLeaf, null, true });

    // 5) Null QueryWorkloadConfig -> should fail due to precondition
    ZNRecord znRecordNullConfig = new ZNRecord("nullConfigId");
    data.add(new Object[] { null, znRecordNullConfig, null, true });

    // 6) Null ZNRecord -> should fail due to precondition
    data.add(new Object[] { validQueryWorkloadConfig, null, null, true });

    // 7) Check behavior with empty ZNRecord ID (still a valid scenario unless your logic forbids it)
    ZNRecord emptyIdZnRecord = new ZNRecord("");
    ZNRecord expectedEmptyIdZnRecord = new ZNRecord("");
    expectedEmptyIdZnRecord.setSimpleField(QueryWorkloadConfig.LEAF_NODE, leafNodeConfig.toJsonString());
    expectedEmptyIdZnRecord.setSimpleField(QueryWorkloadConfig.NON_LEAF_NODE, nonLeafNodeConfig.toJsonString());
    data.add(new Object[] { validQueryWorkloadConfig, emptyIdZnRecord, expectedEmptyIdZnRecord, false });

    return data.toArray(new Object[0][]);
  }
}
