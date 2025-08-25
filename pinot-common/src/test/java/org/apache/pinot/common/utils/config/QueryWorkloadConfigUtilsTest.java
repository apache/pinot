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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.workload.CostSplit;
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
    EnforcementProfile validEnforcementProfile = new EnforcementProfile(100, 100);

    // Server node
    CostSplit costSplit1 = new CostSplit("testId", 50, 50, null);
    List<CostSplit> costSplits = List.of(costSplit1);
    PropagationScheme serverPropagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, costSplits);
    NodeConfig serverNodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, validEnforcementProfile,
        serverPropagationScheme);

    // Broker node
    PropagationScheme brokerPropagationScheme = new PropagationScheme(PropagationScheme.Type.TENANT, costSplits);
    NodeConfig brokerNodeConfig = new NodeConfig(NodeConfig.Type.BROKER_NODE, validEnforcementProfile,
        brokerPropagationScheme);

    List<NodeConfig> nodeConfigs = List.of(serverNodeConfig, brokerNodeConfig);
    QueryWorkloadConfig validQueryWorkloadConfig = new QueryWorkloadConfig("workloadId", nodeConfigs);

    // Valid scenario: NODE_CONFIGS field is a JSON array string
    ZNRecord validZnRecord = new ZNRecord("workloadId");
    validZnRecord.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "workloadId");
    validZnRecord.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, JsonUtils.objectToString(nodeConfigs));
    data.add(new Object[] { validZnRecord, validQueryWorkloadConfig, false });

    // Null propagation scheme
    NodeConfig nodeConfigWithoutPropagationScheme = new NodeConfig(NodeConfig.Type.SERVER_NODE, validEnforcementProfile,
            null);
    List<NodeConfig> nodeConfigsWithoutPropagation = List.of(nodeConfigWithoutPropagationScheme);
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

    EnforcementProfile validEnforcementProfile = new EnforcementProfile(100, 100);
    // Server scheme
    CostSplit costSplit1 = new CostSplit("testId", 50, 50, null);
    List<CostSplit> costSplits = List.of(costSplit1);
    PropagationScheme serverPropagationScheme = new PropagationScheme(PropagationScheme.Type.TABLE, costSplits);
    NodeConfig serverNodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, validEnforcementProfile,
        serverPropagationScheme);
    // Broker scheme
    PropagationScheme brokerPropagationScheme = new PropagationScheme(PropagationScheme.Type.TENANT, costSplits);
    NodeConfig brokerNodeConfig = new NodeConfig(NodeConfig.Type.BROKER_NODE, validEnforcementProfile,
        brokerPropagationScheme);
    List<NodeConfig> nodeConfigs = List.of(serverNodeConfig, brokerNodeConfig);
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
    NodeConfig nodeConfigWithoutPropagation = new NodeConfig(NodeConfig.Type.SERVER_NODE, validEnforcementProfile,
        null);
    List<NodeConfig> nodeConfigsWithoutPropagation = List.of(nodeConfigWithoutPropagation);
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

    // 3) Null server node in QueryWorkloadConfig
    List<NodeConfig> nodeConfigsWithNullServerNode = List.of(brokerNodeConfig);
    QueryWorkloadConfig nullServerNodeConfig = new QueryWorkloadConfig("nullServer", nodeConfigsWithNullServerNode);
    ZNRecord znRecordNullServer = new ZNRecord("nullServerId");
    ZNRecord expectedZnRecordNullServer = new ZNRecord("nullServerId");
    znRecordNullServer.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "nullServer");
    expectedZnRecordNullServer.setSimpleField(QueryWorkloadConfig.QUERY_WORKLOAD_NAME, "nullServer");
    String nodeConfigsWithNullServerJson = JsonUtils.objectToString(nodeConfigsWithNullServerNode);
    znRecordNullServer.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, nodeConfigsWithNullServerJson);
    expectedZnRecordNullServer.setSimpleField(QueryWorkloadConfig.NODE_CONFIGS, nodeConfigsWithNullServerJson);
    data.add(new Object[] { nullServerNodeConfig, znRecordNullServer, expectedZnRecordNullServer, false });

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

  // ========== COMPREHENSIVE TESTS FOR validateQueryWorkloadConfig ==========

  @Test
  public void testValidateQueryWorkloadConfigNullConfig() {
    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(null);
    Assert.assertEquals(errors.size(), 1);
    Assert.assertEquals(errors.get(0), "QueryWorkloadConfig cannot be null");
  }

  @Test
  public void testValidateQueryWorkloadConfigValidConfig() {
    QueryWorkloadConfig validConfig = createValidQueryWorkloadConfig();
    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(validConfig);
    Assert.assertTrue(errors.isEmpty(), "Valid config should have no errors, but got: " + errors);
  }

  @Test(dataProvider = "workloadNameValidationProvider")
  public void testValidateQueryWorkloadConfigWorkloadNameValidation(String workloadName, boolean shouldHaveError,
      String expectedErrorSubstring) {
    QueryWorkloadConfig config = createValidQueryWorkloadConfig();
    // Use reflection or create a new config with the test workload name
    QueryWorkloadConfig testConfig = new QueryWorkloadConfig(workloadName, config.getNodeConfigs());

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(testConfig);

    if (shouldHaveError) {
      Assert.assertFalse(errors.isEmpty(), "Expected validation errors for workload name: " + workloadName);
      Assert.assertTrue(errors.stream().anyMatch(error -> error.contains(expectedErrorSubstring)),
          "Expected error containing '" + expectedErrorSubstring + "' but got: " + errors);
    } else {
      Assert.assertTrue(errors.isEmpty(),
          "Expected no errors for valid workload name: " + workloadName + ", but got: " + errors);
    }
  }

  @DataProvider(name = "workloadNameValidationProvider")
  public Object[][] workloadNameValidationProvider() {
    return new Object[][] {
        {null, true, "queryWorkloadName cannot be null or empty"},
        {"", true, "queryWorkloadName cannot be null or empty"},
        {"   ", true, "queryWorkloadName cannot be null or empty"},
        {"\t\n", true, "queryWorkloadName cannot be null or empty"},
        {"validName", false, null},
        {"valid_name_123", false, null},
        {"valid-name", false, null}
    };
  }

  @Test(dataProvider = "nodeConfigsValidationProvider")
  public void testValidateQueryWorkloadConfigNodeConfigsValidation(List<NodeConfig> nodeConfigs,
      List<String> expectedErrors) {
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", nodeConfigs);
    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);

    if (expectedErrors.isEmpty()) {
      Assert.assertTrue(errors.isEmpty(), "Expected no errors but got: " + errors);
    } else {
      for (String expectedError : expectedErrors) {
        Assert.assertTrue(errors.stream().anyMatch(error -> error.contains(expectedError)),
            "Expected error containing '" + expectedError + "' but got: " + errors);
      }
    }
  }

  @DataProvider(name = "nodeConfigsValidationProvider")
  public Object[][] nodeConfigsValidationProvider() {
    List<Object[]> data = new ArrayList<>();

    // Null nodeConfigs
    data.add(new Object[]{null, Arrays.asList("nodeConfigs cannot be null or empty")});

    // Empty nodeConfigs
    data.add(new Object[]{Collections.emptyList(), Arrays.asList("nodeConfigs cannot be null or empty")});

    // NodeConfigs with null element
    List<NodeConfig> nodeConfigsWithNull = new ArrayList<>();
    nodeConfigsWithNull.add(null);
    data.add(new Object[]{nodeConfigsWithNull, Arrays.asList("nodeConfigs[0] cannot be null")});

    // Valid nodeConfigs
    data.add(new Object[]{Arrays.asList(createValidServerNodeConfig()), Collections.emptyList()});

    return data.toArray(new Object[0][]);
  }

  @Test
  public void testValidateQueryWorkloadConfigNodeConfigNullType() {
    NodeConfig nodeConfig = new NodeConfig(null, createValidEnforcementProfile(), createValidPropagationScheme());
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", Arrays.asList(nodeConfig));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);
    Assert.assertTrue(errors.stream().anyMatch(error -> error.contains("nodeConfigs[0].type cannot be null")),
        "Expected node type validation error, but got: " + errors);
  }

  @Test(dataProvider = "enforcementProfileValidationProvider")
  public void testValidateQueryWorkloadConfigEnforcementProfileValidation(EnforcementProfile profile,
      List<String> expectedErrors) {
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, profile, createValidPropagationScheme());
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", Arrays.asList(nodeConfig));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);

    for (String expectedError : expectedErrors) {
      Assert.assertTrue(errors.stream().anyMatch(error -> error.contains(expectedError)),
          "Expected error containing '" + expectedError + "' but got: " + errors);
    }
  }

  @DataProvider(name = "enforcementProfileValidationProvider")
  public Object[][] enforcementProfileValidationProvider() {
    return new Object[][] {
        {null, Arrays.asList("enforcementProfile cannot be null")},
        {new EnforcementProfile(-1, 100), Arrays.asList("enforcementProfile.cpuCostNs cannot be negative")},
        {new EnforcementProfile(100, -1), Arrays.asList("enforcementProfile.memoryCostBytes cannot be negative")},
        {new EnforcementProfile(-1, -1), Arrays.asList("enforcementProfile.cpuCostNs cannot be negative",
            "enforcementProfile.memoryCostBytes cannot be negative")},
        {new EnforcementProfile(0, 0), Collections.emptyList()}, // Zero values are allowed
        {new EnforcementProfile(100, 100), Collections.emptyList()}
    };
  }

  @Test(dataProvider = "propagationSchemeValidationProvider")
  public void testValidateQueryWorkloadConfigPropagationSchemeValidation(PropagationScheme scheme,
      List<String> expectedErrors) {
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), scheme);
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", Arrays.asList(nodeConfig));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);

    for (String expectedError : expectedErrors) {
      Assert.assertTrue(errors.stream().anyMatch(error -> error.contains(expectedError)),
          "Expected error containing '" + expectedError + "' but got: " + errors);
    }
  }

  @DataProvider(name = "propagationSchemeValidationProvider")
  public Object[][] propagationSchemeValidationProvider() {
    List<Object[]> data = new ArrayList<>();

    // Null propagation scheme
    data.add(new Object[]{null, Arrays.asList("propagationScheme cannot be null")});

    // Null propagation type
    PropagationScheme schemeWithNullType = new PropagationScheme(null, Arrays.asList(createValidCostSplit()));
    data.add(new Object[]{schemeWithNullType, Arrays.asList("propagationScheme.type cannot be null")});

    // Valid propagation scheme
    data.add(new Object[]{createValidPropagationScheme(), Collections.emptyList()});

    return data.toArray(new Object[0][]);
  }

  @Test(dataProvider = "costSplitsValidationProvider")
  public void testValidateQueryWorkloadConfigCostSplitsValidation(List<CostSplit> costSplits,
      List<String> expectedErrors) {
    PropagationScheme scheme = new PropagationScheme(PropagationScheme.Type.TABLE, costSplits);
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), scheme);
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", Arrays.asList(nodeConfig));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);

    for (String expectedError : expectedErrors) {
      Assert.assertTrue(errors.stream().anyMatch(error -> error.contains(expectedError)),
          "Expected error containing '" + expectedError + "' but got: " + errors);
    }
  }

  @DataProvider(name = "costSplitsValidationProvider")
  public Object[][] costSplitsValidationProvider() {
    List<Object[]> data = new ArrayList<>();

    // Null costSplits
    data.add(new Object[]{null, Arrays.asList("costSplits cannot be null")});

    // Empty costSplits
    data.add(new Object[]{Collections.emptyList(), Arrays.asList("costSplits cannot be empty")});

    // CostSplits with null element
    List<CostSplit> costSplitsWithNull = new ArrayList<>();
    costSplitsWithNull.add(null);
    data.add(new Object[]{costSplitsWithNull, Arrays.asList("costSplits[0] cannot be null")});

    // CostSplit with null costId
    CostSplit costSplitWithNullId = new CostSplit(null, 100, 100, null);
    data.add(new Object[]{Arrays.asList(costSplitWithNullId), Arrays.asList("costId cannot be null or empty")});

    // CostSplit with empty costId
    CostSplit costSplitWithEmptyId = new CostSplit("", 100, 100, null);
    data.add(new Object[]{Arrays.asList(costSplitWithEmptyId), Arrays.asList("costId cannot be null or empty")});

    // CostSplit with whitespace-only costId
    CostSplit costSplitWithWhitespaceId = new CostSplit("   ", 100, 100, null);
    data.add(new Object[]{Arrays.asList(costSplitWithWhitespaceId),
        Arrays.asList("costId cannot be null or empty")});

    // CostSplit with negative CPU cost
    CostSplit costSplitWithNegativeCpu = new CostSplit("test", -1, 100, null);
    data.add(new Object[]{Arrays.asList(costSplitWithNegativeCpu),
        Arrays.asList("cpuCostNs cannot be negative")});

    // CostSplit with zero CPU cost
    CostSplit costSplitWithZeroCpu = new CostSplit("test", 0, 100, null);
    data.add(new Object[]{Arrays.asList(costSplitWithZeroCpu), Arrays.asList("cpuCostNs should be positive")});

    // CostSplit with negative memory cost
    CostSplit costSplitWithNegativeMemory = new CostSplit("test", 100, -1, null);
    data.add(new Object[]{Arrays.asList(costSplitWithNegativeMemory),
        Arrays.asList("memoryCostBytes cannot be negative")});

    // CostSplit with zero memory cost
    CostSplit costSplitWithZeroMemory = new CostSplit("test", 100, 0, null);
    data.add(new Object[]{Arrays.asList(costSplitWithZeroMemory),
        Arrays.asList("memoryCostBytes should be positive")});

    // Valid costSplits
    data.add(new Object[]{Arrays.asList(createValidCostSplit()), Collections.emptyList()});

    return data.toArray(new Object[0][]);
  }

  @Test
  public void testValidateQueryWorkloadConfigDuplicateCostIds() {
    CostSplit costSplit1 = new CostSplit("duplicate", 100, 100, null);
    CostSplit costSplit2 = new CostSplit("duplicate", 200, 200, null);

    PropagationScheme scheme = new PropagationScheme(PropagationScheme.Type.TABLE,
        Arrays.asList(costSplit1, costSplit2));
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), scheme);
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", Arrays.asList(nodeConfig));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);
    Assert.assertTrue(errors.stream().anyMatch(error -> error.contains("costId 'duplicate' is duplicated")),
        "Expected duplicate costId error, but got: " + errors);
  }

  @Test(dataProvider = "costIdFormatValidationProvider")
  public void testValidateQueryWorkloadConfigCostIdFormatValidation(String costId, boolean shouldHaveError) {
    CostSplit costSplit = new CostSplit(costId, 100, 100, null);
    PropagationScheme scheme = new PropagationScheme(PropagationScheme.Type.TABLE, Arrays.asList(costSplit));
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), scheme);
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", Arrays.asList(nodeConfig));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);

    if (shouldHaveError) {
      Assert.assertTrue(errors.stream().anyMatch(error -> error.contains("contains invalid characters")),
          "Expected invalid character error for costId: " + costId + ", but got: " + errors);
    } else {
      Assert.assertFalse(errors.stream().anyMatch(error -> error.contains("contains invalid characters")),
          "Expected no invalid character error for costId: " + costId + ", but got: " + errors);
    }
  }

  @DataProvider(name = "costIdFormatValidationProvider")
  public Object[][] costIdFormatValidationProvider() {
    return new Object[][] {
        {"validId", false},
        {"valid_id", false},
        {"valid-id", false},
        {"valid.id", false},
        {"valid123", false},
        {"123valid", false},
        {"Valid_ID-123.test", false},
        {"invalid id", true}, // space
        {"invalid@id", true}, // @ symbol
        {"invalid#id", true}, // # symbol
        {"invalid$id", true}, // $ symbol
        {"invalid%id", true}, // % symbol
        {"invalid&id", true}, // & symbol
        {"invalid*id", true}, // * symbol
        {"invalid(id)", true}, // parentheses
        {"invalid[id]", true}, // brackets
        {"invalid{id}", true}, // braces
        {"invalid/id", true}, // slash
        {"invalid\\id", true}, // backslash
        {"invalid|id", true}, // pipe
        {"invalid+id", true}, // plus
        {"invalid=id", true}, // equals
        {"invalid?id", true}, // question mark
        {"invalid<id>", true}, // angle brackets
        {"invalid,id", true}, // comma
        {"invalid;id", true}, // semicolon
        {"invalid:id", true}, // colon
        {"invalid\"id\"", true}, // quotes
        {"invalid'id'", true} // single quotes
    };
  }

  @Test
  public void testValidateQueryWorkloadConfigCostOverflow() {
    // Create cost splits that would cause overflow when summed
    CostSplit costSplit1 = new CostSplit("cost1", Long.MAX_VALUE, Long.MAX_VALUE, null);
    CostSplit costSplit2 = new CostSplit("cost2", 1, 1, null);

    PropagationScheme scheme = new PropagationScheme(PropagationScheme.Type.TABLE,
        Arrays.asList(costSplit1, costSplit2));
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), scheme);
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", Arrays.asList(nodeConfig));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);
    Assert.assertTrue(errors.stream().anyMatch(error -> error.contains("total CPU cost would overflow")),
        "Expected CPU overflow error, but got: " + errors);
    Assert.assertTrue(errors.stream().anyMatch(error -> error.contains("total memory cost would overflow")),
        "Expected memory overflow error, but got: " + errors);
  }

  @Test
  public void testValidateQueryWorkloadConfigSubAllocationsValidation() {
    // Create sub-allocations that exceed parent limits
    CostSplit subAllocation1 = new CostSplit("sub1", 60, 60, null);
    CostSplit subAllocation2 = new CostSplit("sub2", 60, 60, null);
    List<CostSplit> subAllocations = Arrays.asList(subAllocation1, subAllocation2);

    // Parent has limits of 100 each, but sub-allocations total 120 each
    CostSplit parentCostSplit = new CostSplit("parent", 100, 100, subAllocations);

    PropagationScheme scheme = new PropagationScheme(PropagationScheme.Type.TABLE, Arrays.asList(parentCostSplit));
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), scheme);
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", Arrays.asList(nodeConfig));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);
    Assert.assertTrue(errors.stream().anyMatch(error -> error.contains("exceeds parent limit")),
        "Expected parent limit exceeded error, but got: " + errors);
  }

  @Test
  public void testValidateQueryWorkloadConfigNestedSubAllocations() {
    // Test that nested sub-allocations are not allowed
    CostSplit nestedSubAllocation = new CostSplit("nested", 10, 10, null);
    CostSplit subAllocation = new CostSplit("sub", 50, 50, Arrays.asList(nestedSubAllocation));
    CostSplit parentCostSplit = new CostSplit("parent", 100, 100, Arrays.asList(subAllocation));

    PropagationScheme scheme = new PropagationScheme(PropagationScheme.Type.TABLE, Arrays.asList(parentCostSplit));
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), scheme);
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", Arrays.asList(nodeConfig));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);
    Assert.assertTrue(errors.stream().anyMatch(error -> error.contains("nested sub-allocations are not supported")),
        "Expected nested sub-allocations error, but got: " + errors);
  }

  @Test
  public void testValidateQueryWorkloadConfigDuplicateSubAllocationIds() {
    CostSplit subAllocation1 = new CostSplit("duplicate", 30, 30, null);
    CostSplit subAllocation2 = new CostSplit("duplicate", 40, 40, null);
    List<CostSplit> subAllocations = Arrays.asList(subAllocation1, subAllocation2);

    CostSplit parentCostSplit = new CostSplit("parent", 100, 100, subAllocations);

    PropagationScheme scheme = new PropagationScheme(PropagationScheme.Type.TABLE, Arrays.asList(parentCostSplit));
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), scheme);
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", Arrays.asList(nodeConfig));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);
    Assert.assertTrue(errors.stream().anyMatch(error -> error.contains("is duplicated within sub-allocations")),
        "Expected duplicate sub-allocation ID error, but got: " + errors);
  }

  @Test
  public void testValidateQueryWorkloadConfigComplexValidConfig() {
    // Test a complex but valid configuration with multiple nodes and sub-allocations
    CostSplit subAllocation1 = new CostSplit("sub1", 30, 30, null);
    CostSplit subAllocation2 = new CostSplit("sub2", 40, 40, null);
    List<CostSplit> subAllocations = Arrays.asList(subAllocation1, subAllocation2);

    CostSplit costSplit1 = new CostSplit("cost1", 100, 100, subAllocations);
    CostSplit costSplit2 = new CostSplit("cost2", 200, 200, null);

    PropagationScheme serverScheme = new PropagationScheme(PropagationScheme.Type.TABLE, Arrays.asList(costSplit1,
        costSplit2));
    NodeConfig serverNode = new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), serverScheme);

    PropagationScheme brokerScheme = new PropagationScheme(PropagationScheme.Type.TENANT, Arrays.asList(costSplit2));
    NodeConfig brokerNode = new NodeConfig(NodeConfig.Type.BROKER_NODE, createValidEnforcementProfile(), brokerScheme);

    QueryWorkloadConfig config = new QueryWorkloadConfig("complexWorkload", Arrays.asList(serverNode, brokerNode));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);
    Assert.assertTrue(errors.isEmpty(), "Complex valid config should have no errors, but got: " + errors);
  }

  // ========== HELPER METHODS ==========

  private QueryWorkloadConfig createValidQueryWorkloadConfig() {
    return new QueryWorkloadConfig("testWorkload", Arrays.asList(createValidServerNodeConfig()));
  }

  private NodeConfig createValidServerNodeConfig() {
    return new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), createValidPropagationScheme());
  }

  private EnforcementProfile createValidEnforcementProfile() {
    return new EnforcementProfile(1000, 1000);
  }

  private PropagationScheme createValidPropagationScheme() {
    return new PropagationScheme(PropagationScheme.Type.TABLE, Arrays.asList(createValidCostSplit()));
  }

  private CostSplit createValidCostSplit() {
    return new CostSplit("validCostId", 100, 100, null);
  }
}
