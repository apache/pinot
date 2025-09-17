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
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationEntityOverrides;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class QueryWorkloadConfigUtilsTest {

  @Test(dataProvider = "fromZNRecordDataProvider")
  public void testFromZNRecord(ZNRecord znRecord, QueryWorkloadConfig expectedQueryWorkloadConfig,
                               boolean shouldFail) {
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
    PropagationEntity entity1 = new PropagationEntity("testId", 50L, 50L, null);
    List<PropagationEntity> propagationEntities = List.of(entity1);
    PropagationScheme serverPropagationScheme
        = new PropagationScheme(PropagationScheme.Type.TABLE, propagationEntities);
    NodeConfig serverNodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, validEnforcementProfile,
        serverPropagationScheme);

    // Broker node
    PropagationScheme brokerPropagationScheme
        = new PropagationScheme(PropagationScheme.Type.TENANT, propagationEntities);
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
    PropagationEntity entity1 = new PropagationEntity("testId", 50L, 50L, null);
    List<PropagationEntity> propagationEntities = List.of(entity1);
    PropagationScheme serverPropagationScheme
        = new PropagationScheme(PropagationScheme.Type.TABLE, propagationEntities);
    NodeConfig serverNodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, validEnforcementProfile,
        serverPropagationScheme);
    // Broker scheme
    PropagationScheme brokerPropagationScheme
        = new PropagationScheme(PropagationScheme.Type.TENANT, propagationEntities);
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

  @Test
  public void testValidateQueryWorkloadConfigNullConfig() {
    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(null);
    Assert.assertEquals(errors.size(), 1);
    Assert.assertEquals(errors.get(0), "QueryWorkloadConfig cannot be null");
  }

  @Test
  public void testValidateQueryWorkloadConfigValidConfig() {
    QueryWorkloadConfig validConfig = new QueryWorkloadConfig("testWorkload",
        Arrays.asList(createValidServerNodeConfig()));
    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(validConfig);
    Assert.assertTrue(errors.isEmpty(), "Valid config should have no errors, but got: " + errors);
  }

  @Test(dataProvider = "workloadNameValidationProvider")
  public void testValidateQueryWorkloadConfigWorkloadNameValidation(String workloadName, boolean shouldHaveError,
      String expectedErrorSubstring) {
    QueryWorkloadConfig testConfig = new QueryWorkloadConfig(workloadName,
        Arrays.asList(createValidServerNodeConfig()));
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
    PropagationScheme schemeWithNullType
        = new PropagationScheme(null, Arrays.asList(createValidPropagationEntity()));
    data.add(new Object[]{schemeWithNullType, Arrays.asList("propagationScheme.type cannot be null")});

    // Valid propagation scheme
    data.add(new Object[]{createValidPropagationScheme(), Collections.emptyList()});

    return data.toArray(new Object[0][]);
  }

  @Test(dataProvider = "propagationEntitiesValidationProvider")
  public void testValidateQueryWorkloadConfigPropagationEntitysValidation(List<PropagationEntity> propagationEntities,
      List<String> expectedErrors) {
    PropagationScheme scheme = new PropagationScheme(PropagationScheme.Type.TABLE, propagationEntities);
    NodeConfig nodeConfig = new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), scheme);
    QueryWorkloadConfig config = new QueryWorkloadConfig("testWorkload", Arrays.asList(nodeConfig));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);

    for (String expectedError : expectedErrors) {
      Assert.assertTrue(errors.stream().anyMatch(error -> error.contains(expectedError)),
          "Expected error containing '" + expectedError + "' but got: " + errors);
    }
  }

  @DataProvider(name = "propagationEntitiesValidationProvider")
  public Object[][] propagationEntitiesValidationProvider() {
    List<Object[]> data = new ArrayList<>();

    // Null propagationEntities
    data.add(new Object[]{null, Arrays.asList("propagationEntities cannot be null or empty")});

    // PropagationEntitys with null element
    List<PropagationEntity> propagationEntitiesWithNull = new ArrayList<>();
    propagationEntitiesWithNull.add(null);
    data.add(new Object[]{propagationEntitiesWithNull, Arrays.asList("propagationEntities[0] cannot be null")});

    // PropagationEntity with null entityId
    PropagationEntity propagationEntityWithNullId = new PropagationEntity(null, 100L, 100L, null);
    data.add(new Object[]{Arrays.asList(propagationEntityWithNullId),
        Arrays.asList("propagationEntity cannot be null or empty")});

    // PropagationEntity with empty propagationEntity
    PropagationEntity propagationEntityWithEmptyId = new PropagationEntity("", 100L, 100L, null);
    data.add(new Object[]{Arrays.asList(propagationEntityWithEmptyId),
        Arrays.asList("propagationEntity cannot be null or empty")});

    // PropagationEntity with whitespace-only propagationEntity
    PropagationEntity propagationEntityWithWhitespaceId = new PropagationEntity("   ", 100L, 100L, null);
    data.add(new Object[]{Arrays.asList(propagationEntityWithWhitespaceId),
        Arrays.asList("propagationEntity cannot be null or empty")});

    // PropagationEntity with negative CPU cost
    PropagationEntity propagationEntityWithNegativeCpu = new PropagationEntity("test", -1L, 100L, null);
    data.add(new Object[]{Arrays.asList(propagationEntityWithNegativeCpu),
        Arrays.asList("cpuCostNs cannot be negative")});

    // Duplicate propagationEntity IDs
    PropagationEntity entity1 = new PropagationEntity("duplicate", 100L, 100L, null);
    PropagationEntity entity2 = new PropagationEntity("duplicate", 200L, 200L, null);
    data.add(new Object[]{Arrays.asList(entity1, entity2),
        Arrays.asList("propagationEntity 'duplicate' is duplicated")});

    // Exceeding CPU cost limits
    PropagationEntity highCostEntity1 = new PropagationEntity("highCost", 10000L, 100L, null);
    data.add(new Object[]{Arrays.asList(highCostEntity1),
        Arrays.asList("total CPU cost (10000 ns) exceeds parent/limit")});

    // Exceeding memory cost limits
    PropagationEntity highCostEntity2 = new PropagationEntity("highCost", 100L, 10000L, null);
    data.add(new Object[]{Arrays.asList(highCostEntity2),
        Arrays.asList("total memory cost (10000 bytes) exceeds parent/limit")});

    // Partial defined costs for some entities
    PropagationEntity partialCostEntity1 = new PropagationEntity("partial1", null, 100L, null);
    PropagationEntity partialCostEntity2 = new PropagationEntity("partial2", 100L, null, null);
    data.add(new Object[]{Arrays.asList(partialCostEntity1, partialCostEntity2),
        Arrays.asList("must have both cpuCostNs and memoryCostBytes defined or both null")});

    // Partial defined costs for some entities
    PropagationEntity fullCostEntity = new PropagationEntity("full", 100L, 100L, null);
    PropagationEntity emptyCostEntity = new PropagationEntity("empty", null, null, null);
    data.add(new Object[]{Arrays.asList(fullCostEntity, emptyCostEntity),
        Arrays.asList("must have either all or none of the propagationEntities define costs")});

    // Valid propagationEntities
    data.add(new Object[]{Arrays.asList(createValidPropagationEntity()), Collections.emptyList()});

    return data.toArray(new Object[0][]);
  }

  @Test
  public void testValidateQueryWorkloadConfigComplexValidConfig() {
    // Test a complex but valid configuration with multiple nodes and sub-allocations
    PropagationEntityOverrides overrides1 = new PropagationEntityOverrides("CONSUMING", 30L, 30L);
    PropagationEntityOverrides overrides2 = new PropagationEntityOverrides("COMPLETED", 40L, 40L);
    List<PropagationEntityOverrides> overrides = Arrays.asList(overrides1, overrides2);

    PropagationEntity entity1 = new PropagationEntity("cost1", 100L, 100L, overrides);
    PropagationEntity entity2 = new PropagationEntity("cost2", 200L, 200L, null);

    PropagationScheme serverScheme
        = new PropagationScheme(PropagationScheme.Type.TABLE, Arrays.asList(entity1, entity2));
    NodeConfig serverNode = new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(), serverScheme);

    PropagationScheme brokerScheme
        = new PropagationScheme(PropagationScheme.Type.TENANT, Arrays.asList(entity2));
    NodeConfig brokerNode = new NodeConfig(NodeConfig.Type.BROKER_NODE, createValidEnforcementProfile(), brokerScheme);

    QueryWorkloadConfig config = new QueryWorkloadConfig("complexWorkload", Arrays.asList(serverNode, brokerNode));

    List<String> errors = QueryWorkloadConfigUtils.validateQueryWorkloadConfig(config);
    Assert.assertTrue(errors.isEmpty(), "Complex valid config should have no errors, but got: " + errors);
  }

  private NodeConfig createValidServerNodeConfig() {
    return new NodeConfig(NodeConfig.Type.SERVER_NODE, createValidEnforcementProfile(),
        createValidPropagationScheme());
  }

  private EnforcementProfile createValidEnforcementProfile() {
    return new EnforcementProfile(1000, 1000);
  }

  private PropagationScheme createValidPropagationScheme() {
    return new PropagationScheme(PropagationScheme.Type.TABLE, Arrays.asList(createValidPropagationEntity()));
  }

  private PropagationEntity createValidPropagationEntity() {
    return new PropagationEntity("validCostId", 100L, 100L, null);
  }
}
