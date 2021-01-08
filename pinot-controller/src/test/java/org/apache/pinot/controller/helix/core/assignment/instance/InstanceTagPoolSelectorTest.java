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

package org.apache.pinot.controller.helix.core.assignment.instance;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.testng.annotations.Test;


/**
 * Tests the {@link InstanceTagPoolSelector} class based on different {@link InstanceTagPoolConfig}.
 */
public class InstanceTagPoolSelectorTest {

  private InstanceTagPoolConfig _tagPoolConfig;
  private String _tableNameWithType = "tableNameWithType";
  private static final String _tag = "testTag";

  /**
   * Tests {@link InstanceTagPoolSelector#selectInstances(List)} method for pool based tag config.
   */
  @Test
  public void testPoolTagBasedInstancesSelection() {
    String poolNum = "1";
    String testInstanceName = "testInstanceId";

    //create a single instanceConfig
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig config = prepareInstanceConfigForTest(testInstanceName, poolNum);
    instanceConfigs.add(config);

    //init tagPoolConfig as poolBased
    _tagPoolConfig = prepareInstanceTagPoolConfigForTest(true, 5, Arrays.asList(1));
    InstanceTagPoolSelector selector = new InstanceTagPoolSelector(_tagPoolConfig, _tableNameWithType);

    //test the selectInstances method
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = selector.selectInstances(instanceConfigs);
    List<InstanceConfig> instanceConfigresults = poolToInstanceConfigsMap.get(Integer.parseInt(poolNum));

    assertEquals(instanceConfigresults.size(), 1);
    assertEquals(instanceConfigresults.get(0).getInstanceName(), testInstanceName);
    assertEquals(instanceConfigresults.get(0).getRecord().getMapFields().get("pool").get(_tag), poolNum);
  }

  /**
   * Tests {@link InstanceTagPoolSelector#selectInstances(List)} method for pool based tag config with multiple pools.
   */
  @Test
  public void testPoolTagBasedInstancesSelectionWithMultiplePools() {
    String poolNum = "1";
    String poolNum2 = "2";
    String testInstanceName = "testInstanceId";
    String testInstanceName2 = "testInstanceId2";

    //create multiple instanceConfigs
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig config = prepareInstanceConfigForTest(testInstanceName, poolNum);
    InstanceConfig config2 = prepareInstanceConfigForTest(testInstanceName2, poolNum2);
    instanceConfigs.add(config);
    instanceConfigs.add(config2);

    //init tagPoolConfig as poolBased and empty pool
    _tagPoolConfig = prepareInstanceTagPoolConfigForTest(true, 1, new ArrayList<>());
    InstanceTagPoolSelector selector = new InstanceTagPoolSelector(_tagPoolConfig, _tableNameWithType);

    //test the selectInstances method
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = selector.selectInstances(instanceConfigs);
    List<InstanceConfig> instanceConfigresults = poolToInstanceConfigsMap.get(Integer.parseInt(poolNum2));

    assertEquals(instanceConfigresults.size(), 1);
    assertEquals(instanceConfigresults.get(0).getInstanceName(), testInstanceName2);
    assertEquals(instanceConfigresults.get(0).getRecord().getMapFields().get("pool").get(_tag), poolNum2);
  }

  /**
   * Tests {@link InstanceTagPoolSelector#selectInstances(List)} method for non pool based tag config.
   */
  @Test
  public void testNonPoolTagBasedInstancesSelection() {
    String testInstanceName = "testInstanceId";
    String poolNum = "1";

    //create a single instanceConfig
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig config = prepareInstanceConfigForTest(testInstanceName, poolNum);
    instanceConfigs.add(config);

    //init tagPoolConfig as non poolBased
    _tagPoolConfig = prepareInstanceTagPoolConfigForTest(false, 5, Arrays.asList(1));
    InstanceTagPoolSelector selector = new InstanceTagPoolSelector(_tagPoolConfig, _tableNameWithType);

    //test the selectInstances method
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = selector.selectInstances(instanceConfigs);
    List<InstanceConfig> instanceConfigresults = poolToInstanceConfigsMap.get(0);

    assertEquals(instanceConfigresults.size(), 1);
    assertEquals(instanceConfigresults.get(0).getInstanceName(), testInstanceName);
    assertEquals(instanceConfigresults.get(0).getRecord().getMapFields().get("pool").get(_tag), poolNum);
  }

  /**
   * Tests {@link InstanceTagPoolSelector#selectInstances(List)} method for empty pool based tag config.
   */
  @Test
  public void testPoolTagBasedInstancesSelectionWithEmptyPool() {
    String testInstanceName = "testInstanceId";
    String poolNum = "5";

    //create a single instanceConfig
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig config = prepareInstanceConfigForTest(testInstanceName, poolNum);
    instanceConfigs.add(config);

    //init tagPoolConfig as poolBased and empty pool
    _tagPoolConfig = prepareInstanceTagPoolConfigForTest(true, 1, new ArrayList<>());
    InstanceTagPoolSelector selector = new InstanceTagPoolSelector(_tagPoolConfig, _tableNameWithType);

    //test the selectInstances method
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = selector.selectInstances(instanceConfigs);
    List<InstanceConfig> instanceConfigresults = poolToInstanceConfigsMap.get(Integer.parseInt(poolNum));

    assertEquals(instanceConfigresults.size(), 1);
    assertEquals(instanceConfigresults.get(0).getInstanceName(), testInstanceName);
    assertEquals(instanceConfigresults.get(0).getRecord().getMapFields().get("pool").get(_tag), poolNum);
  }

  /**
   * Tests {@link InstanceTagPoolSelector#selectInstances(List)} method for pool based tag config for negative number of pools.
   */
  @Test
  public void testPoolTagBasedInstancesSelectionWithNegativeNumPools() {
    String testInstanceName = "testInstanceId";
    String poolNum = "1";

    //create a single instanceConfig
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig config = prepareInstanceConfigForTest(testInstanceName, poolNum);
    instanceConfigs.add(config);

    //init tagPoolConfig as poolBased with negative numPools
    _tagPoolConfig = prepareInstanceTagPoolConfigForTest(true, -1, new ArrayList<>());
    InstanceTagPoolSelector selector = new InstanceTagPoolSelector(_tagPoolConfig, _tableNameWithType);

    //test the selectInstances method
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = selector.selectInstances(instanceConfigs);
    List<InstanceConfig> instanceConfigresults = poolToInstanceConfigsMap.get(Integer.parseInt(poolNum));

    assertEquals(instanceConfigresults.size(), 1);
    assertEquals(instanceConfigresults.get(0).getInstanceName(), testInstanceName);
    assertEquals(instanceConfigresults.get(0).getRecord().getMapFields().get("pool").get(_tag), poolNum);
  }

  /**
   * Helper method for preparing InstanceTagPoolConfig.
   * @param poolBased
   *        if poolConfig is poolBased.
   * @param numPools
   *        Number of pools.
   * @param poolList
   *        List of pools.
   * @return instanceTagPoolConfig
   */
  private InstanceTagPoolConfig prepareInstanceTagPoolConfigForTest(Boolean poolBased, int numPools,
      List<Integer> poolList) {
    return new InstanceTagPoolConfig(_tag, poolBased, numPools, poolList);
  }

  /**
   * Helper method for preparing InstanceConfig.
   * @param instanceName
   *        Instance Name
   * @param poolNum
   *        Number of pools.
   * @return instanceConfig
   */
  private InstanceConfig prepareInstanceConfigForTest(String instanceName, String poolNum) {
    InstanceConfig config = new InstanceConfig(instanceName);
    config.addTag(_tag);
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    Map<String, String> poolMap = new HashMap<>();
    poolMap.put(_tag, poolNum);
    mapFields.put(InstanceUtils.POOL_KEY, poolMap);
    config.getRecord().setMapFields(mapFields);
    return config;
  }

}
