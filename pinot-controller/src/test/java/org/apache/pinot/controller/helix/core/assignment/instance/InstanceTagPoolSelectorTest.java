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


public class InstanceTagPoolSelectorTest {

  private InstanceTagPoolConfig _tagPoolConfig;
  private String _tableNameWithType = "tableNameWithType";;
  private static final String _tag = "testTag";

  @Test
  public void testSelectInstances_PoolBasedTagConfig() {
    String poolNum = "1";
    String testInstanceName = "testInstanceId";
    List<Integer> poolList = Arrays.asList(1);
    _tagPoolConfig = new InstanceTagPoolConfig(_tag, true, 5, poolList);
    InstanceTagPoolSelector selector = new InstanceTagPoolSelector(_tagPoolConfig, _tableNameWithType);
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig config = new InstanceConfig(testInstanceName);
    config.addTag(_tag);
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    Map<String, String> poolMap = new HashMap<>();
    poolMap.put(_tag, poolNum);
    mapFields.put(InstanceUtils.POOL_KEY, poolMap);
    config.getRecord().setMapFields(mapFields);
    instanceConfigs.add(config);
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = selector.selectInstances(instanceConfigs);
    List<InstanceConfig> instanceConfigresults = poolToInstanceConfigsMap.get(Integer.parseInt(poolNum));
    assertEquals(instanceConfigresults.size(), 1);
    assertEquals(instanceConfigresults.get(0).getInstanceName(), testInstanceName);
    assertEquals(instanceConfigresults.get(0).getRecord().getMapFields().get("pool").get(_tag), poolNum);
  }

  @Test
  public void testSelectInstances_MultipleInstancesPoolBasedTagConfig() {
    String poolNum = "1";
    String poolNum2 = "2";
    String testInstanceName = "testInstanceId";
    String testInstanceName2 = "testInstanceId2";
    List<Integer> poolList = new ArrayList<>();
    _tagPoolConfig = new InstanceTagPoolConfig(_tag, true, 1, poolList);
    InstanceTagPoolSelector selector = new InstanceTagPoolSelector(_tagPoolConfig, _tableNameWithType);
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig config = new InstanceConfig(testInstanceName);
    InstanceConfig config2 = new InstanceConfig(testInstanceName2);
    config.addTag(_tag);
    config2.addTag(_tag);
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    Map<String, String> poolMap = new HashMap<>();
    Map<String, Map<String, String>> mapFields2 = new HashMap<>();
    Map<String, String> poolMap2 = new HashMap<>();
    poolMap.put(_tag, poolNum);
    poolMap2.put(_tag, poolNum2);
    mapFields.put(InstanceUtils.POOL_KEY, poolMap);
    mapFields2.put(InstanceUtils.POOL_KEY, poolMap2);
    config.getRecord().setMapFields(mapFields);
    config2.getRecord().setMapFields(mapFields2);
    instanceConfigs.add(config);
    instanceConfigs.add(config2);
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = selector.selectInstances(instanceConfigs);
    List<InstanceConfig> instanceConfigresults = poolToInstanceConfigsMap.get(Integer.parseInt(poolNum2));
    assertEquals(instanceConfigresults.size(), 1);
    assertEquals(instanceConfigresults.get(0).getInstanceName(), testInstanceName2);
    assertEquals(instanceConfigresults.get(0).getRecord().getMapFields().get("pool").get(_tag), poolNum2);
  }

  @Test
  public void testSelectInstances_NonPoolBasedTagConfig() {
    String testInstanceName = "testInstanceId";
    String poolNum = "1";
    List<Integer> poolList = Arrays.asList(1);
    _tagPoolConfig = new InstanceTagPoolConfig(_tag, false, 5, poolList);
    InstanceTagPoolSelector selector = new InstanceTagPoolSelector(_tagPoolConfig, _tableNameWithType);
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig config = new InstanceConfig(testInstanceName);
    config.addTag(_tag);
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    Map<String, String> poolMap = new HashMap<>();
    poolMap.put(_tag, poolNum);
    mapFields.put(InstanceUtils.POOL_KEY, poolMap);
    config.getRecord().setMapFields(mapFields);
    instanceConfigs.add(config);
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = selector.selectInstances(instanceConfigs);
    List<InstanceConfig> instanceConfigresults = poolToInstanceConfigsMap.get(0);
    assertEquals(instanceConfigresults.size(), 1);
    assertEquals(instanceConfigresults.get(0).getInstanceName(), testInstanceName);
    assertEquals(instanceConfigresults.get(0).getRecord().getMapFields().get("pool").get(_tag), poolNum);
  }

  @Test
  public void testSelectInstances_EmptyPoolList() {
    String testInstanceName = "testInstanceId";
    String poolNum = "5";
    List<Integer> poolList = new ArrayList<>();
    _tagPoolConfig = new InstanceTagPoolConfig(_tag, true, 1, poolList);
    InstanceTagPoolSelector selector = new InstanceTagPoolSelector(_tagPoolConfig, _tableNameWithType);
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig config = new InstanceConfig(testInstanceName);
    config.addTag(_tag);
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    Map<String, String> poolMap = new HashMap<>();
    poolMap.put(_tag, poolNum);
    mapFields.put(InstanceUtils.POOL_KEY, poolMap);
    config.getRecord().setMapFields(mapFields);
    instanceConfigs.add(config);
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = selector.selectInstances(instanceConfigs);
    List<InstanceConfig> instanceConfigresults = poolToInstanceConfigsMap.get(Integer.parseInt(poolNum));
    assertEquals(instanceConfigresults.size(), 1);
    assertEquals(instanceConfigresults.get(0).getInstanceName(), testInstanceName);
    assertEquals(instanceConfigresults.get(0).getRecord().getMapFields().get("pool").get(_tag), poolNum);
  }
  
  @Test
  public void testSelectInstances_NegativeNumPools() {
    String testInstanceName = "testInstanceId";
    String poolNum = "1";
    List<Integer> poolList = new ArrayList<>();
    _tagPoolConfig = new InstanceTagPoolConfig(_tag, true, -1, poolList);
    InstanceTagPoolSelector selector = new InstanceTagPoolSelector(_tagPoolConfig, _tableNameWithType);
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    InstanceConfig config = new InstanceConfig(testInstanceName);
    config.addTag(_tag);
    Map<String, Map<String, String>> mapFields = new HashMap<>();
    Map<String, String> poolMap = new HashMap<>();
    poolMap.put(_tag, poolNum);
    mapFields.put(InstanceUtils.POOL_KEY, poolMap);
    config.getRecord().setMapFields(mapFields);
    instanceConfigs.add(config);
    Map<Integer, List<InstanceConfig>> poolToInstanceConfigsMap = selector.selectInstances(instanceConfigs);
    List<InstanceConfig> instanceConfigresults = poolToInstanceConfigsMap.get(Integer.parseInt(poolNum));
    assertEquals(instanceConfigresults.size(), 1);
    assertEquals(instanceConfigresults.get(0).getInstanceName(), testInstanceName);
    assertEquals(instanceConfigresults.get(0).getRecord().getMapFields().get("pool").get(_tag), poolNum);
  }

}
