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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.config.GrpcConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class GrpcMvQueryExecutorTest {
  private HelixManager _helixManager;
  private GrpcMvQueryExecutor _queryExecutor;

  @BeforeMethod
  public void setUp() {
    _helixManager = mock(HelixManager.class);
    _queryExecutor = new GrpcMvQueryExecutor(_helixManager, new GrpcConfig(Collections.emptyMap()));
  }

  @AfterMethod
  public void tearDown() {
    _queryExecutor.close();
  }

  @Test
  public void testSelectBrokerRoundRobin() {
    List<InstanceConfig> configs = new ArrayList<>();
    configs.add(buildBrokerConfig("Broker_broker1_8099", "broker1", 8090));
    configs.add(buildBrokerConfig("Broker_broker2_8099", "broker2", 8091));
    configs.add(buildBrokerConfig("Broker_broker3_8099", "broker3", 8092));
    when(_helixManager.getClusterName()).thenReturn("testCluster");
    when(_helixManager.getInstanceName()).thenReturn("Minion_minion1_9514");
    mockHelixInstanceConfigs(configs);

    Set<String> selectedHosts = new HashSet<>();
    for (int i = 0; i < 6; i++) {
      Pair<String, Integer> broker = _queryExecutor.selectBroker();
      selectedHosts.add(broker.getLeft() + ":" + broker.getRight());
    }

    assertEquals(selectedHosts.size(), 3, "All 3 brokers should be selected via round-robin");
    assertTrue(selectedHosts.contains("broker1:8090"));
    assertTrue(selectedHosts.contains("broker2:8091"));
    assertTrue(selectedHosts.contains("broker3:8092"));
  }

  @Test
  public void testSelectBrokerSingleBroker() {
    List<InstanceConfig> configs = new ArrayList<>();
    configs.add(buildBrokerConfig("Broker_broker1_8099", "broker1", 8090));
    mockHelixInstanceConfigs(configs);

    for (int i = 0; i < 3; i++) {
      Pair<String, Integer> broker = _queryExecutor.selectBroker();
      assertEquals(broker.getLeft(), "broker1");
      assertEquals(broker.getRight().intValue(), 8090);
    }
  }

  @Test
  public void testSelectBrokerNoBrokersThrows() {
    mockHelixInstanceConfigs(Collections.emptyList());

    try {
      _queryExecutor.selectBroker();
      fail("Expected IllegalStateException when no gRPC-enabled brokers exist");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("No broker with gRPC enabled"));
    }
  }

  @Test
  public void testSelectBrokerSkipsBrokersWithoutGrpcPort() {
    List<InstanceConfig> configs = new ArrayList<>();
    configs.add(buildBrokerConfig("Broker_broker1_8099", "broker1", 0));
    configs.add(buildBrokerConfig("Broker_broker2_8099", "broker2", 8091));

    InstanceConfig noGrpcBroker = new InstanceConfig("Broker_broker3_8099");
    noGrpcBroker.setHostName("broker3");
    configs.add(noGrpcBroker);

    mockHelixInstanceConfigs(configs);

    for (int i = 0; i < 3; i++) {
      Pair<String, Integer> broker = _queryExecutor.selectBroker();
      assertEquals(broker.getLeft(), "broker2");
      assertEquals(broker.getRight().intValue(), 8091);
    }
  }

  @Test
  public void testSelectBrokerSkipsNonBrokerInstances() {
    List<InstanceConfig> configs = new ArrayList<>();
    configs.add(buildServerConfig("Server_server1_8098", "server1", 8090));
    configs.add(buildBrokerConfig("Broker_broker1_8099", "broker1", 8090));
    mockHelixInstanceConfigs(configs);

    Pair<String, Integer> broker = _queryExecutor.selectBroker();
    assertEquals(broker.getLeft(), "broker1");
  }

  @Test
  public void testStaleClientEviction() {
    List<InstanceConfig> twoConfigs = new ArrayList<>();
    twoConfigs.add(buildBrokerConfig("Broker_broker1_8099", "broker1", 8090));
    twoConfigs.add(buildBrokerConfig("Broker_broker2_8099", "broker2", 8091));
    mockHelixInstanceConfigs(twoConfigs);

    _queryExecutor.selectBroker();
    _queryExecutor.selectBroker();
    assertEquals(_queryExecutor.getCachedClientCount(), 2);

    List<InstanceConfig> oneConfig = new ArrayList<>();
    oneConfig.add(buildBrokerConfig("Broker_broker1_8099", "broker1", 8090));
    mockHelixInstanceConfigs(oneConfig);

    _queryExecutor.selectBroker();
    assertEquals(_queryExecutor.getCachedClientCount(), 1);
  }

  @Test
  public void testClientReuse() {
    List<InstanceConfig> configs = new ArrayList<>();
    configs.add(buildBrokerConfig("Broker_broker1_8099", "broker1", 8090));
    mockHelixInstanceConfigs(configs);

    _queryExecutor.selectBroker();
    _queryExecutor.selectBroker();
    _queryExecutor.selectBroker();

    assertEquals(_queryExecutor.getCachedClientCount(), 1,
        "Repeated selections of the same broker should reuse the cached client");
  }

  @Test
  public void testCloseEvictsAllClients() {
    List<InstanceConfig> configs = new ArrayList<>();
    configs.add(buildBrokerConfig("Broker_broker1_8099", "broker1", 8090));
    configs.add(buildBrokerConfig("Broker_broker2_8099", "broker2", 8091));
    mockHelixInstanceConfigs(configs);

    _queryExecutor.selectBroker();
    _queryExecutor.selectBroker();
    assertEquals(_queryExecutor.getCachedClientCount(), 2);

    _queryExecutor.close();
    assertEquals(_queryExecutor.getCachedClientCount(), 0);
  }

  private InstanceConfig buildBrokerConfig(String instanceName, String hostname, int grpcPort) {
    InstanceConfig config = new InstanceConfig(instanceName);
    config.setHostName(hostname);
    if (grpcPort > 0) {
      config.getRecord().setSimpleField(CommonConstants.Helix.Instance.GRPC_PORT_KEY,
          String.valueOf(grpcPort));
    }
    return config;
  }

  private InstanceConfig buildServerConfig(String instanceName, String hostname, int grpcPort) {
    InstanceConfig config = new InstanceConfig(instanceName);
    config.setHostName(hostname);
    if (grpcPort > 0) {
      config.getRecord().setSimpleField(CommonConstants.Helix.Instance.GRPC_PORT_KEY,
          String.valueOf(grpcPort));
    }
    return config;
  }

  private void mockHelixInstanceConfigs(List<InstanceConfig> configs) {
    org.apache.helix.HelixDataAccessor accessor = mock(org.apache.helix.HelixDataAccessor.class);
    org.apache.helix.PropertyKey.Builder keyBuilder = mock(org.apache.helix.PropertyKey.Builder.class);
    org.apache.helix.PropertyKey propertyKey = mock(org.apache.helix.PropertyKey.class);
    when(_helixManager.getHelixDataAccessor()).thenReturn(accessor);
    when(accessor.keyBuilder()).thenReturn(keyBuilder);
    when(keyBuilder.instanceConfigs()).thenReturn(propertyKey);
    when(accessor.getChildValues(propertyKey, true)).thenReturn(configs);
  }
}
