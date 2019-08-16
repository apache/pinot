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
package org.apache.pinot.controller.api;

import com.fasterxml.jackson.core.type.TypeReference;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.config.TableNameBuilder;
import org.apache.pinot.common.config.TagNameUtils;
import org.apache.pinot.common.config.Tenant;
import org.apache.pinot.common.config.instance.InstanceAssignmentConfig;
import org.apache.pinot.common.config.instance.InstanceReplicaPartitionConfig;
import org.apache.pinot.common.config.instance.InstanceTagPoolConfig;
import org.apache.pinot.common.data.FieldSpec.DataType;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
import org.apache.pinot.common.utils.InstancePartitionsType;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.common.utils.TenantRole;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.assignment.InstancePartitions;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class PinotInstanceAssignmentRestletResourceTest extends ControllerTest {
  private static final int NUM_SERVER_INSTANCES = 3;
  private static final String TENANT_NAME = "testTenant";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TIME_COLUMN_NAME = "daysSinceEpoch";

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    ControllerConf config = getDefaultControllerConfiguration();
    config.setTenantIsolationEnabled(false);
    startController(config);
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, false);
    addFakeServerInstancesToAutoJoinHelixCluster(NUM_SERVER_INSTANCES, false);

    // Create broker and server tenant
    Tenant brokerTenant = new Tenant.TenantBuilder(TENANT_NAME).setRole(TenantRole.BROKER).setTotalInstances(1).build();
    _helixResourceManager.createBrokerTenant(brokerTenant);
    Tenant serverTenant =
        new Tenant.TenantBuilder(TENANT_NAME).setRole(TenantRole.SERVER).setOfflineInstances(1).setRealtimeInstances(1)
            .build();
    _helixResourceManager.createServerTenant(serverTenant);
  }

  @Test
  public void testInstanceAssignment()
      throws Exception {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addTime(TIME_COLUMN_NAME, TimeUnit.DAYS, DataType.INT)
            .build();
    _helixResourceManager.addOrUpdateSchema(schema);
    TableConfig offlineTableConfig =
        new TableConfig.Builder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(TENANT_NAME)
            .setServerTenant(TENANT_NAME).build();
    _helixResourceManager.addTable(offlineTableConfig);
    TableConfig realtimeTableConfig =
        new TableConfig.Builder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setBrokerTenant(TENANT_NAME)
            .setServerTenant(TENANT_NAME).setLLC(true)
            .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()).build();
    _helixResourceManager.addTable(realtimeTableConfig);

    // There should be no instance partitions
    try {
      getInstancePartitionsMap();
      fail();
    } catch (FileNotFoundException e) {
      // Expected
    }

    // Assign instances should fail
    try {
      sendPostRequest(_controllerRequestURLBuilder.forInstanceAssign(RAW_TABLE_NAME, null, true), null);
      fail();
    } catch (FileNotFoundException e) {
      // Expected
    }

    // Add OFFLINE instance assignment config to the OFFLINE table config
    InstanceAssignmentConfig offlineInstanceAssignmentConfig = new InstanceAssignmentConfig();
    InstanceTagPoolConfig offlineInstanceTagPoolConfig = new InstanceTagPoolConfig();
    offlineInstanceTagPoolConfig.setTag(TagNameUtils.getOfflineTagForTenant(TENANT_NAME));
    offlineInstanceAssignmentConfig.setTagPoolConfig(offlineInstanceTagPoolConfig);
    offlineInstanceAssignmentConfig.setReplicaPartitionConfig(new InstanceReplicaPartitionConfig());
    offlineTableConfig.setInstanceAssignmentConfigMap(
        Collections.singletonMap(InstancePartitionsType.OFFLINE, offlineInstanceAssignmentConfig));
    _helixResourceManager.setExistingTableConfig(offlineTableConfig);

    // OFFLINE instance partitions should be generated
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 1);
    InstancePartitions offlineInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    assertNotNull(offlineInstancePartitions);
    assertEquals(offlineInstancePartitions.getNumReplicas(), 1);
    assertEquals(offlineInstancePartitions.getNumPartitions(), 1);
    assertEquals(offlineInstancePartitions.getInstances(0, 0).size(), 1);
    String offlineInstanceId = offlineInstancePartitions.getInstances(0, 0).get(0);

    // Add CONSUMING instance assignment config to the REALTIME table config
    InstanceAssignmentConfig consumingInstanceAssignmentConfig = new InstanceAssignmentConfig();
    InstanceTagPoolConfig realtimeInstanceTagPoolConfig = new InstanceTagPoolConfig();
    realtimeInstanceTagPoolConfig.setTag(TagNameUtils.getRealtimeTagForTenant(TENANT_NAME));
    consumingInstanceAssignmentConfig.setTagPoolConfig(realtimeInstanceTagPoolConfig);
    consumingInstanceAssignmentConfig.setReplicaPartitionConfig(new InstanceReplicaPartitionConfig());
    realtimeTableConfig.setInstanceAssignmentConfigMap(
        Collections.singletonMap(InstancePartitionsType.CONSUMING, consumingInstanceAssignmentConfig));
    _helixResourceManager.setExistingTableConfig(realtimeTableConfig);

    // CONSUMING instance partitions should be generated
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 2);
    offlineInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    assertNotNull(offlineInstancePartitions);
    assertEquals(offlineInstancePartitions.getNumReplicas(), 1);
    assertEquals(offlineInstancePartitions.getNumPartitions(), 1);
    assertEquals(offlineInstancePartitions.getInstances(0, 0), Collections.singletonList(offlineInstanceId));
    InstancePartitions consumingInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    assertNotNull(consumingInstancePartitions);
    assertEquals(consumingInstancePartitions.getNumReplicas(), 1);
    assertEquals(consumingInstancePartitions.getNumPartitions(), 1);
    assertEquals(consumingInstancePartitions.getInstances(0, 0).size(), 1);
    String realtimeInstanceId = consumingInstancePartitions.getInstances(0, 0).get(0);

    // Use OFFLINE instance assignment config as the COMPLETED instance assignment config
    realtimeTableConfig
        .setInstanceAssignmentConfigMap(new TreeMap<InstancePartitionsType, InstanceAssignmentConfig>() {{
          put(InstancePartitionsType.CONSUMING, consumingInstanceAssignmentConfig);
          put(InstancePartitionsType.COMPLETED, offlineInstanceAssignmentConfig);
        }});
    _helixResourceManager.setExistingTableConfig(realtimeTableConfig);

    // COMPLETED instance partitions should be generated
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 3);
    offlineInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    assertNotNull(offlineInstancePartitions);
    assertEquals(offlineInstancePartitions.getNumReplicas(), 1);
    assertEquals(offlineInstancePartitions.getNumPartitions(), 1);
    assertEquals(offlineInstancePartitions.getInstances(0, 0), Collections.singletonList(offlineInstanceId));
    consumingInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    assertNotNull(consumingInstancePartitions);
    assertEquals(consumingInstancePartitions.getNumReplicas(), 1);
    assertEquals(consumingInstancePartitions.getNumPartitions(), 1);
    assertEquals(consumingInstancePartitions.getInstances(0, 0), Collections.singletonList(realtimeInstanceId));
    InstancePartitions completedInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.COMPLETED);
    assertEquals(completedInstancePartitions.getNumReplicas(), 1);
    assertEquals(completedInstancePartitions.getNumPartitions(), 1);
    assertEquals(completedInstancePartitions.getInstances(0, 0), Collections.singletonList(offlineInstanceId));

    // Test fetching instance partitions by table name with type suffix
    instancePartitionsMap = deserializeInstancePartitionsMap(sendGetRequest(_controllerRequestURLBuilder
        .forInstancePartitions(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME), null)));
    assertEquals(instancePartitionsMap.size(), 1);
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.OFFLINE));
    instancePartitionsMap = deserializeInstancePartitionsMap(sendGetRequest(_controllerRequestURLBuilder
        .forInstancePartitions(TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME), null)));
    assertEquals(instancePartitionsMap.size(), 2);
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.CONSUMING));
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.COMPLETED));

    // Test fetching instance partitions by table name and instance partitions type
    for (InstancePartitionsType instancePartitionsType : InstancePartitionsType.values()) {
      instancePartitionsMap = deserializeInstancePartitionsMap(
          sendGetRequest(_controllerRequestURLBuilder.forInstancePartitions(RAW_TABLE_NAME, instancePartitionsType)));
      assertEquals(instancePartitionsMap.size(), 1);
      assertEquals(instancePartitionsMap.get(instancePartitionsType).getName(),
          instancePartitionsType.getInstancePartitionsName(RAW_TABLE_NAME));
    }

    // Remove the instance partitions for both OFFLINE and REALTIME table
    sendDeleteRequest(_controllerRequestURLBuilder.forInstancePartitions(RAW_TABLE_NAME, null));
    try {
      getInstancePartitionsMap();
      fail();
    } catch (FileNotFoundException e) {
      // Expected
    }

    // Assign instances without instance partitions type (dry run)
    instancePartitionsMap = deserializeInstancePartitionsMap(
        sendPostRequest(_controllerRequestURLBuilder.forInstanceAssign(RAW_TABLE_NAME, null, true), null));
    assertEquals(instancePartitionsMap.size(), 3);
    offlineInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    assertNotNull(offlineInstancePartitions);
    assertEquals(offlineInstancePartitions.getNumReplicas(), 1);
    assertEquals(offlineInstancePartitions.getNumPartitions(), 1);
    assertEquals(offlineInstancePartitions.getInstances(0, 0), Collections.singletonList(offlineInstanceId));
    consumingInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    assertNotNull(consumingInstancePartitions);
    assertEquals(consumingInstancePartitions.getNumReplicas(), 1);
    assertEquals(consumingInstancePartitions.getNumPartitions(), 1);
    assertEquals(consumingInstancePartitions.getInstances(0, 0), Collections.singletonList(realtimeInstanceId));
    completedInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.COMPLETED);
    assertEquals(completedInstancePartitions.getNumReplicas(), 1);
    assertEquals(completedInstancePartitions.getNumPartitions(), 1);
    assertEquals(completedInstancePartitions.getInstances(0, 0), Collections.singletonList(offlineInstanceId));

    // Instance partitions should not be persisted
    try {
      getInstancePartitionsMap();
      fail();
    } catch (FileNotFoundException e) {
      // Expected
    }

    // Assign instances for both OFFLINE and REALTIME table
    sendPostRequest(_controllerRequestURLBuilder.forInstanceAssign(RAW_TABLE_NAME, null, false), null);

    // Instance partitions should be persisted
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 3);

    // Remove the instance partitions for REALTIME table
    sendDeleteRequest(_controllerRequestURLBuilder
        .forInstancePartitions(TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME), null));
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 1);
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.OFFLINE));

    // Assign instances for COMPLETED segments
    instancePartitionsMap = deserializeInstancePartitionsMap(sendPostRequest(
        _controllerRequestURLBuilder.forInstanceAssign(RAW_TABLE_NAME, InstancePartitionsType.COMPLETED, false), null));
    assertEquals(instancePartitionsMap.size(), 1);
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.COMPLETED));

    // There should be OFFLINE and COMPLETED instance partitions persisted
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 2);
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.OFFLINE));
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.COMPLETED));

    // Replace OFFLINE instance with REALTIME instance for COMPLETED instance partitions
    instancePartitionsMap = deserializeInstancePartitionsMap(sendPostRequest(_controllerRequestURLBuilder
            .forInstanceReplace(RAW_TABLE_NAME, InstancePartitionsType.COMPLETED, offlineInstanceId, realtimeInstanceId),
        null));
    assertEquals(instancePartitionsMap.size(), 1);
    assertEquals(instancePartitionsMap.get(InstancePartitionsType.COMPLETED).getInstances(0, 0),
        Collections.singletonList(realtimeInstanceId));

    // Replace the instance again using REALTIME table name (old instance does not exist)
    try {
      sendPostRequest(_controllerRequestURLBuilder
          .forInstanceReplace(TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME), null, offlineInstanceId,
              realtimeInstanceId), null);
      fail();
    } catch (FileNotFoundException e) {
      // Expected
    }

    // Post the CONSUMING instance partitions
    instancePartitionsMap = deserializeInstancePartitionsMap(
        sendPutRequest(_controllerRequestURLBuilder.forInstancePartitions(RAW_TABLE_NAME, null),
            JsonUtils.objectToString(consumingInstancePartitions)));
    assertEquals(instancePartitionsMap.size(), 1);
    assertEquals(instancePartitionsMap.get(InstancePartitionsType.CONSUMING).getInstances(0, 0),
        Collections.singletonList(realtimeInstanceId));

    // OFFLINE instance partitions should have OFFLINE instance, CONSUMING and COMPLETED instance partitions should have
    // REALTIME instance
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 3);
    assertEquals(instancePartitionsMap.get(InstancePartitionsType.OFFLINE).getInstances(0, 0),
        Collections.singletonList(offlineInstanceId));
    assertEquals(instancePartitionsMap.get(InstancePartitionsType.CONSUMING).getInstances(0, 0),
        Collections.singletonList(realtimeInstanceId));
    assertEquals(instancePartitionsMap.get(InstancePartitionsType.COMPLETED).getInstances(0, 0),
        Collections.singletonList(realtimeInstanceId));

    // Delete the OFFLINE table
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME);
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 2);
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.CONSUMING));
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.COMPLETED));

    // Delete the REALTIME table
    _helixResourceManager.deleteRealtimeTable(RAW_TABLE_NAME);
    try {
      getInstancePartitionsMap();
      fail();
    } catch (FileNotFoundException e) {
      // Expected
    }
  }

  private Map<InstancePartitionsType, InstancePartitions> getInstancePartitionsMap()
      throws Exception {
    return deserializeInstancePartitionsMap(
        sendGetRequest(_controllerRequestURLBuilder.forInstancePartitions(RAW_TABLE_NAME, null)));
  }

  private Map<InstancePartitionsType, InstancePartitions> deserializeInstancePartitionsMap(
      String instancePartitionsMapString)
      throws Exception {
    return JsonUtils.stringToObject(instancePartitionsMapString,
        new TypeReference<Map<InstancePartitionsType, InstancePartitions>>() {
        });
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }
}
