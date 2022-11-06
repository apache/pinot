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
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pinot.common.assignment.InstancePartitions;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.assignment.InstanceAssignmentConfig;
import org.apache.pinot.spi.config.table.assignment.InstancePartitionsType;
import org.apache.pinot.spi.config.table.assignment.InstanceReplicaGroupPartitionConfig;
import org.apache.pinot.spi.config.table.assignment.InstanceTagPoolConfig;
import org.apache.pinot.spi.config.tenant.Tenant;
import org.apache.pinot.spi.config.tenant.TenantRole;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


@Test(groups = "stateless")
public class PinotInstanceAssignmentRestletResourceStatelessTest extends ControllerTest {
  private static final String BROKER_TENANT_NAME = "testBrokerTenant";
  private static final String SERVER_TENANT_NAME = "testServerTenant";

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String TIME_COLUMN_NAME = "daysSinceEpoch";

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    Map<String, Object> properties = getDefaultControllerConfiguration();
    properties.put(ControllerConf.CLUSTER_TENANT_ISOLATION_ENABLE, false);
    startController(properties);

    addFakeBrokerInstancesToAutoJoinHelixCluster(1, false);
    addFakeServerInstancesToAutoJoinHelixCluster(2, false);

    // Create broker and server tenant
    Tenant brokerTenant = new Tenant(TenantRole.BROKER, BROKER_TENANT_NAME, 1, 0, 0);
    _helixResourceManager.createBrokerTenant(brokerTenant);
    Tenant serverTenant = new Tenant(TenantRole.SERVER, SERVER_TENANT_NAME, 2, 1, 1);
    _helixResourceManager.createServerTenant(serverTenant);
  }

  @Test
  public void testInstanceAssignment()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addDateTime(TIME_COLUMN_NAME, DataType.INT, "1:DAYS:EPOCH", "1:DAYS").build();
    _helixResourceManager.addSchema(schema, true, false);
    TableConfig offlineTableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).build();
    _helixResourceManager.addTable(offlineTableConfig);
    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setBrokerTenant(BROKER_TENANT_NAME)
            .setServerTenant(SERVER_TENANT_NAME).setLLC(true)
            .setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()).build();
    _helixResourceManager.addTable(realtimeTableConfig);

    // There should be no instance partitions
    try {
      getInstancePartitionsMap();
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Failed to find the instance partitions"));
    }

    // Assign instances should fail
    try {
      sendPostRequest(_controllerRequestURLBuilder.forInstanceAssign(RAW_TABLE_NAME, null, true), null);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Failed to find the instance assignment config"));
    }

    // Add OFFLINE instance assignment config to the offline table config
    InstanceAssignmentConfig offlineInstanceAssignmentConfig = new InstanceAssignmentConfig(
        new InstanceTagPoolConfig(TagNameUtils.getOfflineTagForTenant(SERVER_TENANT_NAME), false, 0, null), null,
        new InstanceReplicaGroupPartitionConfig(false, 0, 0, 0, 0, 0, false));
    offlineTableConfig.setInstanceAssignmentConfigMap(
        Collections.singletonMap(InstancePartitionsType.OFFLINE, offlineInstanceAssignmentConfig));
    _helixResourceManager.setExistingTableConfig(offlineTableConfig);

    // OFFLINE instance partitions should be generated
    Map<InstancePartitionsType, InstancePartitions> instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 1);
    InstancePartitions offlineInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    assertNotNull(offlineInstancePartitions);
    assertEquals(offlineInstancePartitions.getNumReplicaGroups(), 1);
    assertEquals(offlineInstancePartitions.getNumPartitions(), 1);
    assertEquals(offlineInstancePartitions.getInstances(0, 0).size(), 1);
    String offlineInstanceId = offlineInstancePartitions.getInstances(0, 0).get(0);

    // Add CONSUMING instance assignment config to the real-time table config
    InstanceAssignmentConfig consumingInstanceAssignmentConfig = new InstanceAssignmentConfig(
        new InstanceTagPoolConfig(TagNameUtils.getRealtimeTagForTenant(SERVER_TENANT_NAME), false, 0, null), null,
        new InstanceReplicaGroupPartitionConfig(false, 0, 0, 0, 0, 0, false));
    realtimeTableConfig.setInstanceAssignmentConfigMap(
        Collections.singletonMap(InstancePartitionsType.CONSUMING, consumingInstanceAssignmentConfig));
    _helixResourceManager.setExistingTableConfig(realtimeTableConfig);

    // CONSUMING instance partitions should be generated
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 2);
    offlineInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    assertNotNull(offlineInstancePartitions);
    assertEquals(offlineInstancePartitions.getNumReplicaGroups(), 1);
    assertEquals(offlineInstancePartitions.getNumPartitions(), 1);
    assertEquals(offlineInstancePartitions.getInstances(0, 0), Collections.singletonList(offlineInstanceId));
    InstancePartitions consumingInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    assertNotNull(consumingInstancePartitions);
    assertEquals(consumingInstancePartitions.getNumReplicaGroups(), 1);
    assertEquals(consumingInstancePartitions.getNumPartitions(), 1);
    assertEquals(consumingInstancePartitions.getInstances(0, 0).size(), 1);
    String consumingInstanceId = consumingInstancePartitions.getInstances(0, 0).get(0);

    // Use OFFLINE instance assignment config as the COMPLETED instance assignment config
    realtimeTableConfig.setInstanceAssignmentConfigMap(
        new TreeMap<InstancePartitionsType, InstanceAssignmentConfig>() {{
          put(InstancePartitionsType.CONSUMING, consumingInstanceAssignmentConfig);
          put(InstancePartitionsType.COMPLETED, offlineInstanceAssignmentConfig);
        }});
    _helixResourceManager.setExistingTableConfig(realtimeTableConfig);

    // COMPLETED instance partitions should be generated
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 3);
    offlineInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    assertNotNull(offlineInstancePartitions);
    assertEquals(offlineInstancePartitions.getNumReplicaGroups(), 1);
    assertEquals(offlineInstancePartitions.getNumPartitions(), 1);
    assertEquals(offlineInstancePartitions.getInstances(0, 0), Collections.singletonList(offlineInstanceId));
    consumingInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    assertNotNull(consumingInstancePartitions);
    assertEquals(consumingInstancePartitions.getNumReplicaGroups(), 1);
    assertEquals(consumingInstancePartitions.getNumPartitions(), 1);
    assertEquals(consumingInstancePartitions.getInstances(0, 0), Collections.singletonList(consumingInstanceId));
    InstancePartitions completedInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.COMPLETED);
    assertEquals(completedInstancePartitions.getNumReplicaGroups(), 1);
    assertEquals(completedInstancePartitions.getNumPartitions(), 1);
    assertEquals(completedInstancePartitions.getInstances(0, 0), Collections.singletonList(offlineInstanceId));

    // Test fetching instance partitions by table name with type suffix
    instancePartitionsMap = deserializeInstancePartitionsMap(sendGetRequest(
        _controllerRequestURLBuilder.forInstancePartitions(TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME),
            null)));
    assertEquals(instancePartitionsMap.size(), 1);
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.OFFLINE));
    instancePartitionsMap = deserializeInstancePartitionsMap(sendGetRequest(
        _controllerRequestURLBuilder.forInstancePartitions(TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME),
            null)));
    assertEquals(instancePartitionsMap.size(), 2);
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.CONSUMING));
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.COMPLETED));

    // Test fetching instance partitions by table name and instance partitions type
    for (InstancePartitionsType instancePartitionsType : InstancePartitionsType.values()) {
      instancePartitionsMap = deserializeInstancePartitionsMap(
          sendGetRequest(_controllerRequestURLBuilder.forInstancePartitions(RAW_TABLE_NAME, instancePartitionsType)));
      assertEquals(instancePartitionsMap.size(), 1);
      assertEquals(instancePartitionsMap.get(instancePartitionsType).getInstancePartitionsName(),
          instancePartitionsType.getInstancePartitionsName(RAW_TABLE_NAME));
    }

    // Remove the instance partitions for both offline and real-time table
    sendDeleteRequest(_controllerRequestURLBuilder.forInstancePartitions(RAW_TABLE_NAME, null));
    try {
      getInstancePartitionsMap();
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Failed to find the instance partitions"));
    }

    // Assign instances without instance partitions type (dry run)
    instancePartitionsMap = deserializeInstancePartitionsMap(
        sendPostRequest(_controllerRequestURLBuilder.forInstanceAssign(RAW_TABLE_NAME, null, true), null));
    assertEquals(instancePartitionsMap.size(), 3);
    offlineInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.OFFLINE);
    assertNotNull(offlineInstancePartitions);
    assertEquals(offlineInstancePartitions.getNumReplicaGroups(), 1);
    assertEquals(offlineInstancePartitions.getNumPartitions(), 1);
    assertEquals(offlineInstancePartitions.getInstances(0, 0), Collections.singletonList(offlineInstanceId));
    consumingInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.CONSUMING);
    assertNotNull(consumingInstancePartitions);
    assertEquals(consumingInstancePartitions.getNumReplicaGroups(), 1);
    assertEquals(consumingInstancePartitions.getNumPartitions(), 1);
    assertEquals(consumingInstancePartitions.getInstances(0, 0), Collections.singletonList(consumingInstanceId));
    completedInstancePartitions = instancePartitionsMap.get(InstancePartitionsType.COMPLETED);
    assertEquals(completedInstancePartitions.getNumReplicaGroups(), 1);
    assertEquals(completedInstancePartitions.getNumPartitions(), 1);
    assertEquals(completedInstancePartitions.getInstances(0, 0), Collections.singletonList(offlineInstanceId));

    // Instance partitions should not be persisted
    try {
      getInstancePartitionsMap();
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Failed to find the instance partitions"));
    }

    // Assign instances for both offline and real-time table
    sendPostRequest(_controllerRequestURLBuilder.forInstanceAssign(RAW_TABLE_NAME, null, false), null);

    // Instance partitions should be persisted
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 3);

    // Remove the instance partitions for real-time table
    sendDeleteRequest(
        _controllerRequestURLBuilder.forInstancePartitions(TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME),
            null));
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

    // Replace OFFLINE instance with CONSUMING instance for COMPLETED instance partitions
    instancePartitionsMap = deserializeInstancePartitionsMap(sendPostRequest(
        _controllerRequestURLBuilder.forInstanceReplace(RAW_TABLE_NAME, InstancePartitionsType.COMPLETED,
            offlineInstanceId, consumingInstanceId), null));
    assertEquals(instancePartitionsMap.size(), 1);
    assertEquals(instancePartitionsMap.get(InstancePartitionsType.COMPLETED).getInstances(0, 0),
        Collections.singletonList(consumingInstanceId));

    // Replace the instance again using real-time table name (old instance does not exist)
    try {
      sendPostRequest(
          _controllerRequestURLBuilder.forInstanceReplace(TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME),
              null, offlineInstanceId, consumingInstanceId), null);
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Failed to find the old instance"));
    }

    // Post the CONSUMING instance partitions
    instancePartitionsMap = deserializeInstancePartitionsMap(
        sendPutRequest(_controllerRequestURLBuilder.forInstancePartitions(RAW_TABLE_NAME, null),
            consumingInstancePartitions.toJsonString()));
    assertEquals(instancePartitionsMap.size(), 1);
    assertEquals(instancePartitionsMap.get(InstancePartitionsType.CONSUMING).getInstances(0, 0),
        Collections.singletonList(consumingInstanceId));

    // OFFLINE instance partitions should have OFFLINE instance, CONSUMING and COMPLETED instance partitions should have
    // CONSUMING instance
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 3);
    assertEquals(instancePartitionsMap.get(InstancePartitionsType.OFFLINE).getInstances(0, 0),
        Collections.singletonList(offlineInstanceId));
    assertEquals(instancePartitionsMap.get(InstancePartitionsType.CONSUMING).getInstances(0, 0),
        Collections.singletonList(consumingInstanceId));
    assertEquals(instancePartitionsMap.get(InstancePartitionsType.COMPLETED).getInstances(0, 0),
        Collections.singletonList(consumingInstanceId));

    // Delete the offline table
    _helixResourceManager.deleteOfflineTable(RAW_TABLE_NAME);
    instancePartitionsMap = getInstancePartitionsMap();
    assertEquals(instancePartitionsMap.size(), 2);
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.CONSUMING));
    assertTrue(instancePartitionsMap.containsKey(InstancePartitionsType.COMPLETED));

    // Delete the real-time table
    _helixResourceManager.deleteRealtimeTable(RAW_TABLE_NAME);
    try {
      getInstancePartitionsMap();
      fail();
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("Failed to find the instance partitions"));
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
