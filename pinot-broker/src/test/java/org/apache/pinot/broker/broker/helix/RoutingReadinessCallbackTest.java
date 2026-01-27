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
package org.apache.pinot.broker.broker.helix;

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.core.routing.RoutingManager;
import org.apache.pinot.spi.utils.CommonConstants.Helix;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class RoutingReadinessCallbackTest {

  private static final String CLUSTER_NAME = "testCluster";
  private static final String INSTANCE_ID = "Broker_localhost_8099";
  private static final String TABLE_1 = "table1_OFFLINE";
  private static final String TABLE_2 = "table2_REALTIME";

  private HelixAdmin _helixAdmin;
  private RoutingManager _routingManager;
  private RoutingReadinessCallback _callback;

  @BeforeMethod
  public void setUp() {
    _helixAdmin = mock(HelixAdmin.class);
    _routingManager = mock(RoutingManager.class);
    _callback = new RoutingReadinessCallback(_helixAdmin, _routingManager, CLUSTER_NAME, INSTANCE_ID);
  }

  @Test
  public void testReturnsStartingWhenIdealStateNotFound() {
    when(_helixAdmin.getResourceIdealState(CLUSTER_NAME, Helix.BROKER_RESOURCE_INSTANCE)).thenReturn(null);

    assertThat(_callback.getServiceStatus()).isEqualTo(ServiceStatus.Status.STARTING);
    assertThat(_callback.getStatusDescription()).contains("Broker resource ideal state not found");
  }

  @Test
  public void testReturnsGoodWhenNoTablesAssigned() {
    IdealState idealState = createIdealStateWithTables();
    when(_helixAdmin.getResourceIdealState(CLUSTER_NAME, Helix.BROKER_RESOURCE_INSTANCE)).thenReturn(idealState);

    assertThat(_callback.getServiceStatus()).isEqualTo(ServiceStatus.Status.GOOD);
    assertThat(_callback.getStatusDescription()).isEqualTo(ServiceStatus.STATUS_DESCRIPTION_NONE);
  }

  @Test
  public void testReturnsStartingWhenRoutingMissing() {
    IdealState idealState = createIdealStateWithTables(TABLE_1, TABLE_2);
    when(_helixAdmin.getResourceIdealState(CLUSTER_NAME, Helix.BROKER_RESOURCE_INSTANCE)).thenReturn(idealState);
    when(_routingManager.routingExists(TABLE_1)).thenReturn(true);
    when(_routingManager.routingExists(TABLE_2)).thenReturn(false);

    assertThat(_callback.getServiceStatus()).isEqualTo(ServiceStatus.Status.STARTING);
    assertThat(_callback.getStatusDescription()).contains("Waiting for routing");
    assertThat(_callback.getStatusDescription()).contains(TABLE_2);
  }

  @Test
  public void testReturnsGoodWhenAllRoutingExists() {
    IdealState idealState = createIdealStateWithTables(TABLE_1, TABLE_2);
    when(_helixAdmin.getResourceIdealState(CLUSTER_NAME, Helix.BROKER_RESOURCE_INSTANCE)).thenReturn(idealState);
    when(_routingManager.routingExists(TABLE_1)).thenReturn(true);
    when(_routingManager.routingExists(TABLE_2)).thenReturn(true);

    assertThat(_callback.getServiceStatus()).isEqualTo(ServiceStatus.Status.GOOD);
    assertThat(_callback.getStatusDescription()).isEqualTo(ServiceStatus.STATUS_DESCRIPTION_NONE);
  }

  @Test
  public void testIdealStatePartitionSetReturnsTableNames() {
    // Verify that IdealState.getPartitionSet() returns table names
    // and getInstanceSet(tableName) returns broker instances
    IdealState idealState = createIdealStateWithTables(TABLE_1, TABLE_2);

    assertThat(idealState.getPartitionSet()).containsExactlyInAnyOrder(TABLE_1, TABLE_2);
    assertThat(idealState.getInstanceSet(TABLE_1)).containsExactly(INSTANCE_ID);
    assertThat(idealState.getInstanceSet(TABLE_2)).containsExactly(INSTANCE_ID);
  }

  @Test
  public void testStatusIsCachedOnceGood() {
    IdealState idealState = createIdealStateWithTables(TABLE_1);
    when(_helixAdmin.getResourceIdealState(CLUSTER_NAME, Helix.BROKER_RESOURCE_INSTANCE)).thenReturn(idealState);
    when(_routingManager.routingExists(TABLE_1)).thenReturn(true);
    assertThat(_callback.getServiceStatus()).isEqualTo(ServiceStatus.Status.GOOD);

    // Change mock to return STARTING conditions
    when(_routingManager.routingExists(TABLE_1)).thenReturn(false);

    // Should still return GOOD (cached)
    assertThat(_callback.getServiceStatus()).isEqualTo(ServiceStatus.Status.GOOD);
    assertThat(_callback.getStatusDescription()).isEqualTo(ServiceStatus.STATUS_DESCRIPTION_NONE);
  }

  private IdealState createIdealStateWithTables(String... tableNames) {
    ZNRecord record = new ZNRecord(Helix.BROKER_RESOURCE_INSTANCE);
    for (String tableName : tableNames) {
      Map<String, String> tableMap = new HashMap<>();
      tableMap.put(INSTANCE_ID, "ONLINE");
      record.setMapField(tableName, tableMap);
    }
    return new IdealState(record);
  }
}
