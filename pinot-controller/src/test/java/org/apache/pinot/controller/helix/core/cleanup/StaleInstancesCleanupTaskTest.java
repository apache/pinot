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
package org.apache.pinot.controller.helix.core.cleanup;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.PinotResourceManagerResponse;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class StaleInstancesCleanupTaskTest {

  private static final String CLUSTER_NAME = "testCluster";
  private static final String TABLE_NAME = "testTable_OFFLINE";
  private static final String SEGMENT_1 = "testTable__0__0__20250101T0000Z";
  private static final String SEGMENT_2 = "testTable__0__1__20250102T0000Z";
  private static final String SERVER_IN_USE = "Server_server1.example.com_8098";
  private static final String SERVER_NOT_IN_USE = "Server_server2.example.com_8098";
  private static final String BROKER_IN_USE = "Broker_broker1.example.com_8099";
  private static final long RETENTION_MS = 1000L;

  @Mock
  private PinotHelixResourceManager _pinotHelixResourceManager;
  @Mock
  private LeadControllerManager _leadControllerManager;
  @Mock
  private ControllerMetrics _controllerMetrics;

  private AutoCloseable _mocks;
  private StaleInstancesCleanupTask _task;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);

    ControllerConf controllerConf = new ControllerConf();
    controllerConf.setStaleInstanceCleanupTaskFrequencyInSeconds("60");
    controllerConf.setStaleInstancesCleanupTaskInstancesRetentionPeriod("1s");

    _task = new StaleInstancesCleanupTask(
        _pinotHelixResourceManager, _leadControllerManager, controllerConf, _controllerMetrics);

    when(_leadControllerManager.isLeaderForTable("StaleInstancesCleanupTask")).thenReturn(true);
    when(_pinotHelixResourceManager.getHelixClusterName()).thenReturn(CLUSTER_NAME);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  /**
   * Verifies that an offline server still referenced in a table's IdealState
   * is NOT attempted to be dropped, validating that getServerInstancesInUse()
   * correctly extracts instances from segment-level partition keys.
   */
  @Test
  public void testServerInIdealStateIsNotDropped() {
    IdealState tableIdealState = buildTableIdealState(TABLE_NAME,
        Map.of(
            SEGMENT_1, Map.of(SERVER_IN_USE, "ONLINE"),
            SEGMENT_2, Map.of(SERVER_IN_USE, "ONLINE")
        ));

    when(_pinotHelixResourceManager.getAllTables()).thenReturn(List.of(TABLE_NAME));
    when(_pinotHelixResourceManager.getTableIdealState(TABLE_NAME)).thenReturn(tableIdealState);

    IdealState brokerIdealState = buildBrokerIdealState(
        Map.of(TABLE_NAME, Map.of(BROKER_IN_USE, "ONLINE")));
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(_pinotHelixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    when(helixAdmin.getResourceIdealState(CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE))
        .thenReturn(brokerIdealState);

    List<String> allInstances = Arrays.asList(SERVER_IN_USE, BROKER_IN_USE);
    when(_pinotHelixResourceManager.getAllInstances()).thenReturn(allInstances);
    when(_pinotHelixResourceManager.getOnlineInstanceList()).thenReturn(Collections.emptyList());
    when(_pinotHelixResourceManager.isInstanceOfflineFor(eq(SERVER_IN_USE), eq(RETENTION_MS))).thenReturn(true);

    _task.runTask(new Properties());

    verify(_pinotHelixResourceManager, never()).dropInstance(SERVER_IN_USE);
  }

  /**
   * Verifies that an offline server NOT referenced in any table's IdealState
   * IS attempted to be dropped.
   */
  @Test
  public void testServerNotInIdealStateIsDropped() {
    IdealState tableIdealState = buildTableIdealState(TABLE_NAME,
        Map.of(
            SEGMENT_1, Map.of(SERVER_IN_USE, "ONLINE"),
            SEGMENT_2, Map.of(SERVER_IN_USE, "ONLINE")
        ));

    when(_pinotHelixResourceManager.getAllTables()).thenReturn(List.of(TABLE_NAME));
    when(_pinotHelixResourceManager.getTableIdealState(TABLE_NAME)).thenReturn(tableIdealState);

    IdealState brokerIdealState = buildBrokerIdealState(
        Map.of(TABLE_NAME, Map.of(BROKER_IN_USE, "ONLINE")));
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(_pinotHelixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    when(helixAdmin.getResourceIdealState(CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE))
        .thenReturn(brokerIdealState);

    List<String> allInstances = Arrays.asList(SERVER_IN_USE, SERVER_NOT_IN_USE, BROKER_IN_USE);
    when(_pinotHelixResourceManager.getAllInstances()).thenReturn(allInstances);
    when(_pinotHelixResourceManager.getOnlineInstanceList()).thenReturn(List.of(SERVER_IN_USE, BROKER_IN_USE));
    when(_pinotHelixResourceManager.isInstanceOfflineFor(eq(SERVER_NOT_IN_USE), eq(RETENTION_MS))).thenReturn(true);
    when(_pinotHelixResourceManager.dropInstance(SERVER_NOT_IN_USE))
        .thenReturn(PinotResourceManagerResponse.success("dropped"));

    _task.runTask(new Properties());

    verify(_pinotHelixResourceManager).dropInstance(SERVER_NOT_IN_USE);
    verify(_pinotHelixResourceManager, never()).dropInstance(SERVER_IN_USE);
  }

  /**
   * Verifies behavior with multiple tables — a server referenced in ANY table's
   * IdealState should be considered in use.
   */
  @Test
  public void testServerInUseAcrossMultipleTables() {
    String table1 = "table1_OFFLINE";
    String table2 = "table2_OFFLINE";
    String segment1 = "table1__0__0__20250101T0000Z";
    String segment2 = "table2__0__0__20250101T0000Z";
    String serverOnlyInTable1 = "Server_s1.example.com_8098";
    String serverOnlyInTable2 = "Server_s2.example.com_8098";
    String serverInBothTables = "Server_s3.example.com_8098";

    IdealState is1 = buildTableIdealState(table1,
        Map.of(segment1, Map.of(serverOnlyInTable1, "ONLINE", serverInBothTables, "ONLINE")));
    IdealState is2 = buildTableIdealState(table2,
        Map.of(segment2, Map.of(serverOnlyInTable2, "ONLINE", serverInBothTables, "ONLINE")));

    when(_pinotHelixResourceManager.getAllTables()).thenReturn(List.of(table1, table2));
    when(_pinotHelixResourceManager.getTableIdealState(table1)).thenReturn(is1);
    when(_pinotHelixResourceManager.getTableIdealState(table2)).thenReturn(is2);

    IdealState brokerIdealState = buildBrokerIdealState(Collections.emptyMap());
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(_pinotHelixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    when(helixAdmin.getResourceIdealState(CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE))
        .thenReturn(brokerIdealState);

    List<String> allInstances =
        Arrays.asList(serverOnlyInTable1, serverOnlyInTable2, serverInBothTables, SERVER_NOT_IN_USE);
    when(_pinotHelixResourceManager.getAllInstances()).thenReturn(allInstances);
    when(_pinotHelixResourceManager.getOnlineInstanceList()).thenReturn(Collections.emptyList());
    when(_pinotHelixResourceManager.isInstanceOfflineFor(anyString(), eq(RETENTION_MS))).thenReturn(true);
    when(_pinotHelixResourceManager.dropInstance(anyString()))
        .thenReturn(PinotResourceManagerResponse.success("dropped"));

    _task.runTask(new Properties());

    verify(_pinotHelixResourceManager, never()).dropInstance(serverOnlyInTable1);
    verify(_pinotHelixResourceManager, never()).dropInstance(serverOnlyInTable2);
    verify(_pinotHelixResourceManager, never()).dropInstance(serverInBothTables);
    verify(_pinotHelixResourceManager).dropInstance(SERVER_NOT_IN_USE);
  }

  /**
   * Verifies that a null IdealState (e.g., table being deleted) is handled gracefully.
   */
  @Test
  public void testNullIdealStateIsHandledGracefully() {
    when(_pinotHelixResourceManager.getAllTables()).thenReturn(List.of(TABLE_NAME));
    when(_pinotHelixResourceManager.getTableIdealState(TABLE_NAME)).thenReturn(null);

    IdealState brokerIdealState = buildBrokerIdealState(Collections.emptyMap());
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(_pinotHelixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    when(helixAdmin.getResourceIdealState(CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE))
        .thenReturn(brokerIdealState);

    List<String> allInstances = List.of(SERVER_IN_USE);
    when(_pinotHelixResourceManager.getAllInstances()).thenReturn(allInstances);
    when(_pinotHelixResourceManager.getOnlineInstanceList()).thenReturn(Collections.emptyList());
    when(_pinotHelixResourceManager.isInstanceOfflineFor(eq(SERVER_IN_USE), eq(RETENTION_MS))).thenReturn(true);
    when(_pinotHelixResourceManager.dropInstance(SERVER_IN_USE))
        .thenReturn(PinotResourceManagerResponse.success("dropped"));

    _task.runTask(new Properties());

    verify(_pinotHelixResourceManager).dropInstance(SERVER_IN_USE);
  }

  /**
   * Verifies that a table with an empty IdealState (no segments) contributes
   * nothing to the in-use set, rather than causing errors.
   */
  @Test
  public void testEmptyIdealStateDoesNotCauseErrors() {
    IdealState emptyIdealState = buildTableIdealState(TABLE_NAME, Collections.emptyMap());

    when(_pinotHelixResourceManager.getAllTables()).thenReturn(List.of(TABLE_NAME));
    when(_pinotHelixResourceManager.getTableIdealState(TABLE_NAME)).thenReturn(emptyIdealState);

    IdealState brokerIdealState = buildBrokerIdealState(Collections.emptyMap());
    HelixAdmin helixAdmin = mock(HelixAdmin.class);
    when(_pinotHelixResourceManager.getHelixAdmin()).thenReturn(helixAdmin);
    when(helixAdmin.getResourceIdealState(CLUSTER_NAME, CommonConstants.Helix.BROKER_RESOURCE_INSTANCE))
        .thenReturn(brokerIdealState);

    List<String> allInstances = List.of(SERVER_NOT_IN_USE);
    when(_pinotHelixResourceManager.getAllInstances()).thenReturn(allInstances);
    when(_pinotHelixResourceManager.getOnlineInstanceList()).thenReturn(Collections.emptyList());
    when(_pinotHelixResourceManager.isInstanceOfflineFor(eq(SERVER_NOT_IN_USE), eq(RETENTION_MS))).thenReturn(true);
    when(_pinotHelixResourceManager.dropInstance(SERVER_NOT_IN_USE))
        .thenReturn(PinotResourceManagerResponse.success("dropped"));

    _task.runTask(new Properties());

    verify(_pinotHelixResourceManager).dropInstance(SERVER_NOT_IN_USE);
  }

  private static IdealState buildTableIdealState(String tableName,
      Map<String, Map<String, String>> segmentAssignment) {
    ZNRecord znRecord = new ZNRecord(tableName);
    for (Map.Entry<String, Map<String, String>> entry : segmentAssignment.entrySet()) {
      znRecord.setMapField(entry.getKey(), new HashMap<>(entry.getValue()));
    }
    IdealState idealState = new IdealState(znRecord);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    return idealState;
  }

  private static IdealState buildBrokerIdealState(Map<String, Map<String, String>> tableAssignment) {
    ZNRecord znRecord = new ZNRecord(CommonConstants.Helix.BROKER_RESOURCE_INSTANCE);
    for (Map.Entry<String, Map<String, String>> entry : tableAssignment.entrySet()) {
      znRecord.setMapField(entry.getKey(), new HashMap<>(entry.getValue()));
    }
    IdealState idealState = new IdealState(znRecord);
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    return idealState;
  }
}
