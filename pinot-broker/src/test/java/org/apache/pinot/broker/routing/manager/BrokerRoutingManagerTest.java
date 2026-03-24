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
package org.apache.pinot.broker.routing.manager;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.routing.instanceselector.InstanceSelector;
import org.apache.pinot.broker.routing.segmentmetadata.SegmentZkMetadataFetcher;
import org.apache.pinot.broker.routing.segmentpartition.SegmentPartitionMetadataManager;
import org.apache.pinot.broker.routing.segmentpreselector.SegmentPreSelector;
import org.apache.pinot.broker.routing.segmentpruner.SegmentPruner;
import org.apache.pinot.broker.routing.segmentselector.SegmentSelector;
import org.apache.pinot.broker.routing.timeboundary.TimeBoundaryManager;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.core.routing.TablePartitionInfo;
import org.apache.pinot.core.routing.TablePartitionReplicatedServersInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class BrokerRoutingManagerTest {
  private static final String SERVER_INSTANCE_ID = "Server_localhost_8000";
  private static final String SERVER_HOST = "localhost";
  private static final int SERVER_PORT = 8000;
  private static final String INSTANCE_CONFIGS_PATH = "/CONFIGS/PARTICIPANT";
  private static final String TEST_TABLE = "testTable_OFFLINE";

  private AutoCloseable _mocks;

  @Mock
  private BrokerMetrics _brokerMetrics;

  @Mock
  private ServerRoutingStatsManager _serverRoutingStatsManager;

  @Mock
  private HelixManager _helixManager;

  @Mock
  private HelixDataAccessor _helixDataAccessor;

  @Mock
  private BaseDataAccessor<ZNRecord> _zkDataAccessor;

  @Mock
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  @Mock
  private PropertyKey.Builder _keyBuilder;

  @Mock
  private PropertyKey _instanceConfigsKey;

  @Mock
  private Consumer<ServerInstance> _serverReenableCallback;

  private BrokerRoutingManager _routingManager;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);

    // Set up Helix mocks
    when(_helixManager.getHelixDataAccessor()).thenReturn(_helixDataAccessor);
    when(_helixManager.getHelixPropertyStore()).thenReturn(_propertyStore);
    when(_helixDataAccessor.getBaseDataAccessor()).thenReturn(_zkDataAccessor);
    when(_helixDataAccessor.keyBuilder()).thenReturn(_keyBuilder);
    when(_keyBuilder.instanceConfigs()).thenReturn(_instanceConfigsKey);
    when(_keyBuilder.externalViews()).thenReturn(mock(PropertyKey.class));
    when(_keyBuilder.idealStates()).thenReturn(mock(PropertyKey.class));
    when(_instanceConfigsKey.getPath()).thenReturn(INSTANCE_CONFIGS_PATH);

    // Mock paths for external views and ideal states
    PropertyKey evKey = mock(PropertyKey.class);
    PropertyKey isKey = mock(PropertyKey.class);
    when(_keyBuilder.externalViews()).thenReturn(evKey);
    when(_keyBuilder.idealStates()).thenReturn(isKey);
    when(evKey.getPath()).thenReturn("/EXTERNALVIEW");
    when(isKey.getPath()).thenReturn("/IDEALSTATES");

    // Create routing manager
    _routingManager = new BrokerRoutingManager(_brokerMetrics, _serverRoutingStatsManager, new PinotConfiguration());
    _routingManager.init(_helixManager);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testNoErrorWhenCallbackNotSet() {
    // Don't set callback

    // Enable server
    List<ZNRecord> instanceConfigs = Collections.singletonList(createEnabledServerZNRecord(SERVER_INSTANCE_ID));
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
            anyInt(), anyInt())).thenReturn(instanceConfigs);
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Exclude server
    _routingManager.excludeServerFromRouting(SERVER_INSTANCE_ID);

    // Disable then re-enable
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
            anyInt(), anyInt())).thenReturn(Collections.emptyList());
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
            anyInt(), anyInt())).thenReturn(instanceConfigs);

    // Should not throw NPE
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Server should be re-enabled in the map
    assertTrue(_routingManager.getEnabledServerInstanceMap().containsKey(SERVER_INSTANCE_ID));
  }

  @Test
  public void testServerReenableCallbackInvokedWhenExcludedServerReenabled() {
    // Set up callback
    _routingManager.setServerReenableCallback(_serverReenableCallback);

    // First, enable the server by processing instance config change
    List<ZNRecord> instanceConfigs = Collections.singletonList(createEnabledServerZNRecord(SERVER_INSTANCE_ID));
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
        anyInt(), anyInt())).thenReturn(instanceConfigs);

    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Verify server is now in enabled map
    assertTrue(_routingManager.getEnabledServerInstanceMap().containsKey(SERVER_INSTANCE_ID));

    // Exclude the server (simulating failure detector marking it unhealthy)
    _routingManager.excludeServerFromRouting(SERVER_INSTANCE_ID);

    // Now simulate server being disabled then re-enabled (e.g., restart)
    // First, disable
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
        anyInt(), anyInt())).thenReturn(Collections.emptyList());
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Then re-enable
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
        anyInt(), anyInt())).thenReturn(instanceConfigs);
    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Verify callback was invoked with correct ServerInstance
    ArgumentCaptor<ServerInstance> captor = ArgumentCaptor.forClass(ServerInstance.class);
    verify(_serverReenableCallback).accept(captor.capture());

    ServerInstance capturedInstance = captor.getValue();
    assertEquals(capturedInstance.getHostname(), SERVER_HOST);
    assertEquals(capturedInstance.getPort(), SERVER_PORT);
  }

  @Test
  public void testServerReenableCallbackNotInvokedForNewServer() {
    // Set up callback
    _routingManager.setServerReenableCallback(_serverReenableCallback);

    // Enable a new server (never excluded)
    List<ZNRecord> instanceConfigs = Collections.singletonList(createEnabledServerZNRecord(SERVER_INSTANCE_ID));
    when(_zkDataAccessor.getChildren(eq(INSTANCE_CONFIGS_PATH), any(), eq(AccessOption.PERSISTENT),
        anyInt(), anyInt())).thenReturn(instanceConfigs);

    _routingManager.processClusterChange(ChangeType.INSTANCE_CONFIG);

    // Verify callback was NOT invoked (server was never excluded)
    verify(_serverReenableCallback, never()).accept(any());
  }

  @Test
  public void testSamplerContextSharesTimeBoundaryAndPartitionMetadata()
      throws Exception {
    TimeBoundaryManager timeBoundaryManager = mock(TimeBoundaryManager.class);
    SegmentPartitionMetadataManager partitionMetadataManager = mock(SegmentPartitionMetadataManager.class);
    TimeBoundaryInfo expectedTimeBoundaryInfo = new TimeBoundaryInfo("DaysSinceEpoch", "20000");
    TablePartitionInfo expectedPartitionInfo =
        new TablePartitionInfo(TEST_TABLE, "partitionCol", "Modulo", 2,
            List.of(Collections.emptyList(), Collections.emptyList()), Collections.emptyList());
    TablePartitionReplicatedServersInfo expectedReplicatedServersInfo = mock(TablePartitionReplicatedServersInfo.class);
    when(timeBoundaryManager.getTimeBoundaryInfo()).thenReturn(expectedTimeBoundaryInfo);
    when(partitionMetadataManager.getTablePartitionInfo()).thenReturn(expectedPartitionInfo);
    when(partitionMetadataManager.getTablePartitionReplicatedServersInfo()).thenReturn(expectedReplicatedServersInfo);

    Object routingEntry = createRoutingEntry(TEST_TABLE, timeBoundaryManager, partitionMetadataManager, Map.of());
    putRoutingEntry(TEST_TABLE, routingEntry);

    assertSame(_routingManager.getTimeBoundaryInfo(TEST_TABLE), expectedTimeBoundaryInfo);
    assertSame(_routingManager.getTablePartitionInfo(TEST_TABLE), expectedPartitionInfo);
    assertSame(_routingManager.getTablePartitionReplicatedServersInfo(TEST_TABLE), expectedReplicatedServersInfo);
  }

  private static Object createRoutingEntry(String tableNameWithType, TimeBoundaryManager timeBoundaryManager,
      SegmentPartitionMetadataManager partitionMetadataManager, Map<String, ?> samplerInfos)
      throws Exception {
    Class<?> routingEntryClass = Class.forName(BaseBrokerRoutingManager.class.getName() + "$RoutingEntry");
    Constructor<?> constructor = routingEntryClass.getDeclaredConstructor(String.class, String.class, String.class,
        SegmentPreSelector.class, SegmentSelector.class, List.class, InstanceSelector.class, int.class, int.class,
        SegmentZkMetadataFetcher.class, TimeBoundaryManager.class, SegmentPartitionMetadataManager.class, Long.class,
        Map.class, boolean.class);
    constructor.setAccessible(true);
    return constructor.newInstance(tableNameWithType, "/IDEALSTATES/" + tableNameWithType,
        "/EXTERNALVIEW/" + tableNameWithType, mock(SegmentPreSelector.class), mock(SegmentSelector.class),
        Collections.<SegmentPruner>emptyList(), mock(InstanceSelector.class), 1, 1,
        mock(SegmentZkMetadataFetcher.class), timeBoundaryManager, partitionMetadataManager, null, samplerInfos,
        false);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void putRoutingEntry(String tableNameWithType, Object routingEntry) {
    Map routingEntries = _routingManager._routingEntryMap;
    routingEntries.put(tableNameWithType, routingEntry);
  }

  /**
   * Creates a ZNRecord representing an enabled server instance.
   */
  private ZNRecord createEnabledServerZNRecord(String instanceId) {
    ZNRecord record = new ZNRecord(instanceId);
    record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_ENABLED.name(), "true");
    record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_HOST.name(),
        instanceId.split("_")[1]); // Extract host from Server_host_port
    record.setSimpleField(InstanceConfig.InstanceConfigProperty.HELIX_PORT.name(),
        instanceId.split("_")[2]); // Extract port from Server_host_port
    // Don't set IS_SHUTDOWN_IN_PROGRESS or QUERIES_DISABLED (they default to false)
    return record;
  }
}
