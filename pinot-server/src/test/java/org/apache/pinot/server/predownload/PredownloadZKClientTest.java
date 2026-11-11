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
package org.apache.pinot.server.predownload;

import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyType;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.server.predownload.PredownloadTestUtil.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class PredownloadZKClientTest {
  private PredownloadZKClient _predownloadZkClient;
  private HelixZkClient _helixZkClient;

  @BeforeClass
  public void setUp()
      throws Exception {
    _predownloadZkClient = new PredownloadZKClient(ZK_ADDRESS, INSTANCE_ID, CLUSTER_NAME);
    _helixZkClient = mock(HelixZkClient.class);
    when(_helixZkClient.waitUntilConnected(anyLong(), any())).thenReturn(true);
    SharedZkClientFactory sharedZkClientFactory = mock(SharedZkClientFactory.class);
    when(sharedZkClientFactory.buildZkClient(any(HelixZkClient.ZkConnectionConfig.class),
        any(HelixZkClient.ZkClientConfig.class))).thenReturn(_helixZkClient);

    try (MockedStatic<SharedZkClientFactory> sharedZkClientFactoryMockedStatic = mockStatic(
        SharedZkClientFactory.class)) {
      sharedZkClientFactoryMockedStatic.when(() -> SharedZkClientFactory.getInstance())
          .thenReturn(sharedZkClientFactory);
      _predownloadZkClient.start();
      assertTrue(_predownloadZkClient.isStarted());
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _predownloadZkClient.close();
    assertFalse(_predownloadZkClient.isStarted());
  }

  @Test
  public void testGetDataAccessor() {
    try {
      _predownloadZkClient.getDataAccessor();
    } catch (Exception e) {
      fail("Expected no exception, but got: " + e.getMessage());
    }
  }

  @Test
  public void testGetInstanceConfig() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);

    when(_helixZkClient.exists(any())).thenReturn(false);
    assertThrows(HelixException.class, () -> _predownloadZkClient.getInstanceConfig(dataAccessor));

    when(_helixZkClient.exists(any())).thenReturn(true);
    when(dataAccessor.keyBuilder()).thenReturn(new org.apache.helix.PropertyKey.Builder(CLUSTER_NAME));
    try {
      _predownloadZkClient.getInstanceConfig(dataAccessor);
    } catch (Exception e) {
      fail("Expected no exception, but got: " + e.getMessage());
    }
  }

  @Test
  public void testGetSegmentsOfInstance() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    org.apache.helix.PropertyKey.Builder keyBuilder = spy(new org.apache.helix.PropertyKey.Builder(CLUSTER_NAME));
    when(dataAccessor.keyBuilder()).thenReturn(keyBuilder);

    // live instance null
    when(dataAccessor.getProperty(any(PropertyKey.class))).thenReturn(null);
    try (MockedStatic<PredownloadStatusRecorder> statusRecorderMockedStatic = mockStatic(
        PredownloadStatusRecorder.class)) {
      when(_helixZkClient.exists(any())).thenReturn(true);
      assertTrue(_predownloadZkClient.getSegmentsOfInstance(dataAccessor).isEmpty());
    }
    try (MockedStatic<PredownloadStatusRecorder> statusRecorderMockedStatic = mockStatic(
        PredownloadStatusRecorder.class)) {
      when(_helixZkClient.exists(any())).thenReturn(false);
      assertTrue(_predownloadZkClient.getSegmentsOfInstance(dataAccessor).isEmpty());
    }

    // live instance not null
    LiveInstance liveInstance = spy(new LiveInstance(INSTANCE_ID));
    when(dataAccessor.getProperty(any(PropertyKey.class))).thenReturn(liveInstance);
    when(liveInstance.getEphemeralOwner()).thenReturn(SESSION_ID);

    // no segments
    when(dataAccessor.getChildValues(any(PropertyKey.class))).thenReturn(null);
    assertTrue(_predownloadZkClient.getSegmentsOfInstance(dataAccessor).isEmpty());
    // with segments
    CurrentState currentState = spy(new CurrentState(TABLE_NAME));
    currentState.setState(SEGMENT_NAME, "ONLINE");
    currentState.setState(SECOND_SEGMENT_NAME, "ONLINE");
    currentState.setState(THIRD_SEGMENT_NAME, "OFFLINE");
    when(dataAccessor.getChildValues(any(PropertyKey.class), anyBoolean())).thenReturn(List.of(currentState));
    List<PredownloadSegmentInfo> predownloadSegmentInfoList = _predownloadZkClient.getSegmentsOfInstance(dataAccessor);
    assertEquals(predownloadSegmentInfoList.size(), 2);
    assertTrue(
        Arrays.asList(SEGMENT_NAME, SECOND_SEGMENT_NAME).contains(predownloadSegmentInfoList.get(0).getSegmentName()));
    assertTrue(
        Arrays.asList(SEGMENT_NAME, SECOND_SEGMENT_NAME).contains(predownloadSegmentInfoList.get(1).getSegmentName()));
  }

  @Test
  public void testUpdateSegmentMetadata()
      throws Exception {
    List<PredownloadSegmentInfo> predownloadSegmentInfoList =
        Arrays.asList(new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME),
            new PredownloadSegmentInfo(TABLE_NAME, SECOND_SEGMENT_NAME));
    Map<String, PredownloadTableInfo> tableInfoMap = new HashMap<>();
    PinotConfiguration pinotConfiguration = getPinotConfiguration();
    InstanceDataManagerConfig instanceDataManagerConfig = new HelixInstanceDataManagerConfig(pinotConfiguration);
    TableConfig tableConfig = mock(TableConfig.class);

    // table config null
    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(null);
      _predownloadZkClient.updateSegmentMetadata(predownloadSegmentInfoList, tableInfoMap, instanceDataManagerConfig);
      assertNull(tableInfoMap.get(TABLE_NAME));
      assertEquals(predownloadSegmentInfoList.get(0).getCrc(), 0);
      assertEquals(predownloadSegmentInfoList.get(1).getCrc(), 0);
    }

    // config table not null, only one of two segments exists
    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString()))
          .thenReturn(tableConfig);
      zkMetadataProviderMockedStatic.when(
              () -> ZKMetadataProvider.getSegmentZKMetadata(any(), eq(TABLE_NAME), eq(SEGMENT_NAME)))
          .thenReturn(createSegmentZKMetadata());
      zkMetadataProviderMockedStatic.when(
              () -> ZKMetadataProvider.getSegmentZKMetadata(any(), eq(TABLE_NAME), eq(SECOND_SEGMENT_NAME)))
          .thenReturn(null);
      _predownloadZkClient.updateSegmentMetadata(predownloadSegmentInfoList, tableInfoMap, instanceDataManagerConfig);
      assertNotNull(tableInfoMap.get(TABLE_NAME));
      assertEquals(predownloadSegmentInfoList.get(0).getCrc(), CRC);
      assertEquals(predownloadSegmentInfoList.get(1).getCrc(), 0);
    }
  }

  @Test
  public void testGetPeerServerURIs()
      throws Exception {
    // Pass accessor directly — no spy needed since method signature accepts accessor as param
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);
    when(dataAccessor.keyBuilder()).thenReturn(new PropertyKey.Builder(CLUSTER_NAME));

    ExternalView externalView = mock(ExternalView.class);
    String peerInstanceId = "Server_peer_8098";
    InstanceConfig peerConfig = mock(InstanceConfig.class);
    when(peerConfig.getHostName()).thenReturn("peerHost");
    ZNRecord peerRecord = new ZNRecord(peerInstanceId);
    peerRecord.setIntField(CommonConstants.Helix.Instance.ADMIN_PORT_KEY, 9000);
    when(peerConfig.getRecord()).thenReturn(peerRecord);

    // Case 1: ExternalView is null → empty list
    doAnswer(inv -> null).when(dataAccessor).getProperty(any(PropertyKey.class));
    assertTrue(_predownloadZkClient.getPeerServerURIs(dataAccessor, TABLE_NAME, SEGMENT_NAME, "http").isEmpty(),
        "Should return empty list when ExternalView is null");

    // Case 2: ExternalView found but segment has no state map → empty list
    doAnswer(inv -> {
      PropertyKey key = inv.getArgument(0);
      return (key.getType() == PropertyType.EXTERNALVIEW) ? externalView : null;
    }).when(dataAccessor).getProperty(any(PropertyKey.class));
    when(externalView.getStateMap(SEGMENT_NAME)).thenReturn(null);
    assertTrue(_predownloadZkClient.getPeerServerURIs(dataAccessor, TABLE_NAME, SEGMENT_NAME, "http").isEmpty(),
        "Should return empty list when segment not in ExternalView");

    // Case 3: Only self ONLINE → empty list
    // In @BeforeClass: new PredownloadZKClient(ZK_ADDRESS, INSTANCE_ID, CLUSTER_NAME)
    // → _instanceName = CLUSTER_NAME
    when(externalView.getStateMap(SEGMENT_NAME)).thenReturn(Map.of(CLUSTER_NAME, "ONLINE"));
    assertTrue(_predownloadZkClient.getPeerServerURIs(dataAccessor, TABLE_NAME, SEGMENT_NAME, "http").isEmpty(),
        "Should exclude self instance from peer list");

    // Case 4: peer ONLINE + one OFFLINE → only peer returned (self-exclusion tested in Case 3)
    Map<String, String> stateMap = new HashMap<>();
    stateMap.put(peerInstanceId, "ONLINE");      // peer → included
    stateMap.put("Server_offline", "OFFLINE");   // offline → excluded
    when(externalView.getStateMap(SEGMENT_NAME)).thenReturn(stateMap);
    doAnswer(inv -> {
      PropertyKey key = inv.getArgument(0);
      return (key.getType() == PropertyType.EXTERNALVIEW) ? externalView : peerConfig;
    }).when(dataAccessor).getProperty(any(PropertyKey.class));

    List<URI> uris = _predownloadZkClient.getPeerServerURIs(dataAccessor, TABLE_NAME, SEGMENT_NAME, "http");
    assertEquals(uris.size(), 1, "Should return exactly one peer URI");
    String uriStr = uris.get(0).toString();
    assertTrue(uriStr.contains("peerHost"), "URI should contain peer hostname");
    assertTrue(uriStr.contains("9000"), "URI should contain peer admin port");
    assertTrue(uriStr.contains(TABLE_NAME), "URI should contain table name");
    assertTrue(uriStr.contains(SEGMENT_NAME), "URI should contain segment name");
    assertTrue(uriStr.startsWith("http://"), "URI should use http scheme");

    // Case 5: Peer InstanceConfig is null → skipped, empty list
    when(externalView.getStateMap(SEGMENT_NAME)).thenReturn(Map.of(peerInstanceId, "ONLINE"));
    doAnswer(inv -> {
      PropertyKey key = inv.getArgument(0);
      return (key.getType() == PropertyType.EXTERNALVIEW) ? externalView : null;
    }).when(dataAccessor).getProperty(any(PropertyKey.class));
    assertTrue(_predownloadZkClient.getPeerServerURIs(dataAccessor, TABLE_NAME, SEGMENT_NAME, "http").isEmpty(),
        "Should skip peer when its InstanceConfig is null");
  }
}
