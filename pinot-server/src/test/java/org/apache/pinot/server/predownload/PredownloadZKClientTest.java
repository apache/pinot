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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.server.starter.helix.HelixInstanceDataManagerConfig;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.server.predownload.PredownloadTestUtil.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.AssertJUnit.*;


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
      Assert.fail("Expected no exception, but got: " + e.getMessage());
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
      Assert.fail("Expected no exception, but got: " + e.getMessage());
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
      assertEquals(0, _predownloadZkClient.getSegmentsOfInstance(dataAccessor).size());
    }
    try (MockedStatic<PredownloadStatusRecorder> statusRecorderMockedStatic = mockStatic(
        PredownloadStatusRecorder.class)) {
      when(_helixZkClient.exists(any())).thenReturn(false);
      assertEquals(0, _predownloadZkClient.getSegmentsOfInstance(dataAccessor).size());
    }

    // live instance not null
    LiveInstance liveInstance = spy(new LiveInstance(INSTANCE_ID));
    when(dataAccessor.getProperty(any(PropertyKey.class))).thenReturn(liveInstance);
    when(liveInstance.getEphemeralOwner()).thenReturn(SESSION_ID);

    // no segments
    when(dataAccessor.getChildValues(any(PropertyKey.class))).thenReturn(null);
    assertEquals(0, _predownloadZkClient.getSegmentsOfInstance(dataAccessor).size());
    // with segments
    CurrentState currentState = spy(new CurrentState(TABLE_NAME));
    currentState.setState(SEGMENT_NAME, "ONLINE");
    currentState.setState(SECOND_SEGMENT_NAME, "ONLINE");
    currentState.setState(THIRD_SEGMENT_NAME, "OFFLINE");
    when(dataAccessor.getChildValues(any(PropertyKey.class), anyBoolean())).thenReturn(List.of(currentState));
    List<PredownloadSegmentInfo> predownloadSegmentInfoList = _predownloadZkClient.getSegmentsOfInstance(dataAccessor);
    assertEquals(2, predownloadSegmentInfoList.size());
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
}
