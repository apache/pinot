package org.apache.pinot.tools.predownload;

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

import static org.apache.pinot.tools.predownload.TestUtil.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.AssertJUnit.*;


public class ZKClientTest {
  private ZKClient zkClient;
  private HelixZkClient helixZkClient;

  @BeforeClass
  public void setUp()
      throws Exception {
    zkClient = new ZKClient(ZK_ADDRESS, INSTANCE_ID, CLUSTER_NAME);
    helixZkClient = mock(HelixZkClient.class);
    when(helixZkClient.waitUntilConnected(anyLong(), any())).thenReturn(true);
    SharedZkClientFactory sharedZkClientFactory = mock(SharedZkClientFactory.class);
    when(sharedZkClientFactory.buildZkClient(any(HelixZkClient.ZkConnectionConfig.class),
        any(HelixZkClient.ZkClientConfig.class))).thenReturn(helixZkClient);

    try (MockedStatic<SharedZkClientFactory> sharedZkClientFactoryMockedStatic = mockStatic(
        SharedZkClientFactory.class)) {
      sharedZkClientFactoryMockedStatic.when(() -> SharedZkClientFactory.getInstance())
          .thenReturn(sharedZkClientFactory);
      zkClient.start();
      assertTrue(zkClient.isStarted());
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    zkClient.close();
    assertFalse(zkClient.isStarted());
  }

  @Test
  public void testGetDataAccessor() {
    try {
      zkClient.getDataAccessor();
    } catch (Exception e) {
      Assert.fail("Expected no exception, but got: " + e.getMessage());
    }
  }

  @Test
  public void testGetInstanceConfig() {
    HelixDataAccessor dataAccessor = mock(HelixDataAccessor.class);

    when(helixZkClient.exists(any())).thenReturn(false);
    assertThrows(HelixException.class, () -> zkClient.getInstanceConfig(dataAccessor));

    when(helixZkClient.exists(any())).thenReturn(true);
    when(dataAccessor.keyBuilder()).thenReturn(new org.apache.helix.PropertyKey.Builder(CLUSTER_NAME));
    try {
      zkClient.getInstanceConfig(dataAccessor);
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
    try (MockedStatic<StatusRecorder> statusRecorderMockedStatic = mockStatic(StatusRecorder.class)) {
      when(helixZkClient.exists(any())).thenReturn(true);
      assertEquals(0, zkClient.getSegmentsOfInstance(dataAccessor).size());
    }
    try (MockedStatic<StatusRecorder> statusRecorderMockedStatic = mockStatic(StatusRecorder.class)) {
      when(helixZkClient.exists(any())).thenReturn(false);
      assertEquals(0, zkClient.getSegmentsOfInstance(dataAccessor).size());
    }

    // live instance not null
    LiveInstance liveInstance = spy(new LiveInstance(INSTANCE_ID));
    when(dataAccessor.getProperty(any(PropertyKey.class))).thenReturn(liveInstance);
    when(liveInstance.getEphemeralOwner()).thenReturn(SESSION_ID);

    // no segments
    when(dataAccessor.getChildValues(any(PropertyKey.class))).thenReturn(null);
    assertEquals(0, zkClient.getSegmentsOfInstance(dataAccessor).size());
    // with segments
    CurrentState currentState = spy(new CurrentState(TABLE_NAME));
    currentState.setState(SEGMENT_NAME, "ONLINE");
    currentState.setState(SECOND_SEGMENT_NAME, "ONLINE");
    currentState.setState(THIRD_SEGMENT_NAME, "OFFLINE");
    when(dataAccessor.getChildValues(any(PropertyKey.class), anyBoolean())).thenReturn(List.of(currentState));
    List<SegmentInfo> segmentInfoList = zkClient.getSegmentsOfInstance(dataAccessor);
    assertEquals(2, segmentInfoList.size());
    assertTrue(Arrays.asList(SEGMENT_NAME, SECOND_SEGMENT_NAME).contains(segmentInfoList.get(0).getSegmentName()));
    assertTrue(Arrays.asList(SEGMENT_NAME, SECOND_SEGMENT_NAME).contains(segmentInfoList.get(1).getSegmentName()));
  }

  @Test
  public void testUpdateSegmentMetadata()
      throws Exception {
    List<SegmentInfo> segmentInfoList =
        Arrays.asList(new SegmentInfo(TABLE_NAME, SEGMENT_NAME), new SegmentInfo(TABLE_NAME, SECOND_SEGMENT_NAME));
    Map<String, TableInfo> tableInfoMap = new HashMap<>();
    PinotConfiguration pinotConfiguration = getPinotConfiguration();
    InstanceDataManagerConfig instanceDataManagerConfig = new HelixInstanceDataManagerConfig(pinotConfiguration);
    TableConfig tableConfig = mock(TableConfig.class);

    // table config null
    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(null);
      zkClient.updateSegmentMetadata(segmentInfoList, tableInfoMap, instanceDataManagerConfig);
      assertNull(tableInfoMap.get(TABLE_NAME));
      assertEquals(segmentInfoList.get(0).getCrc(), 0);
      assertEquals(segmentInfoList.get(1).getCrc(), 0);
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
      zkClient.updateSegmentMetadata(segmentInfoList, tableInfoMap, instanceDataManagerConfig);
      assertNotNull(tableInfoMap.get(TABLE_NAME));
      assertEquals(segmentInfoList.get(0).getCrc(), CRC);
      assertEquals(segmentInfoList.get(1).getCrc(), 0);
    }
  }
}
