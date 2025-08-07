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
package org.apache.pinot.controller.helix.core.rebalance;

import java.util.Collections;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixManager;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.StreamIngestionConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class DataLossRiskAssessorTest extends ControllerTest {
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final int NUM_REPLICAS = 3;

  @BeforeClass
  public void setUp()
      throws Exception {
    startZk();
    startController(getDefaultControllerConfiguration());
    addFakeBrokerInstancesToAutoJoinHelixCluster(1, true);
  }

  @AfterClass
  public void tearDown() {
    stopFakeInstances();
    stopController();
    stopZk();
  }

  @Test
  public void testDataLossRiskAssessorPeerDownloadDisabled() {
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();
    assertThrows(IllegalStateException.class,
        () -> new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig,
            _helixManager, _helixResourceManager.getRealtimeSegmentManager()));

    assertThrows(IllegalStateException.class,
        () -> new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig,
            _helixManager, _helixResourceManager.getRealtimeSegmentManager()));

    TableRebalancer.NoOpRiskAssessor noOpRiskAssessor = new TableRebalancer.NoOpRiskAssessor();
    Pair<Boolean, String> dataLossRiskResult = noOpRiskAssessor.assessDataLossRisk("randomSegmentName");
    assertFalse(dataLossRiskResult.getLeft());
    assertNull(dataLossRiskResult.getRight());
  }

  @Test
  public void testDataLossRiskAssessorPeerDownloadEnabledCompletedSegment() {
    String segmentName = "randomSegmentName";
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();
    tableConfig.getValidationConfig().setPeerSegmentDownloadScheme("http");

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment with non-empty download URL
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      segmentZKMetadata.setDownloadUrl("nonEmptyDownloadURL");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertFalse(dataLossRiskResult.getLeft());
      assertNull(dataLossRiskResult.getRight());
    }

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment with empty download URL
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertTrue(dataLossRiskResult.getLeft());
      assertTrue(dataLossRiskResult.getRight().contains("as the download URL is empty"));
    }

    // Enable dedup on the table, this should return the same results with it disabled
    DedupConfig dedupConfig = new DedupConfig();
    dedupConfig.setDedupEnabled(true);
    tableConfig.setDedupConfig(dedupConfig);

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment with non-empty download URL
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      segmentZKMetadata.setDownloadUrl("nonEmptyDownloadURL");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertFalse(dataLossRiskResult.getLeft());
      assertNull(dataLossRiskResult.getRight());
    }

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment with empty download URL
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertTrue(dataLossRiskResult.getLeft());
      assertTrue(dataLossRiskResult.getRight().contains("as the download URL is empty"));
    }

    // Enable upsert in PARTIAL mode on the table, this should return the same results with it disabled
    tableConfig.setDedupConfig(null);
    UpsertConfig upsertConfig = new UpsertConfig();
    upsertConfig.setMode(UpsertConfig.Mode.PARTIAL);
    tableConfig.setUpsertConfig(upsertConfig);

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment with non-empty download URL
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      segmentZKMetadata.setDownloadUrl("nonEmptyDownloadURL");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertFalse(dataLossRiskResult.getLeft());
      assertNull(dataLossRiskResult.getRight());
    }

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment with empty download URL
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertTrue(dataLossRiskResult.getLeft());
      assertTrue(dataLossRiskResult.getRight().contains("as the download URL is empty"));
    }

    // Enable pauseless, disable upsert and dedup, results should be the same as without pauseless enabled
    tableConfig.setDedupConfig(null);
    tableConfig.setUpsertConfig(null);
    IngestionConfig ingestionConfig = new IngestionConfig();
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(
        Collections.singletonList(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()));
    streamIngestionConfig.setPauselessConsumptionEnabled(true);
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    tableConfig.setIngestionConfig(ingestionConfig);

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment with non-empty download URL
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      segmentZKMetadata.setDownloadUrl("nonEmptyDownloadURL");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertFalse(dataLossRiskResult.getLeft());
      assertNull(dataLossRiskResult.getRight());
    }

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment with empty download URL
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.DONE);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertTrue(dataLossRiskResult.getLeft());
      assertTrue(dataLossRiskResult.getRight().contains("as the download URL is empty"));
    }
  }

  @Test
  public void testDataLossRiskAssessorPeerDownloadEnabledConsumingSegment() {
    String segmentName = "randomSegmentName";
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();
    tableConfig.getValidationConfig().setPeerSegmentDownloadScheme("http");

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create IN_PROGRESS segment with empty download URL. Download URL should not exist for segment that's not yet
      // completed
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertFalse(dataLossRiskResult.getLeft());
      assertNull(dataLossRiskResult.getRight());
    }

    // Enable dedup on the table, this should return the same results with it disabled
    DedupConfig dedupConfig = new DedupConfig();
    dedupConfig.setDedupEnabled(true);
    tableConfig.setDedupConfig(dedupConfig);

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create IN_PROGRESS segment with empty download URL. Download URL should not exist for segment that's not yet
      // completed
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertFalse(dataLossRiskResult.getLeft());
      assertNull(dataLossRiskResult.getRight());
    }

    // Enable upsert in PARTIAL mode on the table, this should return the same results with it disabled
    tableConfig.setDedupConfig(null);
    UpsertConfig upsertConfig = new UpsertConfig();
    upsertConfig.setMode(UpsertConfig.Mode.PARTIAL);
    tableConfig.setUpsertConfig(upsertConfig);

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create IN_PROGRESS segment with empty download URL. Download URL should not exist for segment that's not yet
      // completed
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertFalse(dataLossRiskResult.getLeft());
      assertNull(dataLossRiskResult.getRight());
    }

    // Enable pauseless, disable dedup and upsert, this should return the same results as with it disabled
    tableConfig.setDedupConfig(null);
    tableConfig.setUpsertConfig(null);
    IngestionConfig ingestionConfig = new IngestionConfig();
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(
        Collections.singletonList(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()));
    streamIngestionConfig.setPauselessConsumptionEnabled(true);
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    tableConfig.setIngestionConfig(ingestionConfig);

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create IN_PROGRESS segment with empty download URL. Download URL should not exist for segment that's not yet
      // completed
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.IN_PROGRESS);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertFalse(dataLossRiskResult.getLeft());
      assertNull(dataLossRiskResult.getRight());
    }
  }

  @Test
  public void testDataLossRiskAssessorPeerDownloadEnabledPauselessEnabledCommittingSegment() {
    String segmentName = "randomSegmentName";
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setNumReplicas(NUM_REPLICAS).build();
    tableConfig.getValidationConfig().setPeerSegmentDownloadScheme("https");

    // Enable pauseless, this by itself (without dedup / upsert) should return false
    // No need to test non-pauseless tables with COMMITTING state as this is only applicable to pauseless tables
    IngestionConfig ingestionConfig = new IngestionConfig();
    StreamIngestionConfig streamIngestionConfig = new StreamIngestionConfig(
        Collections.singletonList(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap()));
    streamIngestionConfig.setPauselessConsumptionEnabled(true);
    ingestionConfig.setStreamIngestionConfig(streamIngestionConfig);
    tableConfig.setIngestionConfig(ingestionConfig);

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment in COMMITTING state
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.COMMITTING);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertFalse(dataLossRiskResult.getLeft());
      assertNull(dataLossRiskResult.getRight());
    }

    // Enable dedup on the table, this should return true
    DedupConfig dedupConfig = new DedupConfig();
    dedupConfig.setDedupEnabled(true);
    tableConfig.setDedupConfig(dedupConfig);

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment with empty download URL
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.COMMITTING);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertTrue(dataLossRiskResult.getLeft());
      assertTrue(dataLossRiskResult.getRight().contains("as it is in COMMITING state"));
    }

    // Enable upsert on the table in PARTIAL mode, this should return true
    tableConfig.setDedupConfig(null);
    UpsertConfig upsertConfig = new UpsertConfig();
    upsertConfig.setMode(UpsertConfig.Mode.PARTIAL);
    tableConfig.setUpsertConfig(upsertConfig);

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment with empty download URL
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.COMMITTING);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertTrue(dataLossRiskResult.getLeft());
      assertTrue(dataLossRiskResult.getRight().contains("as it is in COMMITING state"));
    }

    // Enable upsert on the table in FULL mode, this should return false
    upsertConfig = new UpsertConfig();
    upsertConfig.setMode(UpsertConfig.Mode.FULL);
    tableConfig.setUpsertConfig(upsertConfig);

    try (MockedStatic<ZKMetadataProvider> zkMetadataProviderMockedStatic = mockStatic(ZKMetadataProvider.class)) {
      // Create segment with empty download URL
      SegmentZKMetadata segmentZKMetadata = new SegmentZKMetadata(segmentName);
      segmentZKMetadata.setStatus(CommonConstants.Segment.Realtime.Status.COMMITTING);
      segmentZKMetadata.setDownloadUrl("");

      zkMetadataProviderMockedStatic.when(() -> ZKMetadataProvider.getTableConfig(any(), anyString())).thenReturn(null);
      zkMetadataProviderMockedStatic.when(
          () -> ZKMetadataProvider.getSegmentZKMetadata(any(), anyString(), anyString())).thenReturn(segmentZKMetadata);
      ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
      HelixManager helixManager = mock(HelixManager.class);
      when(helixManager.getHelixPropertyStore()).thenReturn(propertyStore);

      TableRebalancer.PeerDownloadTableDataLossRiskAssessor dataLossRiskAssessor =
          new TableRebalancer.PeerDownloadTableDataLossRiskAssessor(REALTIME_TABLE_NAME, tableConfig, helixManager,
              _helixResourceManager.getRealtimeSegmentManager());
      Pair<Boolean, String> dataLossRiskResult = dataLossRiskAssessor.assessDataLossRisk(segmentName);
      assertFalse(dataLossRiskResult.getLeft());
      assertNull(dataLossRiskResult.getRight());
    }
  }
}
