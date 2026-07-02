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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.apache.pinot.server.predownload.PredownloadTestUtil.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class PredownloadSchedulerTest {
  private PredownloadScheduler _predownloadScheduler;
  private InstanceConfig _instanceConfig;
  private InstanceDataManagerConfig _instanceDataManagerConfig;
  // There will be 3 segments
  // Segment 1 will be downloaded and untarred
  // Segment 2 won't have info on ZK
  // Segment 3 is already downloaded
  private List<PredownloadSegmentInfo> _predownloadSegmentInfoList;
  private PredownloadTableInfo _predownloadTableInfo;
  private TableConfig _tableConfig;
  private File _temporaryFolder;
  private Executor _rawExecutor;

  public void setUp(PropertiesConfiguration properties)
      throws Exception {
    _temporaryFolder = new File(FileUtils.getTempDirectory(), this.getClass().getName());
    FileUtils.deleteQuietly(_temporaryFolder);
    _predownloadScheduler = spy(new PredownloadScheduler(properties));
    _instanceConfig = new InstanceConfig(INSTANCE_ID);
    _instanceDataManagerConfig = mock(InstanceDataManagerConfig.class);
    _instanceConfig.addTag(TAG);
    _predownloadSegmentInfoList =
        Arrays.asList(new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME),
            new PredownloadSegmentInfo(TABLE_NAME, SECOND_SEGMENT_NAME),
            new PredownloadSegmentInfo(TABLE_NAME, THIRD_SEGMENT_NAME));
    _predownloadTableInfo = mock(PredownloadTableInfo.class);
    _tableConfig = mock(TableConfig.class);
    _rawExecutor = _predownloadScheduler._executor;
    // static mock is not working in separate threads, according to
    // https://github.com/mockito/mockito/issues/2142
    _predownloadScheduler._executor = Runnable::run;
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    if (_predownloadScheduler != null) {
      _predownloadScheduler._executor = _rawExecutor;
      _predownloadScheduler.stop();
    }
    if (_temporaryFolder != null && _temporaryFolder.exists()) {
      try {
        FileUtils.deleteDirectory(_temporaryFolder);
        System.out.println("Temporary folder deleted: " + _temporaryFolder.getAbsolutePath());
      } catch (IOException e) {
        System.err.println("Failed to delete temporary folder: " + e.getMessage());
      }
    }
  }

  @Test
  public void testStartSeperately()
      throws Exception {
    String propertiesFilePath = this.getClass().getClassLoader().getResource(SAMPLE_PROPERTIES_FILE_NAME).getPath();
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromPath(propertiesFilePath);
    setUp(properties);
    try (
        MockedConstruction<PredownloadZKClient> zkClientMockedConstruction = mockConstruction(PredownloadZKClient.class,
            (mock, context) -> {
              when(mock.getInstanceConfig(any())).thenReturn(_instanceConfig);
            })) {
      initialize();
      getSegmentsInfo(zkClientMockedConstruction.constructed().get(0));
      loadSegmentsFromLocal();
      downloadSegments();
    }
  }

  @Test
  public void testStartSeperatelyWithStreamingUntar()
      throws Exception {
    String propertiesFilePath = this.getClass().getClassLoader().getResource(SAMPLE_PROPERTIES_FILE_NAME).getPath();
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromPath(propertiesFilePath);
    properties.setProperty("pinot.server.instance.segment.stream.download.untar", true);
    setUp(properties);
    try (
        MockedConstruction<PredownloadZKClient> zkClientMockedConstruction = mockConstruction(PredownloadZKClient.class,
            (mock, context) -> {
              when(mock.getInstanceConfig(any())).thenReturn(_instanceConfig);
            })) {
      initialize();
      getSegmentsInfoWithoutCrypterName(zkClientMockedConstruction.constructed().get(0));
      loadSegmentsFromLocal();
      downloadSegments();
    }
  }

  @Test
  public void testStartTogether()
      throws Exception {
    String propertiesFilePath = this.getClass().getClassLoader().getResource(SAMPLE_PROPERTIES_FILE_NAME).getPath();
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromPath(propertiesFilePath);
    setUp(properties);
    try (
        MockedConstruction<PredownloadZKClient> zkClientMockedConstruction = mockConstruction(PredownloadZKClient.class,
            (mock, context) -> {
              when(mock.getInstanceConfig(any())).thenReturn(_instanceConfig);
            })) {
      doNothing().when(_predownloadScheduler).initializeSegmentFetcher();
      doNothing().when(_predownloadScheduler).getSegmentsInfo();
      doNothing().when(_predownloadScheduler).loadSegmentsFromLocal();
      doReturn(PredownloadCompletionReason.NO_SEGMENT_TO_PREDOWNLOAD).when(_predownloadScheduler).downloadSegments();

      try (MockedStatic<PredownloadStatusRecorder> statusRecorderMockedStatic = mockStatic(
          PredownloadStatusRecorder.class)) {
        _predownloadScheduler.start();
        statusRecorderMockedStatic.verify(
            () -> PredownloadStatusRecorder.predownloadComplete(
                eq(PredownloadCompletionReason.NO_SEGMENT_TO_PREDOWNLOAD),
                anyString(), anyString(), anyString()), times(1));
      }
    }
  }

  public void initialize() {
    _predownloadScheduler.initializeZK();
    _predownloadScheduler.initializeMetricsReporter();
    try (MockedStatic<PinotFSFactory> pinotFSFactoryMockedStatic = mockStatic(PinotFSFactory.class)) {
      _predownloadScheduler.initializeSegmentFetcher();
    }
  }

  public void getSegmentsInfo(PredownloadZKClient predownloadZkClient) {
    // no segments
    try (MockedStatic<PredownloadStatusRecorder> statusRecorderMockedStatic = mockStatic(
        PredownloadStatusRecorder.class)) {
      when(predownloadZkClient.getSegmentsOfInstance(any())).thenReturn(new ArrayList<>());
      _predownloadScheduler.getSegmentsInfo();
      statusRecorderMockedStatic.verify(
          () -> PredownloadStatusRecorder.predownloadComplete(eq(PredownloadCompletionReason.NO_SEGMENT_TO_PREDOWNLOAD),
              anyString(),
              anyString(), anyString()), times(1));
    }

    // with segments
    when(predownloadZkClient.getSegmentsOfInstance(any())).thenReturn(_predownloadSegmentInfoList);
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      // Simulate second one without metadata on ZK
      _predownloadSegmentInfoList.get(0).updateSegmentInfo(createSegmentZKMetadata());
      _predownloadSegmentInfoList.get(2).updateSegmentInfo(createSegmentZKMetadata());
      ((Map) args[1]).put(TABLE_NAME, _predownloadTableInfo);
      return null;
    }).when(predownloadZkClient).updateSegmentMetadata(eq(_predownloadSegmentInfoList), any(), any());
    _predownloadScheduler.getSegmentsInfo();
  }

  public void getSegmentsInfoWithoutCrypterName(PredownloadZKClient predownloadZkClient) {
    // with segments
    when(predownloadZkClient.getSegmentsOfInstance(any())).thenReturn(_predownloadSegmentInfoList);
    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      // Simulate second one without metadata on ZK
      SegmentZKMetadata zkMetadataWithoutCrypterName = createSegmentZKMetadata();
      zkMetadataWithoutCrypterName.setCrypterName(null);
      _predownloadSegmentInfoList.get(0).updateSegmentInfo(zkMetadataWithoutCrypterName);
      _predownloadSegmentInfoList.get(2).updateSegmentInfo(createSegmentZKMetadata());
      ((Map) args[1]).put(TABLE_NAME, _predownloadTableInfo);
      return null;
    }).when(predownloadZkClient).updateSegmentMetadata(eq(_predownloadSegmentInfoList), any(), any());
    _predownloadScheduler.getSegmentsInfo();
  }

  public void loadSegmentsFromLocal()
      throws Exception {
    // Only segment 3 will be loaded — create a real creation.meta with matching CRC
    File seg3Dir = Files.createTempDirectory("predownload-seg3-").toFile();
    File creationMeta = new File(seg3Dir, "creation.meta");
    try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(creationMeta))) {
      dos.writeLong(CRC);
      dos.writeLong(0L);
    }
    when(_predownloadTableInfo.loadSegmentFromLocal(eq(_predownloadSegmentInfoList.get(2)))).thenAnswer(
        invocation -> {
          _predownloadSegmentInfoList.get(2).updateSegmentInfoFromLocal(seg3Dir);
          return true;
        });
    when(_predownloadTableInfo.loadSegmentFromLocal(eq(_predownloadSegmentInfoList.get(0)))).thenReturn(false);
    when(_predownloadTableInfo.loadSegmentFromLocal(eq(_predownloadSegmentInfoList.get(1)))).thenReturn(false);

    try {
      _predownloadScheduler.loadSegmentsFromLocal();
      assertEquals(_predownloadScheduler._failedSegments.size(), 1);
      assertEquals(_predownloadScheduler._failedSegments.iterator().next(),
          _predownloadSegmentInfoList.get(0).getSegmentName());
    } finally {
      FileUtils.deleteQuietly(seg3Dir);
    }
  }

  public void downloadSegments()
      throws Exception {
    File testFolder = new File(_temporaryFolder, "test");
    testFolder.mkdir();
    String dataDir = testFolder.getAbsolutePath();
    int lastIndex = dataDir.lastIndexOf(File.separator);
    when(_predownloadTableInfo.getInstanceDataManagerConfig()).thenReturn(_instanceDataManagerConfig);
    when(_predownloadTableInfo.getTableConfig()).thenReturn(_tableConfig);
    when(_instanceDataManagerConfig.getInstanceDataDir()).thenReturn(dataDir.substring(0, lastIndex));
    when(_tableConfig.getTableName()).thenReturn(dataDir.substring(lastIndex + 1));
    // download failure
    try (MockedStatic<SegmentFetcherFactory> segmentFetcherFactoryMockedStatic = mockStatic(
        SegmentFetcherFactory.class)) {
      segmentFetcherFactoryMockedStatic.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(anyString(), any(), anyString()))
          .then(invocation -> null);
      try (MockedStatic<TarCompressionUtils> tarCompressionUtilsMockedStatic = mockStatic(TarCompressionUtils.class)) {
        PredownloadCompletionReason reason = _predownloadScheduler.downloadSegments();
        assertEquals(reason, PredownloadCompletionReason.SOME_SEGMENTS_DOWNLOAD_FAILED);
      }
    }
    // download success
    try (MockedStatic<SegmentFetcherFactory> segmentFetcherFactoryMockedStatic = mockStatic(
        SegmentFetcherFactory.class)) {
      segmentFetcherFactoryMockedStatic.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(anyString(), any(), anyString()))
          .then(invocation -> null);
      segmentFetcherFactoryMockedStatic.when(
              () -> SegmentFetcherFactory.fetchAndStreamUntarToLocal(anyString(), any(), anyLong(), any()))
          .thenAnswer(invocation -> {
            File untaredFile = new File(testFolder, "streamingUntared");
            if (!untaredFile.exists() && !untaredFile.mkdirs()) {
              throw new IOException("Failed to create directory: " + untaredFile.getAbsolutePath());
            }
            FileUtils.writeByteArrayToFile(new File(untaredFile, "dummy.idx"), new byte[]{1, 2, 3, 4, 5});
            return untaredFile;
          });
      try (MockedStatic<TarCompressionUtils> tarCompressionUtilsMockedStatic = mockStatic(TarCompressionUtils.class)) {
        tarCompressionUtilsMockedStatic.when(() -> TarCompressionUtils.untar(any(File.class), any(File.class)))
            .thenAnswer(invocation -> {
              File untaredFile = new File(testFolder, "untared");
              if (!untaredFile.exists() && !untaredFile.mkdirs()) {
                throw new IOException("Failed to create directory: " + untaredFile.getAbsolutePath());
              }
              FileUtils.writeByteArrayToFile(new File(untaredFile, "dummy.idx"), new byte[]{1, 2, 3, 4, 5});
              return List.of(untaredFile);
            });

        PredownloadCompletionReason reason = _predownloadScheduler.downloadSegments();
        assertEquals(reason, PredownloadCompletionReason.ALL_SEGMENTS_DOWNLOADED);
      }
    }
  }

  @Test
  public void testPredownloadParallelismConfiguration()
      throws Exception {
    // Test default parallelism (should use numProcessors * 3)
    Map<String, Object> defaultProps = Map.of(
        "pinot.server.instance.id", INSTANCE_ID,
        "pinot.server.instance.dataDir", INSTANCE_DATA_DIR,
        "pinot.server.instance.readMode", READ_MODE,
        "pinot.cluster.name", CLUSTER_NAME,
        "pinot.zk.server", ZK_ADDRESS
    );
    PropertiesConfiguration defaultConfig = new PropertiesConfiguration();
    defaultProps.forEach((key, value) -> defaultConfig.setProperty(key, value));

    PredownloadScheduler defaultScheduler = new PredownloadScheduler(defaultConfig);
    ThreadPoolExecutor defaultExecutor = (ThreadPoolExecutor) defaultScheduler._executor;
    int expectedDefaultThreads = Runtime.getRuntime().availableProcessors() * 3;
    assertEquals(defaultExecutor.getCorePoolSize(), expectedDefaultThreads,
        "Default parallelism should be numProcessors * 3");
    defaultScheduler.stop();

    // Test custom parallelism
    int customParallelism = 10;
    Map<String, Object> customProps = Map.of(
        "pinot.server.instance.id", INSTANCE_ID,
        "pinot.server.instance.dataDir", INSTANCE_DATA_DIR,
        "pinot.server.instance.readMode", READ_MODE,
        "pinot.cluster.name", CLUSTER_NAME,
        "pinot.zk.server", ZK_ADDRESS,
        "pinot.server.predownload.parallelism", String.valueOf(customParallelism)
    );
    PropertiesConfiguration customConfig = new PropertiesConfiguration();
    customProps.forEach((key, value) -> customConfig.setProperty(key, value));

    PredownloadScheduler customScheduler = new PredownloadScheduler(customConfig);
    ThreadPoolExecutor customExecutor = (ThreadPoolExecutor) customScheduler._executor;
    assertEquals(customExecutor.getCorePoolSize(), customParallelism,
        "Custom parallelism should match configured value");
    customScheduler.stop();

    // Test zero/negative parallelism (should fall back to default)
    Map<String, Object> zeroProps = Map.of(
        "pinot.server.instance.id", INSTANCE_ID,
        "pinot.server.instance.dataDir", INSTANCE_DATA_DIR,
        "pinot.server.instance.readMode", READ_MODE,
        "pinot.cluster.name", CLUSTER_NAME,
        "pinot.zk.server", ZK_ADDRESS,
        "pinot.server.predownload.parallelism", "0"
    );
    PropertiesConfiguration zeroConfig = new PropertiesConfiguration();
    zeroProps.forEach((key, value) -> zeroConfig.setProperty(key, value));

    PredownloadScheduler zeroScheduler = new PredownloadScheduler(zeroConfig);
    ThreadPoolExecutor zeroExecutor = (ThreadPoolExecutor) zeroScheduler._executor;
    assertEquals(zeroExecutor.getCorePoolSize(), expectedDefaultThreads,
        "Zero parallelism should fall back to default");
    zeroScheduler.stop();
  }

  // ── Peer download tests ────────────────────────────────────────────────────

  private PredownloadScheduler buildPeerEnabledScheduler(PropertiesConfiguration properties)
      throws Exception {
    properties.setProperty("pinot.server.peer.download.enabled", true);
    PredownloadScheduler scheduler = spy(new PredownloadScheduler(properties));
    scheduler._executor = Runnable::run;

    Field schemeField = PredownloadScheduler.class.getDeclaredField("_peerDownloadScheme");
    schemeField.setAccessible(true);
    schemeField.set(scheduler, "http");

    Field metricsField = PredownloadScheduler.class.getDeclaredField("_predownloadMetrics");
    metricsField.setAccessible(true);
    metricsField.set(scheduler, mock(PredownloadMetrics.class));

    return scheduler;
  }

  private void injectMockZkClient(PredownloadScheduler scheduler, PredownloadZKClient mockZkClient)
      throws Exception {
    Field zkField = PredownloadScheduler.class.getDeclaredField("_predownloadZkClient");
    zkField.setAccessible(true);
    zkField.set(scheduler, mockZkClient);
  }

  private void injectSegmentState(PredownloadScheduler scheduler,
      List<PredownloadSegmentInfo> segments, Map<String, PredownloadTableInfo> tableInfoMap)
      throws Exception {
    Field segField = PredownloadScheduler.class.getDeclaredField("_predownloadSegmentInfoList");
    segField.setAccessible(true);
    segField.set(scheduler, segments);

    Field mapField = PredownloadScheduler.class.getDeclaredField("_tableInfoMap");
    mapField.setAccessible(true);
    mapField.set(scheduler, tableInfoMap);
  }

  @Test
  public void testPeerDownloadEnabledViaConstructor()
      throws Exception {
    String propertiesFilePath =
        this.getClass().getClassLoader().getResource(SAMPLE_PROPERTIES_FILE_NAME).getPath();
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromPath(propertiesFilePath);

    Field enabledField = PredownloadScheduler.class.getDeclaredField("_peerDownloadEnabled");
    enabledField.setAccessible(true);

    PredownloadScheduler disabled = new PredownloadScheduler(properties);
    assertEquals(enabledField.get(disabled), false);
    disabled.stop();

    properties.setProperty("pinot.server.peer.download.enabled", true);
    PredownloadScheduler enabled = new PredownloadScheduler(properties);
    assertEquals(enabledField.get(enabled), true);
    enabled.stop();
  }

  @Test
  public void testDownloadSegmentFallbackToPeerOnDeepStoreFailure()
      throws Exception {
    String propertiesFilePath =
        this.getClass().getClassLoader().getResource(SAMPLE_PROPERTIES_FILE_NAME).getPath();
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromPath(propertiesFilePath);
    setUp(properties);

    PredownloadScheduler scheduler = buildPeerEnabledScheduler(properties);
    PredownloadZKClient mockZkClient = mock(PredownloadZKClient.class);
    when(mockZkClient.getPeerServerURIs(any(), anyString(), anyString(), anyString())).thenReturn(new ArrayList<>());
    injectMockZkClient(scheduler, mockZkClient);

    PredownloadSegmentInfo segment = new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME);
    segment.updateSegmentInfo(createSegmentZKMetadata());
    scheduler._failedSegments.add(SEGMENT_NAME);

    File testFolder = new File(_temporaryFolder, "peerFallbackTest");
    testFolder.mkdirs();
    String dataDir = testFolder.getAbsolutePath();
    int lastIndex = dataDir.lastIndexOf(File.separator);
    when(_predownloadTableInfo.getInstanceDataManagerConfig()).thenReturn(_instanceDataManagerConfig);
    when(_predownloadTableInfo.getTableConfig()).thenReturn(_tableConfig);
    when(_instanceDataManagerConfig.getInstanceDataDir()).thenReturn(dataDir.substring(0, lastIndex));
    when(_tableConfig.getTableName()).thenReturn(dataDir.substring(lastIndex + 1));
    when(_predownloadTableInfo.loadSegmentFromLocal(any())).thenReturn(false);
    injectSegmentState(scheduler, List.of(segment), Map.of(TABLE_NAME, _predownloadTableInfo));

    try (MockedStatic<SegmentFetcherFactory> sfMock = mockStatic(SegmentFetcherFactory.class)) {
      // 3-arg (deep store) throws — simulating exhausted deep store retries
      sfMock.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(anyString(), any(File.class), anyString()))
          .thenThrow(new AttemptsExceededException("deep store failed", 3));
      // 5-arg (peer) succeeds — supplier lambda is not invoked by mock, so no ZK call needed
      sfMock.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(
                  anyString(), anyString(), any(), any(File.class), anyString()))
          .thenAnswer(inv -> null);

      try (MockedStatic<TarCompressionUtils> tarMock = mockStatic(TarCompressionUtils.class)) {
        tarMock.when(() -> TarCompressionUtils.untar(any(File.class), any(File.class)))
            .thenAnswer(inv -> {
              File untarDir = new File(testFolder, "untared_peer");
              untarDir.mkdirs();
              return List.of(untarDir);
            });
        scheduler.downloadSegment(segment);
      }
    }

    assertFalse(scheduler._failedSegments.contains(SEGMENT_NAME),
        "Segment should be removed from failed set after successful peer download");
    scheduler._executor = null; // lambda can't be cast to ThreadPoolExecutor
    scheduler.stop();
  }

  @Test
  public void testDownloadSegmentNoPeerFallbackWhenPeerDisabled()
      throws Exception {
    String propertiesFilePath =
        this.getClass().getClassLoader().getResource(SAMPLE_PROPERTIES_FILE_NAME).getPath();
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromPath(propertiesFilePath);
    setUp(properties);

    // peerDownloadEnabled=false (default from properties) — no fallback to peer
    PredownloadScheduler scheduler = spy(new PredownloadScheduler(properties));
    scheduler._executor = Runnable::run;
    Field metricsField = PredownloadScheduler.class.getDeclaredField("_predownloadMetrics");
    metricsField.setAccessible(true);
    metricsField.set(scheduler, mock(PredownloadMetrics.class));

    PredownloadSegmentInfo segment = new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME);
    segment.updateSegmentInfo(createSegmentZKMetadata());

    File testFolder = new File(_temporaryFolder, "noPeerTest");
    testFolder.mkdirs();
    String dataDir = testFolder.getAbsolutePath();
    int lastIndex = dataDir.lastIndexOf(File.separator);
    when(_predownloadTableInfo.getInstanceDataManagerConfig()).thenReturn(_instanceDataManagerConfig);
    when(_predownloadTableInfo.getTableConfig()).thenReturn(_tableConfig);
    when(_instanceDataManagerConfig.getInstanceDataDir()).thenReturn(dataDir.substring(0, lastIndex));
    when(_tableConfig.getTableName()).thenReturn(dataDir.substring(lastIndex + 1));
    injectSegmentState(scheduler, List.of(segment), Map.of(TABLE_NAME, _predownloadTableInfo));

    try (MockedStatic<SegmentFetcherFactory> sfMock = mockStatic(SegmentFetcherFactory.class)) {
      sfMock.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(anyString(), any(File.class), anyString()))
          .thenThrow(new AttemptsExceededException("deep store failed", 3));

      try (MockedStatic<TarCompressionUtils> tarMock = mockStatic(TarCompressionUtils.class)) {
        assertThrows(AttemptsExceededException.class, () -> scheduler.downloadSegment(segment));
      }
    }

    assertTrue(scheduler._failedSegments.contains(SEGMENT_NAME),
        "Segment should remain in failed set when peer download is disabled");
    scheduler._executor = null;
    scheduler.stop();
  }

  @Test
  public void testDownloadSegmentBothDeepStoreAndPeerFail()
      throws Exception {
    String propertiesFilePath =
        this.getClass().getClassLoader().getResource(SAMPLE_PROPERTIES_FILE_NAME).getPath();
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromPath(propertiesFilePath);
    setUp(properties);

    PredownloadScheduler scheduler = buildPeerEnabledScheduler(properties);
    PredownloadZKClient mockZkClient = mock(PredownloadZKClient.class);
    when(mockZkClient.getPeerServerURIs(any(), anyString(), anyString(), anyString())).thenReturn(new ArrayList<>());
    injectMockZkClient(scheduler, mockZkClient);

    PredownloadSegmentInfo segment = new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME);
    segment.updateSegmentInfo(createSegmentZKMetadata());

    File testFolder = new File(_temporaryFolder, "bothFailTest");
    testFolder.mkdirs();
    String dataDir = testFolder.getAbsolutePath();
    int lastIndex = dataDir.lastIndexOf(File.separator);
    when(_predownloadTableInfo.getInstanceDataManagerConfig()).thenReturn(_instanceDataManagerConfig);
    when(_predownloadTableInfo.getTableConfig()).thenReturn(_tableConfig);
    when(_instanceDataManagerConfig.getInstanceDataDir()).thenReturn(dataDir.substring(0, lastIndex));
    when(_tableConfig.getTableName()).thenReturn(dataDir.substring(lastIndex + 1));
    injectSegmentState(scheduler, List.of(segment), Map.of(TABLE_NAME, _predownloadTableInfo));

    try (MockedStatic<SegmentFetcherFactory> sfMock = mockStatic(SegmentFetcherFactory.class)) {
      sfMock.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(anyString(), any(File.class), anyString()))
          .thenThrow(new AttemptsExceededException("deep store failed", 3));
      sfMock.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(
                  anyString(), anyString(), any(), any(File.class), anyString()))
          .thenThrow(new AttemptsExceededException("peer download failed", 3));

      try (MockedStatic<TarCompressionUtils> tarMock = mockStatic(TarCompressionUtils.class)) {
        assertThrows(AttemptsExceededException.class, () -> scheduler.downloadSegment(segment));
      }
    }

    assertTrue(scheduler._failedSegments.contains(SEGMENT_NAME),
        "Segment should be in failed set when both deep store and peer download fail");
    scheduler._executor = null;
    scheduler.stop();
  }

  @Test
  public void testDeepStoreSuccessEmitsDeepStoreMetric()
      throws Exception {
    String propertiesFilePath =
        this.getClass().getClassLoader().getResource(SAMPLE_PROPERTIES_FILE_NAME).getPath();
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromPath(propertiesFilePath);
    setUp(properties);

    PredownloadScheduler scheduler = buildPeerEnabledScheduler(properties);

    PredownloadSegmentInfo segment = new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME);
    segment.updateSegmentInfo(createSegmentZKMetadata());

    File testFolder = new File(_temporaryFolder, "deepStoreMetricTest");
    testFolder.mkdirs();
    String dataDir = testFolder.getAbsolutePath();
    int lastIndex = dataDir.lastIndexOf(File.separator);
    when(_predownloadTableInfo.getInstanceDataManagerConfig()).thenReturn(_instanceDataManagerConfig);
    when(_predownloadTableInfo.getTableConfig()).thenReturn(_tableConfig);
    when(_instanceDataManagerConfig.getInstanceDataDir()).thenReturn(dataDir.substring(0, lastIndex));
    when(_tableConfig.getTableName()).thenReturn(dataDir.substring(lastIndex + 1));
    when(_predownloadTableInfo.loadSegmentFromLocal(any())).thenReturn(false);
    injectSegmentState(scheduler, List.of(segment), Map.of(TABLE_NAME, _predownloadTableInfo));

    PredownloadMetrics mockMetrics = getMockMetrics(scheduler);

    try (MockedStatic<SegmentFetcherFactory> sfMock = mockStatic(SegmentFetcherFactory.class)) {
      sfMock.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(anyString(), any(File.class), anyString()))
          .thenAnswer(inv -> null);
      try (MockedStatic<TarCompressionUtils> tarMock = mockStatic(TarCompressionUtils.class)) {
        tarMock.when(() -> TarCompressionUtils.untar(any(File.class), any(File.class)))
            .thenAnswer(inv -> {
              File untarDir = new File(testFolder, "untared");
              untarDir.mkdirs();
              return List.of(untarDir);
            });
        scheduler.downloadSegment(segment);
      }
    }

    verify(mockMetrics).deepStoreSegmentDownloaded();
    verify(mockMetrics).segmentDownloaded(eq(true), eq(SEGMENT_NAME), anyLong(), anyLong());
    verify(mockMetrics, never()).peerSegmentDownloaded(anyBoolean(), anyString(), anyLong(), anyLong());
    scheduler._executor = null;
    scheduler.stop();
  }

  @Test
  public void testPeerDownloadSuccessEmitsPeerMetrics()
      throws Exception {
    String propertiesFilePath =
        this.getClass().getClassLoader().getResource(SAMPLE_PROPERTIES_FILE_NAME).getPath();
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromPath(propertiesFilePath);
    setUp(properties);

    PredownloadScheduler scheduler = buildPeerEnabledScheduler(properties);
    PredownloadZKClient mockZkClient = mock(PredownloadZKClient.class);
    when(mockZkClient.getPeerServerURIs(any(), anyString(), anyString(), anyString())).thenReturn(new ArrayList<>());
    injectMockZkClient(scheduler, mockZkClient);

    PredownloadSegmentInfo segment = new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME);
    segment.updateSegmentInfo(createSegmentZKMetadata());

    File testFolder = new File(_temporaryFolder, "peerMetricTest");
    testFolder.mkdirs();
    String dataDir = testFolder.getAbsolutePath();
    int lastIndex = dataDir.lastIndexOf(File.separator);
    when(_predownloadTableInfo.getInstanceDataManagerConfig()).thenReturn(_instanceDataManagerConfig);
    when(_predownloadTableInfo.getTableConfig()).thenReturn(_tableConfig);
    when(_instanceDataManagerConfig.getInstanceDataDir()).thenReturn(dataDir.substring(0, lastIndex));
    when(_tableConfig.getTableName()).thenReturn(dataDir.substring(lastIndex + 1));
    when(_predownloadTableInfo.loadSegmentFromLocal(any())).thenReturn(false);
    injectSegmentState(scheduler, List.of(segment), Map.of(TABLE_NAME, _predownloadTableInfo));

    PredownloadMetrics mockMetrics = getMockMetrics(scheduler);

    try (MockedStatic<SegmentFetcherFactory> sfMock = mockStatic(SegmentFetcherFactory.class)) {
      sfMock.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(anyString(), any(File.class), anyString()))
          .thenThrow(new AttemptsExceededException("deep store failed", 3));
      sfMock.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(
                  anyString(), anyString(), any(), any(File.class), anyString()))
          .thenAnswer(inv -> null);

      try (MockedStatic<TarCompressionUtils> tarMock = mockStatic(TarCompressionUtils.class)) {
        tarMock.when(() -> TarCompressionUtils.untar(any(File.class), any(File.class)))
            .thenAnswer(inv -> {
              File untarDir = new File(testFolder, "untared_peer");
              untarDir.mkdirs();
              return List.of(untarDir);
            });
        scheduler.downloadSegment(segment);
      }
    }

    verify(mockMetrics, never()).deepStoreSegmentDownloaded();
    verify(mockMetrics).peerSegmentDownloaded(eq(true), eq(SEGMENT_NAME), anyLong(), anyLong());
    verify(mockMetrics).segmentDownloaded(eq(true), eq(SEGMENT_NAME), anyLong(), anyLong());
    scheduler._executor = null;
    scheduler.stop();
  }

  @Test
  public void testPeerDownloadFailureEmitsSegmentLevelMetric()
      throws Exception {
    String propertiesFilePath =
        this.getClass().getClassLoader().getResource(SAMPLE_PROPERTIES_FILE_NAME).getPath();
    PropertiesConfiguration properties = CommonsConfigurationUtils.fromPath(propertiesFilePath);
    setUp(properties);

    PredownloadScheduler scheduler = buildPeerEnabledScheduler(properties);
    PredownloadZKClient mockZkClient = mock(PredownloadZKClient.class);
    when(mockZkClient.getPeerServerURIs(any(), anyString(), anyString(), anyString())).thenReturn(new ArrayList<>());
    injectMockZkClient(scheduler, mockZkClient);

    PredownloadSegmentInfo segment = new PredownloadSegmentInfo(TABLE_NAME, SEGMENT_NAME);
    segment.updateSegmentInfo(createSegmentZKMetadata());

    File testFolder = new File(_temporaryFolder, "peerFailMetricTest");
    testFolder.mkdirs();
    String dataDir = testFolder.getAbsolutePath();
    int lastIndex = dataDir.lastIndexOf(File.separator);
    when(_predownloadTableInfo.getInstanceDataManagerConfig()).thenReturn(_instanceDataManagerConfig);
    when(_predownloadTableInfo.getTableConfig()).thenReturn(_tableConfig);
    when(_instanceDataManagerConfig.getInstanceDataDir()).thenReturn(dataDir.substring(0, lastIndex));
    when(_tableConfig.getTableName()).thenReturn(dataDir.substring(lastIndex + 1));
    injectSegmentState(scheduler, List.of(segment), Map.of(TABLE_NAME, _predownloadTableInfo));

    PredownloadMetrics mockMetrics = getMockMetrics(scheduler);

    try (MockedStatic<SegmentFetcherFactory> sfMock = mockStatic(SegmentFetcherFactory.class)) {
      sfMock.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(anyString(), any(File.class), anyString()))
          .thenThrow(new AttemptsExceededException("deep store failed", 3));
      sfMock.when(
              () -> SegmentFetcherFactory.fetchAndDecryptSegmentToLocal(
                  anyString(), anyString(), any(), any(File.class), anyString()))
          .thenThrow(new AttemptsExceededException("peer download failed", 3));

      try (MockedStatic<TarCompressionUtils> tarMock = mockStatic(TarCompressionUtils.class)) {
        assertThrows(AttemptsExceededException.class, () -> scheduler.downloadSegment(segment));
      }
    }

    verify(mockMetrics, never()).deepStoreSegmentDownloaded();
    verify(mockMetrics).peerSegmentDownloaded(eq(false), eq(SEGMENT_NAME), eq(0L), eq(0L));
    verify(mockMetrics).segmentDownloaded(eq(false), eq(SEGMENT_NAME), eq(0L), eq(0L));
    scheduler._executor = null;
    scheduler.stop();
  }

  private PredownloadMetrics getMockMetrics(PredownloadScheduler scheduler)
      throws Exception {
    Field metricsField = PredownloadScheduler.class.getDeclaredField("_predownloadMetrics");
    metricsField.setAccessible(true);
    return (PredownloadMetrics) metricsField.get(scheduler);
  }
}
