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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.instance.InstanceDataManagerConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static org.apache.pinot.server.predownload.PredownloadTestUtil.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;


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

  public void loadSegmentsFromLocal() {
    // Only segment 3 will be loaded
    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getDiskSizeBytes()).thenReturn(DISK_SIZE_BYTES);
    when(segmentMetadata.getCrc()).thenReturn(String.valueOf(CRC));
    when(_predownloadTableInfo.loadSegmentFromLocal(eq(_predownloadSegmentInfoList.get(2)))).thenAnswer(
        invocation -> {
          _predownloadSegmentInfoList.get(2).updateSegmentInfoFromLocal(segmentDirectory);
          return true;
        });
    when(_predownloadTableInfo.loadSegmentFromLocal(eq(_predownloadSegmentInfoList.get(0)))).thenReturn(false);
    when(_predownloadTableInfo.loadSegmentFromLocal(eq(_predownloadSegmentInfoList.get(1)))).thenReturn(false);

    _predownloadScheduler.loadSegmentsFromLocal();
    assertEquals(1, _predownloadScheduler._failedSegments.size());
    assertEquals(_predownloadSegmentInfoList.get(0).getSegmentName(),
        _predownloadScheduler._failedSegments.iterator().next());
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
        assertEquals(PredownloadCompletionReason.SOME_SEGMENTS_DOWNLOAD_FAILED, reason);
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
            return untaredFile;
          });
      try (MockedStatic<TarCompressionUtils> tarCompressionUtilsMockedStatic = mockStatic(TarCompressionUtils.class)) {
        tarCompressionUtilsMockedStatic.when(() -> TarCompressionUtils.untar(any(File.class), any(File.class)))
            .thenAnswer(invocation -> {
              File untaredFile = new File(testFolder, "untared");
              if (!untaredFile.exists() && !untaredFile.mkdirs()) {
                throw new IOException("Failed to create directory: " + untaredFile.getAbsolutePath());
              }
              return List.of(untaredFile);
            });

        PredownloadCompletionReason reason = _predownloadScheduler.downloadSegments();
        assertEquals(PredownloadCompletionReason.ALL_SEGMENTS_DOWNLOADED, reason);
      }
    }
  }
}
