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
package org.apache.pinot.core.data.manager.realtime;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.protocols.SegmentCompletionProtocol;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.core.metadata.DefaultMetadataExtractor;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.server.realtime.ServerSegmentCompletionProtocolHandler;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;


public class SplitSegmentCommitterTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplitSegmentCommitterTest.class);
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SplitSegmentCommitterTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension("col1", DataType.STRING)
          .build();

  private LLCSegmentName _llcSegmentName;
  private String _segmentName;
  private File _indexDir;
  private File _segmentTarFile;
  private Map<String, File> _metadataFiles;
  private ServerMetrics _serverMetrics;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    FileUtils.forceMkdir(TEMP_DIR);
    _llcSegmentName = new LLCSegmentName(RAW_TABLE_NAME, 0, 0, System.currentTimeMillis());
    _segmentName = _llcSegmentName.getSegmentName();

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(_segmentName);
    List<GenericRow> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      GenericRow row = new GenericRow();
      row.putValue("col1", "v" + i);
      rows.add(row);
    }
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    _indexDir = new File(TEMP_DIR, _segmentName);

    _segmentTarFile = new File(TEMP_DIR, _segmentName + ".tar.gz");
    TarCompressionUtils.createCompressedTarFile(_indexDir, _segmentTarFile);
    _metadataFiles = new HashMap<>();
    _metadataFiles.put(V1Constants.MetadataKeys.METADATA_FILE_NAME, SegmentDirectoryPaths.findMetadataFile(_indexDir));
    _metadataFiles.put(V1Constants.SEGMENT_CREATION_META, SegmentDirectoryPaths.findCreationMetaFile(_indexDir));
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @BeforeMethod
  public void resetServerMetrics() {
    _serverMetrics = Mockito.mock(ServerMetrics.class);
  }

  private ServerSegmentCompletionProtocolHandler mockProtocolHandler() {
    ServerSegmentCompletionProtocolHandler handler = Mockito.mock(ServerSegmentCompletionProtocolHandler.class);
    Mockito.when(handler.segmentCommitStart(any())).thenReturn(SegmentCompletionProtocol.RESP_COMMIT_CONTINUE);
    Mockito.when(handler.segmentCommitEndWithMetadata(any(), any()))
        .thenReturn(SegmentCompletionProtocol.RESP_COMMIT_SUCCESS);
    return handler;
  }

  private RealtimeSegmentDataManager.SegmentBuildDescriptor mockBuildDescriptor() {
    RealtimeSegmentDataManager.SegmentBuildDescriptor descriptor =
        Mockito.mock(RealtimeSegmentDataManager.SegmentBuildDescriptor.class);
    Mockito.when(descriptor.getSegmentTarFile()).thenReturn(_segmentTarFile);
    Mockito.when(descriptor.getMetadataFiles()).thenReturn(_metadataFiles);
    return descriptor;
  }

  private SplitSegmentCommitter newCommitter(ServerSegmentCompletionProtocolHandler handler, SegmentUploader uploader,
      String peerDownloadScheme) {
    return newCommitter(handler, uploader, peerDownloadScheme, true);
  }

  private SplitSegmentCommitter newCommitter(ServerSegmentCompletionProtocolHandler handler, SegmentUploader uploader,
      String peerDownloadScheme, boolean uploadMetadataTar) {
    SegmentCompletionProtocol.Request.Params params =
        new SegmentCompletionProtocol.Request.Params().withSegmentName(_segmentName);
    return new SplitSegmentCommitter(LOGGER, handler, params, uploader, peerDownloadScheme, uploadMetadataTar,
        _serverMetrics);
  }

  private SegmentUploader mockUploader(String segmentLocation)
      throws Exception {
    SegmentUploader uploader = Mockito.mock(SegmentUploader.class);
    Mockito.when(uploader.isMetadataTarUploadSupported()).thenReturn(true);
    Mockito.when(uploader.uploadSegment(any(), any(LLCSegmentName.class)))
        .thenReturn(segmentLocation == null ? null : new URI(segmentLocation));
    return uploader;
  }

  private SegmentCompletionProtocol.Request.Params commitAndCaptureParams(SegmentUploader uploader,
      boolean uploadMetadataTar)
      throws Exception {
    ServerSegmentCompletionProtocolHandler handler = mockProtocolHandler();
    newCommitter(handler, uploader, null, uploadMetadataTar).commit(mockBuildDescriptor());
    ArgumentCaptor<SegmentCompletionProtocol.Request.Params> captor =
        ArgumentCaptor.forClass(SegmentCompletionProtocol.Request.Params.class);
    Mockito.verify(handler).segmentCommitEndWithMetadata(captor.capture(), any());
    return captor.getValue();
  }

  @Test
  public void testCommitEndCarriesMetadataTarLocationOnUploadSuccess()
      throws Exception {
    SegmentUploader uploader = mockUploader("hdfs://root/" + RAW_TABLE_NAME + "/" + _segmentName);
    URI tmpLocation =
        new URI("hdfs://root/" + RAW_TABLE_NAME + "/" + _segmentName + ".metadata.tar.gz.tmp.123");
    Mockito.when(uploader.uploadMetadataTar(any(), any(LLCSegmentName.class), Mockito.anyInt()))
        .thenReturn(tmpLocation);
    Assert.assertEquals(commitAndCaptureParams(uploader, true).getMetadataTarLocation(), tmpLocation.toString());
  }

  @Test
  public void testCommitEndHasNoMetadataTarLocationWhenUploadFails()
      throws Exception {
    // mockUploader does not stub uploadMetadataTar, so it returns null like a failed upload.
    SegmentUploader uploader = mockUploader("hdfs://root/" + RAW_TABLE_NAME + "/" + _segmentName);
    Assert.assertNull(commitAndCaptureParams(uploader, true).getMetadataTarLocation());
  }

  @Test
  public void testCommitEndHasNoMetadataTarLocationWhenFlagOff()
      throws Exception {
    SegmentUploader uploader = mockUploader("hdfs://root/" + RAW_TABLE_NAME + "/" + _segmentName);
    Assert.assertNull(commitAndCaptureParams(uploader, false).getMetadataTarLocation());
  }

  @Test
  public void testMetadataTarUploadedForDeepStoreLocation()
      throws Exception {
    SegmentUploader uploader = mockUploader("hdfs://root/" + RAW_TABLE_NAME + "/" + _segmentName);
    ServerSegmentCompletionProtocolHandler handler = mockProtocolHandler();
    SegmentCompletionProtocol.Response response = newCommitter(handler, uploader, null).commit(mockBuildDescriptor());
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    Mockito.verify(uploader).uploadMetadataTar(any(), any(LLCSegmentName.class),
        Mockito.eq(PinotFSSegmentUploader.DEFAULT_SEGMENT_UPLOAD_TIMEOUT_MILLIS));
  }

  @Test
  public void testMetadataTarNotUploadedWhenFlagOff()
      throws Exception {
    SegmentUploader uploader = mockUploader("hdfs://root/" + RAW_TABLE_NAME + "/" + _segmentName);
    ServerSegmentCompletionProtocolHandler handler = mockProtocolHandler();
    SegmentCompletionProtocol.Response response =
        newCommitter(handler, uploader, null, false).commit(mockBuildDescriptor());
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    Mockito.verify(uploader, Mockito.never()).uploadMetadataTar(any(), any(), anyInt());
  }

  @Test
  public void testMetadataTarNotUploadedForPeerLocation()
      throws Exception {
    // Deep store upload fails (null) and a peer scheme is configured: the location becomes peer://...
    SegmentUploader uploader = mockUploader(null);
    ServerSegmentCompletionProtocolHandler handler = mockProtocolHandler();
    SegmentCompletionProtocol.Response response =
        newCommitter(handler, uploader, "http").commit(mockBuildDescriptor());
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    Mockito.verify(uploader, Mockito.never()).uploadMetadataTar(any(), any(), anyInt());
  }

  @Test
  public void testCommitSucceedsWhenMetadataTarUploadThrows()
      throws Exception {
    SegmentUploader uploader = mockUploader("hdfs://root/" + RAW_TABLE_NAME + "/" + _segmentName);
    Mockito.doThrow(new RuntimeException("boom")).when(uploader).uploadMetadataTar(any(), any(), anyInt());
    ServerSegmentCompletionProtocolHandler handler = mockProtocolHandler();
    SegmentCompletionProtocol.Response response = newCommitter(handler, uploader, null).commit(mockBuildDescriptor());
    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    Mockito.verify(handler).segmentCommitEndWithMetadata(any(), any());
    assertNoTempLeak();
  }

  @Test
  public void testStagingFailureEmitsFailureMeterAndCommitSucceeds()
      throws Exception {
    Map<String, File> brokenFiles = new HashMap<>(_metadataFiles);
    brokenFiles.put(V1Constants.SEGMENT_CREATION_META, new File(TEMP_DIR, "does-not-exist"));
    RealtimeSegmentDataManager.SegmentBuildDescriptor descriptor = mockBuildDescriptor();
    Mockito.when(descriptor.getMetadataFiles()).thenReturn(brokenFiles);
    SegmentUploader uploader = mockUploader("hdfs://root/" + RAW_TABLE_NAME + "/" + _segmentName);
    ServerSegmentCompletionProtocolHandler handler = mockProtocolHandler();

    SegmentCompletionProtocol.Response response = newCommitter(handler, uploader, null).commit(descriptor);

    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    Mockito.verify(handler).segmentCommitEndWithMetadata(any(), any());
    Mockito.verify(uploader, Mockito.never()).uploadMetadataTar(any(), any(), anyInt());
    Mockito.verify(_serverMetrics).addMeteredTableValue(RAW_TABLE_NAME, ServerMeter.METADATA_TAR_UPLOAD_FAILURE, 1L);
    assertNoTempLeak();
  }

  @Test
  public void testMetadataTarSkippedWhenUploaderDoesNotSupportIt()
      throws Exception {
    Map<String, File> brokenFiles = new HashMap<>(_metadataFiles);
    brokenFiles.put(V1Constants.SEGMENT_CREATION_META, new File(TEMP_DIR, "does-not-exist"));
    RealtimeSegmentDataManager.SegmentBuildDescriptor descriptor = mockBuildDescriptor();
    Mockito.when(descriptor.getMetadataFiles()).thenReturn(brokenFiles);
    SegmentUploader uploader = Mockito.mock(SegmentUploader.class);
    Mockito.when(uploader.uploadSegment(any(), any(LLCSegmentName.class)))
        .thenReturn(new URI("http://controller/" + _segmentName));
    ServerSegmentCompletionProtocolHandler handler = mockProtocolHandler();

    SegmentCompletionProtocol.Response response = newCommitter(handler, uploader, null).commit(descriptor);

    Assert.assertEquals(response.getStatus(), SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS);
    Mockito.verify(uploader, Mockito.never()).uploadMetadataTar(any(), any(), anyInt());
    Mockito.verifyNoInteractions(_serverMetrics);
    assertNoTempLeak();
  }

  private void assertNoTempLeak() {
    File[] leaked = FileUtils.getTempDirectory().listFiles((dir, name) -> name.startsWith(_segmentName));
    Assert.assertEquals(leaked == null ? 0 : leaked.length, 0);
  }

  @Test
  public void testMetadataTarRoundTripAndCleanup()
      throws Exception {
    File capturedCopy = new File(TEMP_DIR, "captured.metadata.tar.gz");
    SegmentUploader uploader = mockUploader("hdfs://root/" + RAW_TABLE_NAME + "/" + _segmentName);
    // The committer deletes its local tar once the upload call returns, so copy it inside the call.
    Mockito.doAnswer(invocation -> {
      FileUtils.copyFile(invocation.getArgument(0), capturedCopy);
      return new URI("hdfs://root/uploaded");
    }).when(uploader).uploadMetadataTar(any(), any(), anyInt());

    newCommitter(mockProtocolHandler(), uploader, null).commit(mockBuildDescriptor());

    // First top-level entry must be a directory holding both files (DefaultMetadataExtractor contract).
    File untarDir = new File(TEMP_DIR, "untar");
    File topLevel = TarCompressionUtils.untar(capturedCopy, untarDir).get(0);
    Assert.assertTrue(topLevel.isDirectory());
    Assert.assertTrue(new File(topLevel, V1Constants.MetadataKeys.METADATA_FILE_NAME).isFile());
    Assert.assertTrue(new File(topLevel, V1Constants.SEGMENT_CREATION_META).isFile());
    // creation.meta is binary and must be copied byte-for-byte.
    Assert.assertEquals(FileUtils.readFileToByteArray(new File(topLevel, V1Constants.SEGMENT_CREATION_META)),
        FileUtils.readFileToByteArray(_metadataFiles.get(V1Constants.SEGMENT_CREATION_META)));

    SegmentMetadata extracted =
        new DefaultMetadataExtractor().extractMetadata(capturedCopy, new File(TEMP_DIR, "extracted"));
    SegmentMetadata source = new SegmentMetadataImpl(_indexDir);
    Assert.assertEquals(extracted.getName(), source.getName());
    Assert.assertEquals(extracted.getCrc(), source.getCrc());
    Assert.assertEquals(extracted.getTotalDocs(), source.getTotalDocs());

    // No staging dir or local tar may leak in the system temp dir.
    assertNoTempLeak();
  }
}
