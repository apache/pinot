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
package org.apache.pinot.controller.api.upload;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.utils.FileUploadDownloadClient.FileUploadType;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.common.utils.URIUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.api.exception.ControllerApplicationException;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants.Segment.Realtime.Status;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


public class ZKOperatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "ZKOperatorTest");
  private static final File SEGMENT_DIR = new File(TEMP_DIR, "segmentDir");
  private static final File DATA_DIR = new File(TEMP_DIR, "dataDir");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String OFFLINE_TABLE_NAME = TableNameBuilder.OFFLINE.tableNameWithType(RAW_TABLE_NAME);
  private static final String REALTIME_TABLE_NAME = TableNameBuilder.REALTIME.tableNameWithType(RAW_TABLE_NAME);
  private static final String TIME_COLUMN = "timeColumn";
  private static final String SEGMENT_NAME = "testSegment";
  // NOTE: The FakeStreamConsumerFactory will create 2 stream partitions. Use partition 2 to avoid conflict.
  private static final String LLC_SEGMENT_NAME =
      new LLCSegmentName(RAW_TABLE_NAME, 2, 0, System.currentTimeMillis()).getSegmentName();
  private static final ControllerTest TEST_INSTANCE = ControllerTest.getInstance();

  private PinotHelixResourceManager _resourceManager;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    TEST_INSTANCE.setupSharedStateAndValidate();
    _resourceManager = TEST_INSTANCE.getHelixResourceManager();

    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addDateTime(TIME_COLUMN, DataType.TIMESTAMP, "1:MILLISECONDS:TIMESTAMP", "1:MILLISECONDS").build();
    TableConfig offlineTableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
    TableConfig realtimeTableConfig =
        new TableConfigBuilder(TableType.REALTIME).setTableName(RAW_TABLE_NAME).setTimeColumnName(TIME_COLUMN)
            .setStreamConfigs(getStreamConfigs()).setLLC(true).setNumReplicas(1).build();

    _resourceManager.addSchema(schema, false, false);
    _resourceManager.addTable(offlineTableConfig);
    _resourceManager.addTable(realtimeTableConfig);
  }

  private Map<String, String> getStreamConfigs() {
    Map<String, String> streamConfigs = new HashMap<>();
    streamConfigs.put("streamType", "kafka");
    streamConfigs.put("stream.kafka.topic.name", "kafkaTopic");
    streamConfigs.put("stream.kafka.consumer.type", "simple");
    streamConfigs.put("stream.kafka.decoder.class.name",
        "org.apache.pinot.plugin.stream.kafka.KafkaAvroMessageDecoder");
    streamConfigs.put("stream.kafka.consumer.factory.class.name",
        "org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConsumerFactory");
    return streamConfigs;
  }

  private File generateSegment()
      throws Exception {
    Schema schema =
        new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME).addSingleValueDimension("colA", DataType.INT).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    File outputDir = new File(SEGMENT_DIR, "segment");
    config.setOutDir(outputDir.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);
    GenericRow row = new GenericRow();
    row.putValue("colA", "100");
    List<GenericRow> rows = ImmutableList.of(row);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    File segmentTar = new File(SEGMENT_DIR, SEGMENT_NAME + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarGzCompressionUtils.createTarGzFile(new File(outputDir, SEGMENT_NAME),
        new File(SEGMENT_DIR, SEGMENT_NAME + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION));
    FileUtils.deleteQuietly(outputDir);
    return segmentTar;
  }

  private void checkSegmentZkMetadata(String segmentName, long crc, long creationTime) {
    SegmentZKMetadata segmentZKMetadata = _resourceManager.getSegmentZKMetadata(OFFLINE_TABLE_NAME, segmentName);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getCrc(), crc);
    assertEquals(segmentZKMetadata.getCreationTime(), creationTime);
    long pushTime = segmentZKMetadata.getPushTime();
    assertTrue(pushTime > 0);
    assertEquals(segmentZKMetadata.getRefreshTime(), Long.MIN_VALUE);
    assertEquals(segmentZKMetadata.getDownloadUrl(), "downloadUrl");
    assertEquals(segmentZKMetadata.getCrypterName(), "crypter");
    assertEquals(segmentZKMetadata.getSegmentUploadStartTime(), -1);
    assertEquals(segmentZKMetadata.getSizeInBytes(), 10);
  }

  @Test
  public void testMetadataUploadType()
      throws Exception {
    String segmentName = "metadataTest";
    FileUtils.deleteQuietly(TEMP_DIR);
    ZKOperator zkOperator = new ZKOperator(_resourceManager, mock(ControllerConf.class), mock(ControllerMetrics.class));

    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getName()).thenReturn(segmentName);
    when(segmentMetadata.getCrc()).thenReturn("12345");
    when(segmentMetadata.getIndexCreationTime()).thenReturn(123L);
    HttpHeaders httpHeaders = mock(HttpHeaders.class);

    File segmentTar = generateSegment();
    String sourceDownloadURIStr = segmentTar.toURI().toString();
    File segmentFile = new File("metadataOnly");

    // with finalSegmentLocation not null
    File finalSegmentLocation = new File(DATA_DIR, segmentName);
    Assert.assertFalse(finalSegmentLocation.exists());
    zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, FileUploadType.METADATA,
        finalSegmentLocation.toURI(), segmentFile, sourceDownloadURIStr, "downloadUrl", "crypter", 10, true, true,
        httpHeaders);
    Assert.assertTrue(finalSegmentLocation.exists());
    Assert.assertTrue(segmentTar.exists());
    checkSegmentZkMetadata(segmentName, 12345L, 123L);

    _resourceManager.deleteSegment(OFFLINE_TABLE_NAME, segmentName);
    // Wait for the segment Zk entry to be deleted.
    TestUtils.waitForCondition(aVoid -> {
      SegmentZKMetadata segmentZKMetadata = _resourceManager.getSegmentZKMetadata(OFFLINE_TABLE_NAME, segmentName);
      return segmentZKMetadata == null;
    }, 30_000L, "Failed to delete segmentZkMetadata.");

    FileUtils.deleteQuietly(DATA_DIR);
    // with finalSegmentLocation null
    zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, FileUploadType.METADATA, null,
        segmentFile, sourceDownloadURIStr, "downloadUrl", "crypter", 10, true, true, httpHeaders);
    Assert.assertFalse(finalSegmentLocation.exists());
    Assert.assertTrue(segmentTar.exists());
    checkSegmentZkMetadata(segmentName, 12345L, 123L);
  }

  @Test
  public void testCompleteSegmentOperations()
      throws Exception {
    ZKOperator zkOperator = new ZKOperator(_resourceManager, mock(ControllerConf.class), mock(ControllerMetrics.class));

    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getName()).thenReturn(SEGMENT_NAME);
    when(segmentMetadata.getCrc()).thenReturn("12345");
    when(segmentMetadata.getIndexCreationTime()).thenReturn(123L);
    HttpHeaders httpHeaders = mock(HttpHeaders.class);

    // Test if Zk segment metadata is removed if exception is thrown when moving segment to final location.
    try {
      // Create mock finalSegmentLocationURI and segmentFile.
      URI finalSegmentLocationURI =
          URIUtils.getUri("mockPath", OFFLINE_TABLE_NAME, URIUtils.encode(segmentMetadata.getName()));
      File segmentFile = new File(new File("foo/bar"), "mockChild");
      zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT,
          finalSegmentLocationURI, segmentFile, "downloadUrl", "downloadUrl", "crypter", 10, true, true, httpHeaders);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Wait for the segment Zk entry to be deleted.
    TestUtils.waitForCondition(aVoid -> {
      SegmentZKMetadata segmentZKMetadata = _resourceManager.getSegmentZKMetadata(OFFLINE_TABLE_NAME, SEGMENT_NAME);
      return segmentZKMetadata == null;
    }, 30_000L, "Failed to delete segmentZkMetadata.");

    zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT, null, null,
        "downloadUrl", "downloadUrl", "crypter", 10, true, true, httpHeaders);
    SegmentZKMetadata segmentZKMetadata = _resourceManager.getSegmentZKMetadata(OFFLINE_TABLE_NAME, SEGMENT_NAME);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getCrc(), 12345L);
    assertEquals(segmentZKMetadata.getCreationTime(), 123L);
    long pushTime = segmentZKMetadata.getPushTime();
    assertTrue(pushTime > 0);
    assertEquals(segmentZKMetadata.getRefreshTime(), Long.MIN_VALUE);
    assertEquals(segmentZKMetadata.getDownloadUrl(), "downloadUrl");
    assertEquals(segmentZKMetadata.getCrypterName(), "crypter");
    assertEquals(segmentZKMetadata.getSegmentUploadStartTime(), -1);
    assertEquals(segmentZKMetadata.getSizeInBytes(), 10);

    // Test if the same segment can be uploaded when the previous upload failed after segment ZK metadata is created but
    // before segment is assigned to the ideal state
    // Manually remove the segment from the ideal state
    IdealState idealState = _resourceManager.getTableIdealState(OFFLINE_TABLE_NAME);
    assertNotNull(idealState);
    idealState.getRecord().getMapFields().remove(SEGMENT_NAME);
    _resourceManager.getHelixAdmin()
        .setResourceIdealState(_resourceManager.getHelixClusterName(), OFFLINE_TABLE_NAME, idealState);
    // The segment should be uploaded as a new segment (push time should change, and refresh time shouldn't be set)
    zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT, null, null,
        "downloadUrl", "downloadUrl", "crypter", 10, true, true, httpHeaders);
    segmentZKMetadata = _resourceManager.getSegmentZKMetadata(OFFLINE_TABLE_NAME, SEGMENT_NAME);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getCrc(), 12345L);
    assertEquals(segmentZKMetadata.getCreationTime(), 123L);
    assertTrue(segmentZKMetadata.getPushTime() > pushTime);
    pushTime = segmentZKMetadata.getPushTime();
    assertTrue(pushTime > 0);
    assertEquals(segmentZKMetadata.getRefreshTime(), Long.MIN_VALUE);
    assertEquals(segmentZKMetadata.getDownloadUrl(), "downloadUrl");
    assertEquals(segmentZKMetadata.getCrypterName(), "crypter");
    assertEquals(segmentZKMetadata.getSegmentUploadStartTime(), -1);
    assertEquals(segmentZKMetadata.getSizeInBytes(), 10);

    // Upload the same segment with allowRefresh = false. Validate that an exception is thrown.
    try {
      zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT, null, null,
          "otherDownloadUrl", "otherDownloadUrl", "otherCrypter", 10, true, false, httpHeaders);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Refresh the segment with unmatched IF_MATCH field
    when(httpHeaders.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn("123");
    try {
      zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT, null, null,
          "otherDownloadUrl", "otherDownloadUrl", "otherCrypter", 10, true, true, httpHeaders);
      fail();
    } catch (Exception e) {
      // Expected
    }

    // Refresh the segment with the same segment (same CRC) with matched IF_MATCH field but different creation time,
    // downloadURL and crypter
    when(httpHeaders.getHeaderString(HttpHeaders.IF_MATCH)).thenReturn("12345");
    when(segmentMetadata.getIndexCreationTime()).thenReturn(456L);
    zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT, null, null,
        "otherDownloadUrl", "otherDownloadUrl", "otherCrypter", 10, true, true, httpHeaders);

    segmentZKMetadata = _resourceManager.getSegmentZKMetadata(OFFLINE_TABLE_NAME, SEGMENT_NAME);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getCrc(), 12345L);
    // Push time should not change
    assertEquals(segmentZKMetadata.getPushTime(), pushTime);
    // Creation time and refresh time should change
    assertEquals(segmentZKMetadata.getCreationTime(), 456L);
    long refreshTime = segmentZKMetadata.getRefreshTime();
    assertTrue(refreshTime > 0);
    // Download URL should change. Refer: https://github.com/apache/pinot/issues/11535
    assertEquals(segmentZKMetadata.getDownloadUrl(), "otherDownloadUrl");
    // crypter should not be changed
    assertEquals(segmentZKMetadata.getCrypterName(), "crypter");
    assertEquals(segmentZKMetadata.getSegmentUploadStartTime(), -1);
    assertEquals(segmentZKMetadata.getSizeInBytes(), 10);

    // Refresh the segment with a different segment (different CRC)
    when(segmentMetadata.getCrc()).thenReturn("23456");
    when(segmentMetadata.getIndexCreationTime()).thenReturn(789L);
    // Add a tiny sleep to guarantee that refresh time is different from the previous round
    Thread.sleep(10);
    zkOperator.completeSegmentOperations(OFFLINE_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT, null, null,
        "otherDownloadUrl", "otherDownloadUrl", "otherCrypter", 100, true, true, httpHeaders);

    segmentZKMetadata = _resourceManager.getSegmentZKMetadata(OFFLINE_TABLE_NAME, SEGMENT_NAME);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getCrc(), 23456L);
    // Push time should not change
    assertEquals(segmentZKMetadata.getPushTime(), pushTime);
    // Creation time, refresh time, downloadUrl and crypter should change
    assertEquals(segmentZKMetadata.getCreationTime(), 789L);
    assertTrue(segmentZKMetadata.getRefreshTime() > refreshTime);
    assertEquals(segmentZKMetadata.getDownloadUrl(), "otherDownloadUrl");
    assertEquals(segmentZKMetadata.getCrypterName(), "otherCrypter");
    assertEquals(segmentZKMetadata.getSizeInBytes(), 100);
  }

  @Test
  public void testPushToRealtimeTable()
      throws Exception {
    ZKOperator zkOperator = new ZKOperator(_resourceManager, mock(ControllerConf.class), mock(ControllerMetrics.class));

    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    when(segmentMetadata.getName()).thenReturn(SEGMENT_NAME);
    when(segmentMetadata.getCrc()).thenReturn("12345");
    zkOperator.completeSegmentOperations(REALTIME_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT, null, null,
        "downloadUrl", "downloadUrl", null, 10, true, true, mock(HttpHeaders.class));

    SegmentZKMetadata segmentZKMetadata = _resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, SEGMENT_NAME);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getStatus(), Status.UPLOADED);
    assertNull(segmentMetadata.getStartOffset());
    assertNull(segmentMetadata.getEndOffset());

    // Uploading a segment with LLC segment name but without start/end offset should fail
    when(segmentMetadata.getName()).thenReturn(LLC_SEGMENT_NAME);
    when(segmentMetadata.getCrc()).thenReturn("23456");
    try {
      zkOperator.completeSegmentOperations(REALTIME_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT, null, null,
          "downloadUrl", "downloadUrl", null, 10, true, true, mock(HttpHeaders.class));
      fail();
    } catch (ControllerApplicationException e) {
      assertEquals(e.getResponse().getStatus(), Response.Status.BAD_REQUEST.getStatusCode());
    }
    assertNull(_resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, LLC_SEGMENT_NAME));

    // Uploading a segment with LLC segment name and start/end offset should success
    when(segmentMetadata.getStartOffset()).thenReturn("0");
    when(segmentMetadata.getEndOffset()).thenReturn("1234");
    zkOperator.completeSegmentOperations(REALTIME_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT, null, null,
        "downloadUrl", "downloadUrl", null, 10, true, true, mock(HttpHeaders.class));

    segmentZKMetadata = _resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, LLC_SEGMENT_NAME);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getStatus(), Status.UPLOADED);
    assertEquals(segmentZKMetadata.getStartOffset(), "0");
    assertEquals(segmentZKMetadata.getEndOffset(), "1234");

    // Refreshing a segment with LLC segment name but without start/end offset should success
    when(segmentMetadata.getCrc()).thenReturn("34567");
    when(segmentMetadata.getStartOffset()).thenReturn(null);
    when(segmentMetadata.getEndOffset()).thenReturn(null);
    zkOperator.completeSegmentOperations(REALTIME_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT, null, null,
        "downloadUrl", "downloadUrl", null, 10, true, true, mock(HttpHeaders.class));

    segmentZKMetadata = _resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, LLC_SEGMENT_NAME);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getStatus(), Status.UPLOADED);
    assertEquals(segmentZKMetadata.getStartOffset(), "0");
    assertEquals(segmentZKMetadata.getEndOffset(), "1234");

    // Refreshing a segment with LLC segment name and start/end offset should override the offsets
    when(segmentMetadata.getCrc()).thenReturn("45678");
    when(segmentMetadata.getStartOffset()).thenReturn("1234");
    when(segmentMetadata.getEndOffset()).thenReturn("2345");
    zkOperator.completeSegmentOperations(REALTIME_TABLE_NAME, segmentMetadata, FileUploadType.SEGMENT, null, null,
        "downloadUrl", "downloadUrl", null, 10, true, true, mock(HttpHeaders.class));

    segmentZKMetadata = _resourceManager.getSegmentZKMetadata(REALTIME_TABLE_NAME, LLC_SEGMENT_NAME);
    assertNotNull(segmentZKMetadata);
    assertEquals(segmentZKMetadata.getStatus(), Status.UPLOADED);
    assertEquals(segmentZKMetadata.getStartOffset(), "1234");
    assertEquals(segmentZKMetadata.getEndOffset(), "2345");
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
    TEST_INSTANCE.cleanup();
  }
}
