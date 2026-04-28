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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.plugin.segmentuploader.SegmentUploaderDefault;
import org.apache.pinot.plugin.segmentwriter.filebased.FileBasedSegmentWriter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.segment.uploader.SegmentUploader;
import org.apache.pinot.spi.ingestion.segment.writer.SegmentWriter;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests creating segments via the {@link SegmentWriter} implementations
 */
public class SegmentWriterUploaderIntegrationTest extends SharedRichClusterIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentWriterUploaderIntegrationTest.class);
  private static final String SHARED_TABLE_NAME = "segment_writer_uploader";

  private Schema _schema;
  private String _tableNameWithType;
  private List<File> _avroFiles;
  private File _classTempDir;
  private File _classSegmentDir;
  private File _classTarDir;

  @BeforeClass
  public void setUp()
      throws Exception {
    _classTempDir = getClassTempDir();
    _classSegmentDir = getClassSegmentDir();
    _classTarDir = getClassTarDir();
    TestUtils.ensureDirectoriesExistAndEmpty(_classTempDir, _classSegmentDir, _classTarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    cleanTableAndSchema();

    // Create and upload the schema
    _schema = createSchema();
    addSchema(_schema);
    _tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(getTableName());

    // Get avro files
    _avroFiles = getAllAvroFiles();
  }

  @Nullable
  protected IngestionConfig getIngestionConfig() {
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, _classTarDir.getAbsolutePath());
    batchConfigMap.put(BatchConfigProperties.OVERWRITE_OUTPUT, "false");
    batchConfigMap.put(BatchConfigProperties.PUSH_CONTROLLER_URI, getControllerBaseApiUrl());
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(batchConfigMap), "APPEND", "HOURLY"));
    return ingestionConfig;
  }

  @Override
  protected String getTableName() {
    return isSharedRichClusterEnabled() ? SHARED_TABLE_NAME : super.getTableName();
  }

  @Override
  protected boolean shouldStartSharedKafka() {
    return false;
  }

  @Override
  protected int getSharedNumBrokers() {
    return 1;
  }

  @Override
  protected int getSharedNumServers() {
    return 1;
  }

  @Override
  protected boolean shouldStartSharedMinion() {
    return false;
  }

  /**
   * Write the records from 3 avro files into the Pinot table using the {@link FileBasedSegmentWriter}
   * Calls {@link SegmentWriter#flush()} after writing records from each avro file
   * Checks the number of segments created and total docs from the query
   */
  @Test
  public void testFileBasedSegmentWriterAndDefaultUploader()
      throws Exception {

    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);

    SegmentUploader segmentUploader = new SegmentUploaderDefault();
    segmentUploader.init(offlineTableConfig);

    GenericRow reuse = new GenericRow();
    long totalDocs = 0;
    try (SegmentWriter segmentWriter = new FileBasedSegmentWriter()) {
      segmentWriter.init(offlineTableConfig, _schema);
      for (int i = 0; i < 3; i++) {
        long numDocsInSegment = 0;
        try (AvroRecordReader avroRecordReader = new AvroRecordReader()) {
          avroRecordReader.init(_avroFiles.get(i), null, null);

          while (avroRecordReader.hasNext()) {
            avroRecordReader.next(reuse);
            segmentWriter.collect(reuse);
            numDocsInSegment++;
            totalDocs++;
          }
        }
        // flush to segment
        URI segmentTarURI = segmentWriter.flush();
        // upload
        segmentUploader.uploadSegment(segmentTarURI, null);

        // check num segments
        Assert.assertEquals(getNumSegments(), i + 1);
        // check numDocs in latest segment
        Assert.assertEquals(getNumDocsInLatestSegment(), numDocsInSegment);
        // check totalDocs in query
        checkTotalDocsInQuery(totalDocs);
      }
    }

    dropAllSegments(getTableName(), TableType.OFFLINE);
    checkNumSegments(0);

    // upload all together using dir
    segmentUploader.uploadSegmentsFromDir(_classTarDir.toURI(), null);
    // check num segments
    Assert.assertEquals(getNumSegments(), 3);
    // check totalDocs in query
    checkTotalDocsInQuery(totalDocs);

    cleanTableAndSchema();
  }

  private int getNumSegments()
      throws Exception {
    List<String> segments =
        getOrCreateAdminClient().getSegmentClient().listSegments(_tableNameWithType, TableType.OFFLINE.toString(),
            false);
    return segments.size();
  }

  private int getTotalDocsFromQuery()
      throws Exception {
    JsonNode response = postQuery(String.format("select count(*) from %s", _tableNameWithType));
    return response.get("resultTable").get("rows").get(0).get(0).asInt();
  }

  private int getNumDocsInLatestSegment()
      throws Exception {
    List<String> segments =
        getOrCreateAdminClient().getSegmentClient().listSegments(_tableNameWithType, TableType.OFFLINE.toString(),
            false);
    String segmentName = segments.get(segments.size() - 1);
    Map<String, Object> metadata =
        getOrCreateAdminClient().getSegmentClient().getSegmentMetadata(_tableNameWithType, segmentName, null);
    Object totalDocs = metadata.get("segment.total.docs");
    return totalDocs == null ? 0 : Integer.parseInt(totalDocs.toString());
  }

  private void checkTotalDocsInQuery(long expectedTotalDocs) {
    TestUtils.waitForCondition(new Function<>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          return getTotalDocsFromQuery() == expectedTotalDocs;
        } catch (Exception e) {
          LOGGER.error("Caught exception when getting totalDocs from query: {}", e.getMessage());
          return null;
        }
      }
    }, 100L, 120_000L, "Failed to load " + expectedTotalDocs + " documents");
  }

  private void checkNumSegments(int expectedNumSegments) {
    TestUtils.waitForCondition(new Function<>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          return getNumSegments() == expectedNumSegments;
        } catch (Exception e) {
          LOGGER.error("Caught exception when getting num segments: {}", e.getMessage());
          return null;
        }
      }
    }, 100L, 120_000L, "Failed to load get num segments");
  }

  @Override
  protected List<File> getAllAvroFiles()
      throws Exception {
    int numSegments = unpackAvroData(_classTempDir).size();

    List<File> avroFiles = new ArrayList<>(numSegments);
    for (int i = 1; i <= numSegments; i++) {
      avroFiles.add(new File(_classTempDir, "On_Time_On_Time_Performance_2014_" + i + ".avro"));
    }

    return avroFiles;
  }

  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws Exception {
    Exception exception = null;
    exception = runCleanup(exception, this::cleanTableAndSchema);
    exception = runCleanup(exception, this::stopServer);
    exception = runCleanup(exception, this::stopBroker);
    exception = runCleanup(exception, this::stopControllerIfStarted);
    exception = runCleanup(exception, this::stopZk);
    exception = runCleanup(exception, this::cleanClassTempDirectory);
    if (exception != null) {
      throw exception;
    }
  }

  private File getClassTempDir() {
    return isSharedRichClusterEnabled() ? new File(_tempDir, "testData") : _tempDir;
  }

  private File getClassSegmentDir() {
    return isSharedRichClusterEnabled() ? new File(_classTempDir, "segmentDir") : _segmentDir;
  }

  private File getClassTarDir() {
    return isSharedRichClusterEnabled() ? new File(_classTempDir, "tarDir") : _tarDir;
  }

  private void cleanTableAndSchema()
      throws Exception {
    if (_helixResourceManager == null) {
      return;
    }

    String tableName = getTableName();
    String offlineTableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
    if (_helixResourceManager.getAllTables().contains(offlineTableName) || _helixResourceManager.hasOfflineTable(
        tableName)) {
      dropOfflineTable(tableName);
      waitForTableDataManagerRemoved(offlineTableName);
      waitForEVToDisappear(offlineTableName);
    }
    if (_helixResourceManager.getSchema(tableName) != null) {
      deleteSchema(tableName);
    }
  }

  private void stopControllerIfStarted() {
    if (_controllerStarter != null) {
      stopController();
    }
  }

  private void cleanClassTempDirectory()
      throws Exception {
    if (_classTempDir != null) {
      FileUtils.deleteDirectory(_classTempDir);
    }
  }

  private Exception runCleanup(Exception firstException, Cleanup cleanup) {
    try {
      cleanup.run();
    } catch (Exception e) {
      if (firstException == null) {
        return e;
      }
      firstException.addSuppressed(e);
    }
    return firstException;
  }

  private interface Cleanup {
    void run()
        throws Exception;
  }
}
