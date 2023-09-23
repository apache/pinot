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
import java.io.IOException;
import java.net.URI;
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
import org.apache.pinot.spi.utils.JsonUtils;
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
@Test(suiteName = "integration-suite-2", groups = {"integration-suite-2"})
public class SegmentWriterUploaderIntegrationTest extends BaseClusterIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentWriterUploaderIntegrationTest.class);

  private Schema _schema;
  private String _tableNameWithType;
  private List<File> _avroFiles;

  @BeforeClass
  public void setUp()
      throws Exception {
    System.out.println("this.getClass().getName() = " + this.getClass().getName());
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

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
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, _tarDir.getAbsolutePath());
    batchConfigMap.put(BatchConfigProperties.OVERWRITE_OUTPUT, "false");
    batchConfigMap.put(BatchConfigProperties.PUSH_CONTROLLER_URI, getControllerBaseApiUrl());
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(Collections.singletonList(batchConfigMap), "APPEND", "HOURLY"));
    return ingestionConfig;
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

    SegmentWriter segmentWriter = new FileBasedSegmentWriter();
    segmentWriter.init(offlineTableConfig, _schema);
    SegmentUploader segmentUploader = new SegmentUploaderDefault();
    segmentUploader.init(offlineTableConfig);

    GenericRow reuse = new GenericRow();
    long totalDocs = 0;
    for (int i = 0; i < 3; i++) {
      AvroRecordReader avroRecordReader = new AvroRecordReader();
      avroRecordReader.init(_avroFiles.get(i), null, null);

      long numDocsInSegment = 0;
      while (avroRecordReader.hasNext()) {
        avroRecordReader.next(reuse);
        segmentWriter.collect(reuse);
        numDocsInSegment++;
        totalDocs++;
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
    segmentWriter.close();

    dropAllSegments(_tableNameWithType, TableType.OFFLINE);
    checkNumSegments(0);

    // upload all together using dir
    segmentUploader.uploadSegmentsFromDir(_tarDir.toURI(), null);
    // check num segments
    Assert.assertEquals(getNumSegments(), 3);
    // check totalDocs in query
    checkTotalDocsInQuery(totalDocs);

    dropOfflineTable(_tableNameWithType);
  }

  private int getNumSegments()
      throws IOException {
    String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPI(_tableNameWithType, TableType.OFFLINE.toString()));
    JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
    return array.get(0).get("OFFLINE").size();
  }

  private int getTotalDocsFromQuery()
      throws Exception {
    JsonNode response = postQuery(String.format("select count(*) from %s", _tableNameWithType));
    return response.get("resultTable").get("rows").get(0).get(0).asInt();
  }

  private int getNumDocsInLatestSegment()
      throws IOException {
    String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPI(_tableNameWithType, TableType.OFFLINE.toString()));
    JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
    JsonNode segments = array.get(0).get("OFFLINE");
    String segmentName = segments.get(segments.size() - 1).asText();

    jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentMetadata(_tableNameWithType, segmentName));
    JsonNode metadata = JsonUtils.stringToJsonNode(jsonOutputStr);
    return metadata.get("segment.total.docs").asInt();
  }

  private void checkTotalDocsInQuery(long expectedTotalDocs) {
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
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
    }, 100L, 120_000, "Failed to load " + expectedTotalDocs + " documents", true);
  }

  private void checkNumSegments(int expectedNumSegments) {
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
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
    }, 100L, 120_000, "Failed to load get num segments", true);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    stopServer();
    stopBroker();
    stopController();
    stopZk();

    FileUtils.deleteDirectory(_tempDir);
  }
}
