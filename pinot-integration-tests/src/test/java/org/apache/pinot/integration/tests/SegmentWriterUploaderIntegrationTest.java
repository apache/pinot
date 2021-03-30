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
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.plugin.segmentwriter.filebased.FileBasedSegmentWriter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
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
public class SegmentWriterUploaderIntegrationTest extends BaseClusterIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentWriterUploaderIntegrationTest.class);

  private Schema _schema;
  private String _tableNameWithType;
  private List<File> _avroFiles;

  @BeforeClass
  public void setUp()
      throws Exception {
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
    return new IngestionConfig(new BatchIngestionConfig(Lists.newArrayList(batchConfigMap), "APPEND", "HOURLY"), null,
        null, null);
  }

  /**
   * Write the records from 3 avro files into the Pinot table using the {@link FileBasedSegmentWriter}
   * Calls {@link SegmentWriter#flush()} after writing records from each avro file
   * Checks the number of segments created and total docs from the query
   */
  @Test
  public void testFileBasedSegmentWriter()
      throws Exception {

    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);

    SegmentWriter segmentWriter = new FileBasedSegmentWriter();
    segmentWriter.init(offlineTableConfig, _schema);

    GenericRow reuse = new GenericRow();
    long totalDocs = 0;
    for (int i = 0; i < 3; i++) {
      AvroRecordReader avroRecordReader = new AvroRecordReader();
      avroRecordReader.init(_avroFiles.get(i), null, null);

      while (avroRecordReader.hasNext()) {
        avroRecordReader.next(reuse);
        segmentWriter.collect(reuse);
        totalDocs++;
      }
      segmentWriter.flush();
    }
    segmentWriter.close();

    // Manually upload
    // TODO: once an implementation of SegmentUploader is available, use that instead
    uploadSegments(_tableNameWithType, _tarDir);

    // check num segments
    Assert.assertEquals(getNumSegments(), 3);
    final long expectedDocs = totalDocs;
    TestUtils.waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          return getTotalDocsFromQuery() == expectedDocs;
        } catch (Exception e) {
          LOGGER.error("Caught exception when getting totalDocs from query: {}", e.getMessage());
          return null;
        }
      }
    }, 100L, 120_000, "Failed to load " + expectedDocs + " documents", true);

    dropOfflineTable(_tableNameWithType);
  }

  private int getNumSegments()
      throws IOException {
    String jsonOutputStr = sendGetRequest(_controllerRequestURLBuilder.
        forSegmentListAPIWithTableType(_tableNameWithType, TableType.OFFLINE.toString()));
    JsonNode array = JsonUtils.stringToJsonNode(jsonOutputStr);
    return array.get(0).get("OFFLINE").size();
  }

  private int getTotalDocsFromQuery()
      throws Exception {
    JsonNode response = postSqlQuery(String.format("select count(*) from %s", _tableNameWithType), _brokerBaseApiUrl);
    return response.get("resultTable").get("rows").get(0).get(0).asInt();
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
