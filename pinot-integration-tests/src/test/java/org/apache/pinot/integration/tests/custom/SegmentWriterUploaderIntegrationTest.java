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
package org.apache.pinot.integration.tests.custom;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
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
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests creating segments via the [SegmentWriter] implementations
@Test(suiteName = "CustomClusterIntegrationTest")
public class SegmentWriterUploaderIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentWriterUploaderIntegrationTest.class);

  private Schema _schema;
  private String _tableNameWithType;
  private List<File> _avroFiles;

  @Override
  @BeforeClass
  public void setUp()
      throws Exception {
    initControllerRequestURLBuilder();
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Create and upload the schema
    _schema = createSchema();
    addSchema(_schema);
    _tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE).tableNameWithType(getTableName());

    // Get avro files
    _avroFiles = createAvroFiles();
  }

  @Override
  public String getTableName() {
    return getClass().getSimpleName();
  }

  @Override
  public Schema createSchema() {
    try {
      Schema schema = createSchema(getSchemaFileName());
      schema.setSchemaName(getTableName());
      return schema;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    return getAllAvroFiles();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    // Preserve the airline table settings used before sharing the cluster.
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setTimeColumnName(DEFAULT_TIME_COLUMN_NAME)
        .setSortedColumn(DEFAULT_SORTED_COLUMN)
        .setInvertedIndexColumns(new ArrayList<>(DEFAULT_INVERTED_INDEX_COLUMNS))
        .setNoDictionaryColumns(new ArrayList<>(DEFAULT_NO_DICTIONARY_COLUMNS))
        .setRangeIndexColumns(new ArrayList<>(List.of("Origin")))
        .setBloomFilterColumns(new ArrayList<>(List.of("FlightNum", "Origin")))
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setTaskConfig(getTaskConfig())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setQueryConfig(getQueryConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .setSegmentPartitionConfig(getSegmentPartitionConfig())
        .setOptimizeNoDictStatsCollection(true)
        .build();
  }

  @Override
  @Nullable
  protected IngestionConfig getIngestionConfig() {
    Map<String, String> batchConfigMap = new HashMap<>();
    batchConfigMap.put(BatchConfigProperties.OUTPUT_DIR_URI, _tarDir.getAbsolutePath());
    batchConfigMap.put(BatchConfigProperties.OVERWRITE_OUTPUT, "false");
    batchConfigMap.put(BatchConfigProperties.PUSH_CONTROLLER_URI, _controllerRequestURLBuilder.getBaseUrl());
    IngestionConfig ingestionConfig = new IngestionConfig();
    ingestionConfig.setBatchIngestionConfig(
        new BatchIngestionConfig(List.of(batchConfigMap), "APPEND", "HOURLY"));
    return ingestionConfig;
  }

  /// Write the records from 3 avro files into the Pinot table using the [FileBasedSegmentWriter]
  /// Calls [SegmentWriter#flush()] after writing records from each avro file
  /// Checks the number of segments created and total docs from the query
  @Test
  public void testFileBasedSegmentWriterAndDefaultUploader()
      throws Exception {

    TableConfig offlineTableConfig = createOfflineTableConfig();
    addTableConfig(offlineTableConfig);

    SegmentUploader segmentUploader = new SegmentUploaderDefault();
    GenericRow reuse = new GenericRow();
    long totalDocs = 0;
    try (SegmentWriter segmentWriter = new FileBasedSegmentWriter()) {
      segmentWriter.init(offlineTableConfig, _schema);
      segmentUploader.init(offlineTableConfig);

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
        assertEquals(getNumSegments(), i + 1);
        // check numDocs in latest segment
        assertEquals(getNumDocsInLatestSegment(), numDocsInSegment);
        // check totalDocs in query
        checkTotalDocsInQuery(totalDocs);
      }
    }

    dropAllSegments(_tableNameWithType, TableType.OFFLINE);
    checkNumSegments(0);

    // upload all together using dir
    segmentUploader.uploadSegmentsFromDir(_tarDir.toURI(), null);
    // check num segments
    assertEquals(getNumSegments(), 3);
    // check totalDocs in query
    checkTotalDocsInQuery(totalDocs);

    dropOfflineTable(_tableNameWithType);
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
  @AfterClass(alwaysRun = true)
  public void tearDown()
      throws IOException {
    try {
      if (getSharedHelixResourceManager().getOfflineTableConfig(getTableName()) != null) {
        dropOfflineTable(getTableName());
      }
    } finally {
      try {
        if (getSharedHelixResourceManager().getSchema(getTableName()) != null) {
          deleteSchema(getTableName());
        }
      } finally {
        String tableNameWithType = TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
        TestUtils.waitForCondition(aVoid -> getSharedServerStarters().stream().allMatch(
            server -> server.getServerInstance().getInstanceDataManager().getTableDataManager(tableNameWithType)
                == null), 60_000L, "Failed to remove table data manager for table: " + tableNameWithType);
        FileUtils.deleteDirectory(_tempDir);
      }
    }
  }
}
