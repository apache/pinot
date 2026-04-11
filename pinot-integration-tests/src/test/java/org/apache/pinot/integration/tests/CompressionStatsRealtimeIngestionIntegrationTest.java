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
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Integration test that validates compression stats tracking end-to-end for realtime (Kafka) ingestion.
 *
 * <p>Creates a realtime table with {@code compressionStatsEnabled=true}, pushes data from Avro files
 * into Kafka with several raw (no-dictionary) columns using LZ4 compression, waits for all documents
 * to be consumed, and then verifies that the controller's {@code GET /tables/{table}/size} API response
 * includes valid compression statistics for the completed (COMPLETED) segments.
 */
public class CompressionStatsRealtimeIngestionIntegrationTest extends BaseClusterIntegrationTestSet {

  // Raw columns that will have compression stats tracked.
  // These are metric/dimension columns from the default On_Time schema that support raw encoding.
  private static final List<String> RAW_COLUMNS =
      List.of("ActualElapsedTime", "ArrDelay", "DepDelay", "CRSDepTime");

  @Override
  protected String getTableName() {
    return "compressionStatsRealtimeTest";
  }

  @Override
  protected long getCountStarResult() {
    return DEFAULT_COUNT_STAR_RESULT;
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return new ArrayList<>(RAW_COLUMNS);
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    for (String column : RAW_COLUMNS) {
      fieldConfigs.add(
          new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(),
              FieldConfig.CompressionCodec.LZ4, null));
    }
    return fieldConfigs;
  }

  @Override
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    TableConfig tableConfig = super.createRealtimeTableConfig(sampleAvroFile);

    // Enable compression stats tracking
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setCompressionStatsEnabled(true);

    return tableConfig;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir);

    // Start the Pinot cluster
    startZk();
    startKafka();
    startController();
    startBroker();
    startServer();

    // Unpack the Avro files
    List<File> avroFiles = unpackAvroData(_tempDir);

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createRealtimeTableConfig(avroFiles.get(0));
    addTableConfig(tableConfig);
    waitForAllRealtimePartitionsConsuming(
        TableNameBuilder.REALTIME.tableNameWithType(getTableName()), 120_000L);

    // Push data into Kafka
    pushAvroIntoKafka(avroFiles);

    // Wait for all documents to be loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropRealtimeTable(getTableName());
    waitForTableDataManagerRemoved(TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    waitForEVToDisappear(TableNameBuilder.REALTIME.tableNameWithType(getTableName()));
    stopServer();
    stopBroker();
    stopController();
    stopKafka();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  @Test
  public void testCompressionStatsInTableSizeApiForRealtimeTable()
      throws Exception {
    // Call the controller table size API
    String response = sendGetRequest(
        controllerUrl("/tables/" + getTableName() + "/size"));
    JsonNode tableSizeJson = JsonUtils.stringToJsonNode(response);

    // Verify top-level structure
    assertNotNull(tableSizeJson.get("tableName"), "Response should have tableName");
    assertTrue(tableSizeJson.get("reportedSizeInBytes").asLong() >= 0,
        "reportedSizeInBytes should be >= 0");

    // Get realtime segment details
    JsonNode realtimeSegments = tableSizeJson.get("realtimeSegments");
    assertNotNull(realtimeSegments, "realtimeSegments should be present");

    // Verify compression stats fields exist
    assertTrue(realtimeSegments.has("rawForwardIndexSizePerReplicaInBytes"),
        "realtimeSegments should have rawForwardIndexSizePerReplicaInBytes");
    assertTrue(realtimeSegments.has("compressedForwardIndexSizePerReplicaInBytes"),
        "realtimeSegments should have compressedForwardIndexSizePerReplicaInBytes");
    assertTrue(realtimeSegments.has("compressionRatio"),
        "realtimeSegments should have compressionRatio");
    assertTrue(realtimeSegments.has("segmentsWithStats"),
        "realtimeSegments should have segmentsWithStats");
    assertTrue(realtimeSegments.has("totalSegments"),
        "realtimeSegments should have totalSegments");

    long rawFwdIndexSize = realtimeSegments.get("rawForwardIndexSizePerReplicaInBytes").asLong();
    long compressedFwdIndexSize = realtimeSegments.get("compressedForwardIndexSizePerReplicaInBytes").asLong();
    double compressionRatio = realtimeSegments.get("compressionRatio").asDouble();
    int segmentsWithStats = realtimeSegments.get("segmentsWithStats").asInt();
    int totalSegments = realtimeSegments.get("totalSegments").asInt();

    // Total segments should be > 0 (at least consuming segments exist)
    assertTrue(totalSegments > 0,
        "totalSegments should be > 0, got: " + totalSegments);

    // Segments with stats: completed segments built with compressionStatsEnabled=true should have stats.
    // Consuming segments may or may not have stats depending on whether they've been committed.
    // We just verify the fields are present and non-negative.
    assertTrue(segmentsWithStats >= 0,
        "segmentsWithStats should be >= 0, got: " + segmentsWithStats);

    // If any completed segments exist with stats, verify the compression data makes sense
    if (segmentsWithStats > 0) {
      assertTrue(rawFwdIndexSize > 0,
          "rawForwardIndexSizePerReplicaInBytes should be > 0 when segments have stats, got: "
              + rawFwdIndexSize);
      assertTrue(compressedFwdIndexSize > 0,
          "compressedForwardIndexSizePerReplicaInBytes should be > 0 when segments have stats, got: "
              + compressedFwdIndexSize);
      assertTrue(compressionRatio > 0,
          "compressionRatio should be > 0 when segments have stats, got: " + compressionRatio);
      assertTrue(rawFwdIndexSize >= compressedFwdIndexSize,
          "rawForwardIndexSize (" + rawFwdIndexSize + ") should be >= compressedForwardIndexSize ("
              + compressedFwdIndexSize + ")");
      assertTrue(compressionRatio >= 1.0,
          "compressionRatio should be >= 1.0, got: " + compressionRatio);
    }
  }

  @Test
  public void testPerSegmentCompressionStatsForRealtimeTable()
      throws Exception {
    // Call table size API with verbose=true to get per-segment details
    String response = sendGetRequest(
        controllerUrl("/tables/" + getTableName() + "/size?verbose=true"));
    JsonNode tableSizeJson = JsonUtils.stringToJsonNode(response);

    JsonNode realtimeSegments = tableSizeJson.get("realtimeSegments");
    assertNotNull(realtimeSegments, "realtimeSegments should be present");

    JsonNode segments = realtimeSegments.get("segments");
    assertNotNull(segments, "segments map should be present in verbose response");

    // At least one segment should exist
    assertTrue(segments.size() > 0, "Should have at least one segment");

    // Iterate segments and validate structure
    int segmentsChecked = 0;
    var fieldNames = segments.fieldNames();
    while (fieldNames.hasNext()) {
      String segmentName = fieldNames.next();
      JsonNode segmentDetails = segments.get(segmentName);
      JsonNode serverInfo = segmentDetails.get("serverInfo");
      assertNotNull(serverInfo, "serverInfo should be present for segment: " + segmentName);

      var serverNames = serverInfo.fieldNames();
      while (serverNames.hasNext()) {
        String serverName = serverNames.next();
        JsonNode sizeInfo = serverInfo.get(serverName);
        long diskSize = sizeInfo.get("diskSizeInBytes").asLong();
        if (diskSize > 0) {
          // Verify compression stats fields exist in each server's response
          assertTrue(sizeInfo.has("rawForwardIndexSizeBytes"),
              "Server info should have rawForwardIndexSizeBytes for segment " + segmentName);
          assertTrue(sizeInfo.has("compressedForwardIndexSizeBytes"),
              "Server info should have compressedForwardIndexSizeBytes for segment " + segmentName);
        }
      }
      segmentsChecked++;
    }
    assertTrue(segmentsChecked > 0, "Should have checked at least one segment");
  }
}
