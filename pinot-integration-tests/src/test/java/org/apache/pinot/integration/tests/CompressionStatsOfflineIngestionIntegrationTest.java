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
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Integration test that validates compression stats tracking end-to-end for offline batch ingestion.
 *
 * <p>Creates an offline table with {@code compressionStatsEnabled=true}, ingests data from Avro files
 * with several raw (no-dictionary) columns using LZ4 compression, and then verifies that the
 * controller's {@code GET /tables/{table}/size} API response includes valid compression statistics:
 * raw forward index sizes, compressed forward index sizes, compression ratio, and segment coverage.
 */
public class CompressionStatsOfflineIngestionIntegrationTest extends BaseClusterIntegrationTest {
  private static final int NUM_BROKERS = 1;
  private static final int NUM_SERVERS = 1;

  // Raw columns that will have compression stats tracked.
  // These are metric/dimension columns from the default On_Time schema that support raw encoding.
  private static final List<String> RAW_COLUMNS =
      List.of("ActualElapsedTime", "ArrDelay", "DepDelay", "CRSDepTime");

  @Override
  protected String getTableName() {
    return "compressionStatsOfflineTest";
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
  protected TableConfig createOfflineTableConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .build();

    // Enable compression stats tracking
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setCompressionStatsEnabled(true);

    return tableConfig;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    // Start the Pinot cluster
    startZk();
    startController();
    startBroker();
    startServer();

    // Create and upload the schema and table config
    Schema schema = createSchema();
    addSchema(schema);
    TableConfig tableConfig = createOfflineTableConfig();
    addTableConfig(tableConfig);

    // Unpack Avro data, build segments, and upload
    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, _segmentDir, _tarDir);
    uploadSegments(getTableName(), _tarDir);

    // Wait for all documents to be loaded
    waitForAllDocsLoaded(600_000L);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    String offlineTableName =
        org.apache.pinot.spi.utils.builder.TableNameBuilder.OFFLINE.tableNameWithType(getTableName());
    dropOfflineTable(getTableName());
    waitForTableDataManagerRemoved(offlineTableName);
    waitForEVToDisappear(offlineTableName);
    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testCompressionStatsInTableSizeApi()
      throws Exception {
    // Call the controller table size API
    String response = sendGetRequest(
        controllerUrl("/tables/" + getTableName() + "/size"));
    JsonNode tableSizeJson = JsonUtils.stringToJsonNode(response);

    // Verify top-level structure
    assertNotNull(tableSizeJson.get("tableName"), "Response should have tableName");
    assertTrue(tableSizeJson.get("reportedSizeInBytes").asLong() > 0,
        "reportedSizeInBytes should be > 0");

    // Get offline segment details
    JsonNode offlineSegments = tableSizeJson.get("offlineSegments");
    assertNotNull(offlineSegments, "offlineSegments should be present");

    // Verify compression stats are nested under compressionStats object
    JsonNode compressionStatsNode = offlineSegments.get("compressionStats");
    assertNotNull(compressionStatsNode, "compressionStats should be present");

    long rawFwdIndexSize = compressionStatsNode.get("rawIngestSizePerReplicaInBytes").asLong();
    long compressedFwdIndexSize = compressionStatsNode.get("onDiskSizePerReplicaInBytes").asLong();
    double compressionRatio = compressionStatsNode.get("compressionRatio").asDouble();
    int segmentsWithStats = compressionStatsNode.get("segmentsWithStats").asInt();
    int totalSegments = compressionStatsNode.get("totalSegments").asInt();
    boolean isPartialCoverage = compressionStatsNode.get("isPartialCoverage").asBoolean();

    // Raw forward index size should be > 0 (we have 4 raw columns across 12 segments)
    assertTrue(rawFwdIndexSize > 0,
        "rawIngestSizePerReplicaInBytes should be > 0, got: " + rawFwdIndexSize);

    // On-disk forward index size should be > 0
    assertTrue(compressedFwdIndexSize > 0,
        "onDiskSizePerReplicaInBytes should be > 0, got: " + compressedFwdIndexSize);

    // Compression ratio should be > 0 (raw / compressed)
    assertTrue(compressionRatio > 0,
        "compressionRatio should be > 0, got: " + compressionRatio);

    // Raw size should be >= compressed size (compression should not expand data for numeric columns)
    assertTrue(rawFwdIndexSize >= compressedFwdIndexSize,
        "rawIngestSize (" + rawFwdIndexSize + ") should be >= onDiskSize ("
            + compressedFwdIndexSize + ")");

    // Compression ratio = raw / compressed, should be >= 1.0
    assertTrue(compressionRatio >= 1.0,
        "compressionRatio should be >= 1.0, got: " + compressionRatio);

    // All 12 segments should have compression stats since compressionStatsEnabled=true
    assertEquals(totalSegments, 12, "totalSegments should be 12");
    assertEquals(segmentsWithStats, 12,
        "segmentsWithStats should equal totalSegments (all segments built with stats enabled)");

    // No partial coverage since all segments have stats
    assertFalse(isPartialCoverage,
        "isPartialCoverage should be false when all segments have stats");
  }

  @Test
  public void testPerSegmentCompressionStats()
      throws Exception {
    // Call table size API with verbose=true (default) to get per-segment details
    String response = sendGetRequest(
        controllerUrl("/tables/" + getTableName() + "/size?verbose=true"));
    JsonNode tableSizeJson = JsonUtils.stringToJsonNode(response);

    JsonNode offlineSegments = tableSizeJson.get("offlineSegments");
    JsonNode segments = offlineSegments.get("segments");
    assertNotNull(segments, "segments map should be present in verbose response");

    // Each segment should have server info with compression stats
    int segmentsChecked = 0;
    var fieldNames = segments.fieldNames();
    while (fieldNames.hasNext()) {
      String segmentName = fieldNames.next();
      JsonNode segmentDetails = segments.get(segmentName);
      JsonNode serverInfo = segmentDetails.get("serverInfo");
      assertNotNull(serverInfo, "serverInfo should be present for segment: " + segmentName);

      // Check each server's response for this segment
      var serverNames = serverInfo.fieldNames();
      while (serverNames.hasNext()) {
        String serverName = serverNames.next();
        JsonNode sizeInfo = serverInfo.get(serverName);
        long diskSize = sizeInfo.get("diskSizeInBytes").asLong();
        if (diskSize > 0) {
          // Segment should have raw and compressed forward index sizes
          long rawSize = sizeInfo.get("rawIngestSizeBytes").asLong();
          long compressedSize = sizeInfo.get("onDiskSizeBytes").asLong();
          assertTrue(rawSize > 0,
              "rawIngestSizeBytes should be > 0 for segment " + segmentName);
          assertTrue(compressedSize > 0,
              "onDiskSizeBytes should be > 0 for segment " + segmentName);

          // Verify per-column compression stats if present
          JsonNode columnStats = sizeInfo.get("columnCompressionStats");
          if (columnStats != null && !columnStats.isNull()) {
            // At least some of our RAW_COLUMNS should appear
            int columnsWithStats = 0;
            for (String col : RAW_COLUMNS) {
              if (columnStats.has(col)) {
                JsonNode colInfo = columnStats.get(col);
                assertTrue(colInfo.get("rawIngestSizeInBytes").asLong() > 0,
                    "Per-column raw ingest size should be > 0 for " + col);
                assertTrue(colInfo.get("onDiskSizeInBytes").asLong() > 0,
                    "Per-column on-disk size should be > 0 for " + col);
                assertTrue(colInfo.get("compressionRatio").asDouble() > 0,
                    "Per-column compression ratio should be > 0 for " + col);
                assertEquals(colInfo.get("codec").asText(), "LZ4",
                    "Compression codec should be LZ4 for " + col);
                // hasDictionary is no longer emitted — raw columns are identified by their codec value
                assertFalse(colInfo.has("hasDictionary"),
                    "hasDictionary field should no longer be present in the response for " + col);
                columnsWithStats++;
              }
            }
            assertTrue(columnsWithStats > 0,
                "At least one raw column should have compression stats in segment " + segmentName);
          }
        }
      }
      segmentsChecked++;
    }
    assertTrue(segmentsChecked > 0, "Should have checked at least one segment");
  }

  @Test
  public void testCompressionStatsDisabledTable()
      throws Exception {
    // Create a second table WITHOUT compressionStatsEnabled
    String noStatsTableName = "compressionStatsDisabledTest";
    Schema schema = createSchema();
    schema.setSchemaName(noStatsTableName);
    addSchema(schema);

    TableConfig noStatsConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(noStatsTableName)
        .setTimeColumnName(getTimeColumnName())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .build();
    // compressionStatsEnabled defaults to false — do NOT set it

    addTableConfig(noStatsConfig);

    // Build and upload segments for the no-stats table
    File noStatsSegmentDir = new File(_tempDir, "noStatsSegmentDir");
    File noStatsTarDir = new File(_tempDir, "noStatsTarDir");
    TestUtils.ensureDirectoriesExistAndEmpty(noStatsSegmentDir, noStatsTarDir);

    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, noStatsConfig, schema, 0,
        noStatsSegmentDir, noStatsTarDir);
    uploadSegments(noStatsTableName, noStatsTarDir);

    // Wait for docs to load
    TestUtils.waitForCondition(aVoid -> {
      try {
        return getCurrentCountStarResult(noStatsTableName) == DEFAULT_COUNT_STAR_RESULT;
      } catch (Exception e) {
        return false;
      }
    }, 600_000L, "Failed to load documents for no-stats table");

    try {
      // Query table size
      String response = sendGetRequest(
          controllerUrl("/tables/" + noStatsTableName + "/size"));
      JsonNode tableSizeJson = JsonUtils.stringToJsonNode(response);

      JsonNode offlineSegments = tableSizeJson.get("offlineSegments");
      assertNotNull(offlineSegments);

      // compressionStats should be absent (null/suppressed) since compressionStatsEnabled was false
      JsonNode compressionStatsNode = offlineSegments.get("compressionStats");
      assertNull(compressionStatsNode,
          "compressionStats should be absent when compressionStatsEnabled is false");

      // storageBreakdown should still be present (always reported regardless of flag)
      JsonNode storageBreakdown = offlineSegments.get("storageBreakdown");
      assertNotNull(storageBreakdown, "storageBreakdown should be present even when flag is off");
    } finally {
      // Clean up the second table even if assertions fail
      dropOfflineTable(noStatsTableName);
    }
  }
}
