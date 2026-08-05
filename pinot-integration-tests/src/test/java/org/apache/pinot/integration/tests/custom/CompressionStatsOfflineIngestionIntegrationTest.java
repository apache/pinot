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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Integration test that validates compression stats tracking end-to-end for offline batch ingestion.
///
/// Creates an offline table with `compressionStatsEnabled=true`, ingests data from Avro files with several raw
/// (no-dictionary) columns using LZ4 compression, and then verifies that the controller's
/// `GET /tables/{table}/size` API response includes valid uncompressed-value sizes, forward-index and dictionary
/// storage sizes, compression ratios, and segment coverage.
@Test(suiteName = "CustomClusterIntegrationTest")
public class CompressionStatsOfflineIngestionIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  // Raw columns that will have compression stats tracked.
  // These are metric/dimension columns from the default On_Time schema that support raw encoding.
  private static final List<String> RAW_COLUMNS =
      List.of("ActualElapsedTime", "ArrDelay", "DepDelay", "CRSDepTime");

  @Override
  public String getTableName() {
    return "compressionStatsOfflineTest";
  }

  @Override
  public String getTimeColumnName() {
    return "DaysSinceEpoch";
  }

  @Override
  protected long getCountStarResult() {
    return DEFAULT_COUNT_STAR_RESULT;
  }

  @Override
  public Schema createSchema() {
    try {
      Schema schema = createSchema(getSchemaFileName());
      schema.setSchemaName(getTableName());
      return schema;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    return unpackAvroData(_tempDir);
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
  public TableConfig createOfflineTableConfig() {
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

  @Test
  public void testCompressionStatsInTableSizeApi()
      throws Exception {
    // Call the controller table size API
    String response = sendGetRequest(
        "http://localhost:" + getControllerPort() + "/tables/" + getTableName() + "/size");
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

    long uncompressedValueSizePerReplicaInBytes =
        compressionStatsNode.get("uncompressedValueSizePerReplicaInBytes").asLong();
    long forwardIndexAndDictionaryStorageSizePerReplicaInBytes =
        compressionStatsNode.get("forwardIndexAndDictionaryStorageSizePerReplicaInBytes").asLong();
    double compressionRatio = compressionStatsNode.get("compressionRatio").asDouble();
    int segmentsWithCompleteStats = compressionStatsNode.get("segmentsWithCompleteStats").asInt();
    int totalSegments = compressionStatsNode.get("totalSegments").asInt();
    boolean partialCoverage = compressionStatsNode.get("partialCoverage").asBoolean();

    // Uncompressed value size should be > 0 (we have 4 raw columns across 12 segments).
    assertTrue(uncompressedValueSizePerReplicaInBytes > 0,
        "uncompressedValueSizePerReplicaInBytes should be > 0, got: " + uncompressedValueSizePerReplicaInBytes);

    // Forward-index and dictionary storage size should be > 0.
    assertTrue(forwardIndexAndDictionaryStorageSizePerReplicaInBytes > 0,
        "forwardIndexAndDictionaryStorageSizePerReplicaInBytes should be > 0, got: "
            + forwardIndexAndDictionaryStorageSizePerReplicaInBytes);

    assertEquals(compressionRatio,
        (double) uncompressedValueSizePerReplicaInBytes / forwardIndexAndDictionaryStorageSizePerReplicaInBytes, 1e-9,
        "compressionRatio should equal uncompressedValueSize / forwardIndexAndDictionaryStorageSize");

    // Uncompressed value size should be >= encoded value storage size for these numeric columns.
    assertTrue(uncompressedValueSizePerReplicaInBytes >= forwardIndexAndDictionaryStorageSizePerReplicaInBytes,
        "uncompressedValueSize (" + uncompressedValueSizePerReplicaInBytes
            + ") should be >= forwardIndexAndDictionaryStorageSize ("
            + forwardIndexAndDictionaryStorageSizePerReplicaInBytes + ")");

    // Compression ratio = raw / compressed, should be >= 1.0
    assertTrue(compressionRatio >= 1.0,
        "compressionRatio should be >= 1.0, got: " + compressionRatio);

    // All 12 segments should have compression stats since compressionStatsEnabled=true
    assertEquals(totalSegments, 12, "totalSegments should be 12");
    assertEquals(segmentsWithCompleteStats, 12,
        "segmentsWithCompleteStats should equal totalSegments (all segments built with stats enabled)");

    // No partial coverage since all segments have stats
    assertFalse(partialCoverage,
        "partialCoverage should be false when all segments have stats");
  }

  @Test
  public void testPerSegmentCompressionStats()
      throws Exception {
    // Call table size API with verbose=true (default) to get per-segment details
    String response = sendGetRequest(
        "http://localhost:" + getControllerPort() + "/tables/" + getTableName()
            + "/size?verbose=true&includeColumnCompressionStats=true");
    JsonNode tableSizeJson = JsonUtils.stringToJsonNode(response);

    JsonNode offlineSegments = tableSizeJson.get("offlineSegments");
    JsonNode segments = offlineSegments.get("segments");
    assertNotNull(segments, "segments map should be present in verbose response");

    // Each segment should have server info with compression stats
    int segmentsChecked = 0;
    int serverStatsChecked = 0;
    int columnStatsChecked = 0;
    var fieldNames = segments.fieldNames();
    while (fieldNames.hasNext()) {
      String segmentName = fieldNames.next();
      JsonNode segmentDetails = segments.get(segmentName);
      JsonNode serverInfo = segmentDetails.get("serverInfo");
      assertNotNull(serverInfo, "serverInfo should be present for segment: " + segmentName);

      // The bounded controller fan-out selects one complete replica and attaches stats only to that server entry.
      boolean selectedReplicaFound = false;
      var serverNames = serverInfo.fieldNames();
      while (serverNames.hasNext()) {
        String serverName = serverNames.next();
        JsonNode sizeInfo = serverInfo.get(serverName);
        long diskSize = sizeInfo.get("diskSizeInBytes").asLong();
        if (sizeInfo.has("compressionStatsUncompressedValueSizeInBytes")) {
          assertTrue(diskSize > 0, "The selected stats-bearing replica should also report segment size");
          assertTrue(sizeInfo.has("compressionStatsForwardIndexAndDictionaryStorageSizeInBytes"),
              "Server info should have compressionStatsForwardIndexAndDictionaryStorageSizeInBytes for segment "
                  + segmentName);
          // Segment should have uncompressed-value and encoded-storage sizes.
          long rawSize = sizeInfo.get("compressionStatsUncompressedValueSizeInBytes").asLong();
          long compressedSize = sizeInfo.get("compressionStatsForwardIndexAndDictionaryStorageSizeInBytes").asLong();
          assertTrue(rawSize > 0,
              "compressionStatsUncompressedValueSizeInBytes should be > 0 for segment " + segmentName);
          assertTrue(compressedSize > 0,
              "compressionStatsForwardIndexAndDictionaryStorageSizeInBytes should be > 0 for segment " + segmentName);
          serverStatsChecked++;

          // Verify all requested raw columns have compression statistics.
          JsonNode columnStats = sizeInfo.get("columnCompressionStats");
          assertNotNull(columnStats, "columnCompressionStats should be present for segment " + segmentName);
          for (String col : RAW_COLUMNS) {
            assertTrue(columnStats.has(col), "Missing compression stats for raw column " + col);
            JsonNode colInfo = columnStats.get(col);
            long columnUncompressedSize = colInfo.get("uncompressedValueSizeInBytes").asLong();
            long columnStorageSize = colInfo.get("forwardIndexAndDictionaryStorageSizeInBytes").asLong();
            assertTrue(columnUncompressedSize > 0,
                "Per-column uncompressed value size should be > 0 for " + col);
            assertTrue(columnStorageSize > 0,
                "Per-column forward-index and dictionary storage size should be > 0 for " + col);
            assertEquals(colInfo.get("compressionRatio").asDouble(),
                (double) columnUncompressedSize / columnStorageSize, 1e-9,
                "Per-column compression ratio should equal uncompressed size / storage size for " + col);
            JsonNode encoding = colInfo.get("encodingBreakdown").get(0);
            assertEquals(encoding.get("encoding").asText(), "RAW");
            assertEquals(encoding.get("chunkCompressionType").asText(), "LZ4",
                "Chunk compression type should be LZ4 for " + col);
            // Encoding is represented by encodingBreakdown; the old hasDictionary field is not emitted.
            assertFalse(colInfo.has("hasDictionary"),
                "hasDictionary field should no longer be present in the response for " + col);
            columnStatsChecked++;
          }
          selectedReplicaFound = true;
        }
      }
      assertTrue(selectedReplicaFound, "Each complete segment should expose one selected stats-bearing replica");
      segmentsChecked++;
    }
    assertTrue(segmentsChecked > 0, "Should have checked at least one segment");
    assertTrue(serverStatsChecked > 0, "At least one immutable segment must expose compression statistics");
    assertTrue(columnStatsChecked > 0, "At least one raw column must expose compression statistics");
  }

  @Test
  public void testColumnCompressionStatsAreOptIn()
      throws Exception {
    String sizeApi = "http://localhost:" + getControllerPort() + "/tables/" + getTableName()
        + "/size?verbose=true";
    assertSizeColumnStatsPresence(JsonUtils.stringToJsonNode(sendGetRequest(sizeApi)), false);
    assertSizeColumnStatsPresence(
        JsonUtils.stringToJsonNode(sendGetRequest(sizeApi + "&includeColumnCompressionStats=true")), true);

    String metadataApi = "http://localhost:" + getControllerPort() + "/tables/" + getTableName()
        + "/metadata?type=OFFLINE";
    JsonNode metadataWithoutColumns = JsonUtils.stringToJsonNode(sendGetRequest(metadataApi));
    assertNotNull(metadataWithoutColumns.get("compressionStats"));
    assertFalse(metadataWithoutColumns.has("columnCompressionStats"));

    JsonNode metadataWithColumns =
        JsonUtils.stringToJsonNode(sendGetRequest(metadataApi + "&includeColumnCompressionStats=true"));
    assertNotNull(metadataWithColumns.get("compressionStats"));
    JsonNode columnCompressionStats = metadataWithColumns.get("columnCompressionStats");
    assertNotNull(columnCompressionStats);
    assertTrue(columnCompressionStats.isArray());
    assertTrue(columnCompressionStats.size() > 0);

    String filteredColumn = RAW_COLUMNS.get(0);
    JsonNode filteredMetadata = JsonUtils.stringToJsonNode(sendGetRequest(
        metadataApi + "&columns=" + filteredColumn + "&includeColumnCompressionStats=true"));
    JsonNode filteredColumnStats = filteredMetadata.get("columnCompressionStats");
    assertNotNull(filteredColumnStats);
    assertEquals(filteredColumnStats.size(), 1);
    assertEquals(filteredColumnStats.get(0).get("column").asText(), filteredColumn);
  }

  private static void assertSizeColumnStatsPresence(JsonNode tableSizeJson, boolean expected) {
    JsonNode segments = tableSizeJson.get("offlineSegments").get("segments");
    int statsBearingReplicas = 0;
    var segmentNames = segments.fieldNames();
    while (segmentNames.hasNext()) {
      JsonNode serverInfo = segments.get(segmentNames.next()).get("serverInfo");
      var serverNames = serverInfo.fieldNames();
      while (serverNames.hasNext()) {
        JsonNode sizeInfo = serverInfo.get(serverNames.next());
        if (sizeInfo.has("compressionStatsUncompressedValueSizeInBytes")) {
          assertEquals(sizeInfo.has("columnCompressionStats"), expected);
          statsBearingReplicas++;
        }
      }
    }
    assertTrue(statsBearingReplicas > 0, "The opt-in check must inspect at least one stats-bearing replica");
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
    // compressionStatsEnabled defaults to false; do NOT set it

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
          "http://localhost:" + getControllerPort() + "/tables/" + noStatsTableName + "/size");
      JsonNode tableSizeJson = JsonUtils.stringToJsonNode(response);

      JsonNode offlineSegments = tableSizeJson.get("offlineSegments");
      assertNotNull(offlineSegments);

      // compressionStats should be absent (null/suppressed) since compressionStatsEnabled was false
      JsonNode compressionStatsNode = offlineSegments.get("compressionStats");
      assertNull(compressionStatsNode,
          "compressionStats should be absent when compressionStatsEnabled is false");

      // Enable tracking after the original segments were built, then add one newly tracked segment. This models a
      // rolling table upgrade where legacy and current segments coexist.
      noStatsConfig.getIndexingConfig().setCompressionStatsEnabled(true);
      updateTableConfig(noStatsConfig);

      File trackedSegmentDir = new File(_tempDir, "trackedSegmentDir");
      File trackedTarDir = new File(_tempDir, "trackedTarDir");
      TestUtils.ensureDirectoriesExistAndEmpty(trackedSegmentDir, trackedTarDir);
      int legacySegmentCount = avroFiles.size();
      ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles.subList(0, 1), noStatsConfig, schema,
          legacySegmentCount, trackedSegmentDir, trackedTarDir);
      uploadSegments(noStatsTableName, trackedTarDir);

      TestUtils.waitForCondition(aVoid -> {
        try {
          JsonNode stats = JsonUtils.stringToJsonNode(sendGetRequest(
              "http://localhost:" + getControllerPort() + "/tables/" + noStatsTableName + "/size"))
              .path("offlineSegments").path("compressionStats");
          return stats.path("totalSegments").asInt() == legacySegmentCount + 1
              && stats.path("segmentsWithCompleteStats").asInt() == 1;
        } catch (Exception e) {
          return false;
        }
      }, 600_000L, "Failed to observe mixed legacy and tracked compression-statistics coverage");

      JsonNode mixedCompressionStats = JsonUtils.stringToJsonNode(sendGetRequest(
          "http://localhost:" + getControllerPort() + "/tables/" + noStatsTableName + "/size"))
          .path("offlineSegments").path("compressionStats");
      assertEquals(mixedCompressionStats.get("totalSegments").asInt(), legacySegmentCount + 1);
      assertEquals(mixedCompressionStats.get("segmentsWithCompleteStats").asInt(), 1);
      assertTrue(mixedCompressionStats.get("partialCoverage").asBoolean());
      assertTrue(mixedCompressionStats.get("uncompressedValueSizePerReplicaInBytes").asLong() > 0);
      assertTrue(mixedCompressionStats.get("forwardIndexAndDictionaryStorageSizePerReplicaInBytes").asLong() > 0);
    } finally {
      // Clean up the second table even if assertions fail
      dropOfflineTable(noStatsTableName);
    }
  }
}
