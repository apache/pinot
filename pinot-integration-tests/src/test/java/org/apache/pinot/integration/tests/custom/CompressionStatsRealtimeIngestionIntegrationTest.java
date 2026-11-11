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
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Integration test that validates compression stats tracking end-to-end for realtime (Kafka) ingestion.
///
/// Creates a realtime table with `compressionStatsEnabled=true`, pushes data from Avro files into Kafka with several
/// raw (no-dictionary) columns using LZ4 compression, waits for all documents to be consumed, and then verifies that
/// the controller's `GET /tables/{table}/size` API response includes valid compression statistics for the completed
/// (COMPLETED) segments.
@Test(suiteName = "CustomClusterIntegrationTest")
public class CompressionStatsRealtimeIngestionIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  // Raw columns that will have compression stats tracked.
  // These are metric/dimension columns from the default On_Time schema that support raw encoding.
  private static final List<String> RAW_COLUMNS =
      List.of("ActualElapsedTime", "ArrDelay", "DepDelay", "CRSDepTime");

  @Override
  public String getTableName() {
    return "compressionStatsRealtimeTest";
  }

  @Override
  public String getTimeColumnName() {
    return "DaysSinceEpoch";
  }

  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  protected long getCountStarResult() {
    return DEFAULT_COUNT_STAR_RESULT;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
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
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    TableConfig tableConfig = super.createRealtimeTableConfig(sampleAvroFile);

    // Enable compression stats tracking
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setCompressionStatsEnabled(true);

    return tableConfig;
  }

  @Test
  public void testCompressionStatsInTableSizeApiForRealtimeTable()
      throws Exception {
    // Call the controller table size API
    String response = sendGetRequest(
        "http://localhost:" + getControllerPort() + "/tables/" + getTableName() + "/size");
    JsonNode tableSizeJson = JsonUtils.stringToJsonNode(response);

    // Verify top-level structure
    assertNotNull(tableSizeJson.get("tableName"), "Response should have tableName");
    assertTrue(tableSizeJson.get("reportedSizeInBytes").asLong() >= 0,
        "reportedSizeInBytes should be >= 0");

    // Get realtime segment details
    JsonNode realtimeSegments = tableSizeJson.get("realtimeSegments");
    assertNotNull(realtimeSegments, "realtimeSegments should be present");

    // Verify compression stats are nested under compressionStats object
    JsonNode compressionStatsNode = realtimeSegments.get("compressionStats");
    assertNotNull(compressionStatsNode, "compressionStats should be present");
    assertTrue(compressionStatsNode.has("uncompressedValueSizePerReplicaInBytes"),
        "compressionStats should have uncompressedValueSizePerReplicaInBytes");
    assertTrue(compressionStatsNode.has("forwardIndexAndDictionaryStorageSizePerReplicaInBytes"),
        "compressionStats should have forwardIndexAndDictionaryStorageSizePerReplicaInBytes");
    assertTrue(compressionStatsNode.has("compressionRatio"),
        "compressionStats should have compressionRatio");
    assertTrue(compressionStatsNode.has("segmentsWithCompleteStats"),
        "compressionStats should have segmentsWithCompleteStats");
    assertTrue(compressionStatsNode.has("totalSegments"),
        "compressionStats should have totalSegments");

    long uncompressedValueSizePerReplicaInBytes =
        compressionStatsNode.get("uncompressedValueSizePerReplicaInBytes").asLong();
    long forwardIndexAndDictionaryStorageSizePerReplicaInBytes =
        compressionStatsNode.get("forwardIndexAndDictionaryStorageSizePerReplicaInBytes").asLong();
    double compressionRatio = compressionStatsNode.get("compressionRatio").asDouble();
    int segmentsWithCompleteStats = compressionStatsNode.get("segmentsWithCompleteStats").asInt();
    int totalSegments = compressionStatsNode.get("totalSegments").asInt();

    // Total segments should be > 0 (at least consuming segments exist)
    assertTrue(totalSegments > 0,
        "totalSegments should be > 0, got: " + totalSegments);

    assertTrue(segmentsWithCompleteStats > 0,
        "At least one committed realtime segment must contribute compression stats");
    assertTrue(uncompressedValueSizePerReplicaInBytes > 0,
        "uncompressedValueSizePerReplicaInBytes should be > 0 when segments have stats, got: "
            + uncompressedValueSizePerReplicaInBytes);
    assertTrue(forwardIndexAndDictionaryStorageSizePerReplicaInBytes > 0,
        "forwardIndexAndDictionaryStorageSizePerReplicaInBytes should be > 0 when segments have stats, got: "
            + forwardIndexAndDictionaryStorageSizePerReplicaInBytes);
    assertEquals(compressionRatio,
        (double) uncompressedValueSizePerReplicaInBytes / forwardIndexAndDictionaryStorageSizePerReplicaInBytes, 1e-9,
        "compressionRatio should equal uncompressedValueSize / forwardIndexAndDictionaryStorageSize");
    assertTrue(uncompressedValueSizePerReplicaInBytes >= forwardIndexAndDictionaryStorageSizePerReplicaInBytes,
        "uncompressedValueSize (" + uncompressedValueSizePerReplicaInBytes
            + ") should be >= forwardIndexAndDictionaryStorageSize ("
            + forwardIndexAndDictionaryStorageSizePerReplicaInBytes + ")");
    assertTrue(compressionRatio >= 1.0,
        "compressionRatio should be >= 1.0, got: " + compressionRatio);
  }

  @Test
  public void testPerSegmentCompressionStatsForRealtimeTable()
      throws Exception {
    // Call table size API with verbose=true to get per-segment details
    String response = sendGetRequest(
        "http://localhost:" + getControllerPort() + "/tables/" + getTableName() + "/size?verbose=true");
    JsonNode tableSizeJson = JsonUtils.stringToJsonNode(response);

    JsonNode realtimeSegments = tableSizeJson.get("realtimeSegments");
    assertNotNull(realtimeSegments, "realtimeSegments should be present");

    JsonNode segments = realtimeSegments.get("segments");
    assertNotNull(segments, "segments map should be present in verbose response");

    // At least one segment should exist
    assertTrue(segments.size() > 0, "Should have at least one segment");

    // Iterate segments and validate structure
    int segmentsChecked = 0;
    int statsBearingSegments = 0;
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
        // Only the immutable replica selected by bounded compression fan-out carries summary fields.
        if (sizeInfo.has("compressionStatsUncompressedValueSizeInBytes")) {
          assertTrue(diskSize > 0, "The selected stats-bearing replica should also report segment size");
          assertTrue(sizeInfo.has("compressionStatsForwardIndexAndDictionaryStorageSizeInBytes"),
              "Server info should have compressionStatsForwardIndexAndDictionaryStorageSizeInBytes for segment "
                  + segmentName);
          assertTrue(sizeInfo.get("compressionStatsUncompressedValueSizeInBytes").asLong() > 0,
              "Uncompressed value size should be positive for segment " + segmentName);
          assertTrue(sizeInfo.get("compressionStatsForwardIndexAndDictionaryStorageSizeInBytes").asLong() > 0,
              "Forward-index storage size should be positive for segment " + segmentName);
          statsBearingSegments++;
        }
      }
      segmentsChecked++;
    }
    assertTrue(segmentsChecked > 0, "Should have checked at least one segment");
    assertTrue(statsBearingSegments > 0, "At least one completed realtime segment must expose compression stats");
  }
}
