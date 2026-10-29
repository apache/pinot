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
import java.util.List;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Integration test that validates per-index-type size tracking end-to-end for offline batch ingestion.
///
/// Creates an offline table with `indexSizeStatsEnabled=true`, ingests data from Avro files, and then verifies that
/// the controller's `GET /tables/{table}/size?includeIndexSizeStats=true` API response includes a per-index-type
/// `indexSizeBreakdown` with non-zero sizes, and that the breakdown is absent when the flag is not set.
@Test(suiteName = "CustomClusterIntegrationTest")
public class IndexSizeBreakdownOfflineIngestionIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  @Override
  public String getTableName() {
    return "indexSizeBreakdownOfflineTest";
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
  public TableConfig createOfflineTableConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setNumReplicas(getNumReplicas())
        .build();

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setIndexSizeStatsEnabled(true);

    return tableConfig;
  }

  @Test
  public void testIndexSizeBreakdownInTableSizeApi()
      throws Exception {
    String response = sendGetRequest(
        "http://localhost:" + getControllerPort() + "/tables/" + getTableName()
            + "/size?includeIndexSizeStats=true");
    JsonNode tableSizeJson = JsonUtils.stringToJsonNode(response);

    JsonNode offlineSegments = tableSizeJson.get("offlineSegments");
    assertNotNull(offlineSegments, "offlineSegments should be present");

    JsonNode breakdown = offlineSegments.get("indexSizeBreakdown");
    assertNotNull(breakdown, "indexSizeBreakdown should be present when includeIndexSizeStats=true");

    // The forward index exists for every column of every segment, so it must always appear.
    assertTrue(breakdown.has("forward_index"), "forward_index must appear in the breakdown");
    JsonNode forwardIndex = breakdown.get("forward_index");
    assertTrue(forwardIndex.get("sizePerReplicaInBytes").asLong() > 0,
        "forward_index sizePerReplicaInBytes should be > 0, got: "
            + forwardIndex.get("sizePerReplicaInBytes").asLong());
    assertTrue(forwardIndex.get("segmentsWithStats").asInt() > 0,
        "forward_index segmentsWithStats should be > 0");

    // The default table config also creates a dictionary for most columns.
    assertTrue(breakdown.has("dictionary"), "dictionary must appear in the breakdown");
    JsonNode dictionary = breakdown.get("dictionary");
    assertTrue(dictionary.get("sizePerReplicaInBytes").asLong() > 0,
        "dictionary sizePerReplicaInBytes should be > 0, got: " + dictionary.get("sizePerReplicaInBytes").asLong());
  }

  @Test
  public void testIndexSizeBreakdownIsOptIn()
      throws Exception {
    String response = sendGetRequest(
        "http://localhost:" + getControllerPort() + "/tables/" + getTableName() + "/size");
    JsonNode tableSizeJson = JsonUtils.stringToJsonNode(response);

    JsonNode offlineSegments = tableSizeJson.get("offlineSegments");
    assertNotNull(offlineSegments, "offlineSegments should be present");
    assertFalse(offlineSegments.has("indexSizeBreakdown"),
        "indexSizeBreakdown should be absent when includeIndexSizeStats is not requested");
  }

  /// Spec 13 end-to-end: the breakdown must track a reload, not stay frozen at ingestion time. The base table config
  /// configures no inverted index columns, so `DivActualElapsedTime` starts with none; adding one via table config
  /// and reloading must make `inverted_index` appear in the aggregated breakdown with a positive size, proving
  /// `SegmentPreProcessor`'s opportunistic refresh (unit-tested in `IndexSizeStatsTest`) also works end-to-end
  /// through the real reload API and the controller's size-aggregation path.
  @Test
  public void testIndexSizeBreakdownReflectsReload()
      throws Exception {
    JsonNode breakdownBeforeReload = getIndexSizeBreakdown();
    assertFalse(breakdownBeforeReload.has("inverted_index"),
        "Sanity: no inverted index column is configured yet, so inverted_index must not appear in the breakdown");

    TableConfig tableConfig = createOfflineTableConfig();
    tableConfig.getIndexingConfig().setInvertedIndexColumns(List.of("DivActualElapsedTime"));
    updateTableConfig(tableConfig);
    reloadOfflineTable(getTableName());

    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode breakdown = getIndexSizeBreakdown();
        return breakdown.has("inverted_index")
            && breakdown.get("inverted_index").get("sizePerReplicaInBytes").asLong() > 0;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }, 60_000L, "Reload did not refresh the index size breakdown with the newly added inverted index");

    JsonNode breakdownAfterReload = getIndexSizeBreakdown();
    assertTrue(breakdownAfterReload.has("forward_index"),
        "forward_index must still appear in the breakdown after a reload that only adds an inverted index");
    assertTrue(breakdownAfterReload.get("forward_index").get("sizePerReplicaInBytes").asLong() > 0,
        "forward_index size must still be positive after a reload that only adds an inverted index");
  }

  private JsonNode getIndexSizeBreakdown()
      throws IOException {
    String response = sendGetRequest(
        "http://localhost:" + getControllerPort() + "/tables/" + getTableName()
            + "/size?includeIndexSizeStats=true");
    JsonNode offlineSegments = JsonUtils.stringToJsonNode(response).get("offlineSegments");
    assertNotNull(offlineSegments, "offlineSegments should be present");
    JsonNode breakdown = offlineSegments.get("indexSizeBreakdown");
    assertNotNull(breakdown, "indexSizeBreakdown should be present when includeIndexSizeStats=true");
    return breakdown;
  }
}
