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
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Integration test that validates per-index-type size tracking end-to-end for realtime (Kafka) ingestion.
///
/// Creates a realtime table with `indexSizeStatsEnabled=true`, pushes data from Avro files into Kafka, waits for
/// segments to commit, and then verifies that the controller's `GET /tables/{table}/size?includeIndexSizeStats=true`
/// API response includes a per-index-type `indexSizeBreakdown` populated from committed segments. Collection happens
/// at seal time, so a consuming (uncommitted) segment must not contribute sizes.
@Test(suiteName = "CustomClusterIntegrationTest")
public class IndexSizeBreakdownRealtimeIngestionIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  @Override
  public String getTableName() {
    return "indexSizeBreakdownRealtimeTest";
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
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    TableConfig tableConfig = super.createRealtimeTableConfig(sampleAvroFile);

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    indexingConfig.setIndexSizeStatsEnabled(true);

    return tableConfig;
  }

  @Test
  public void testIndexSizeBreakdownInTableSizeApiForRealtimeTable()
      throws Exception {
    // Committed segments carry sizes collected at seal time; wait for at least one to commit and be discoverable.
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode breakdown = JsonUtils.stringToJsonNode(sendGetRequest(
            "http://localhost:" + getControllerPort() + "/tables/" + getTableName()
                + "/size?includeIndexSizeStats=true"))
            .path("realtimeSegments").path("indexSizeBreakdown");
        return breakdown.has("forward_index") && breakdown.path("forward_index").path("segmentsWithStats").asInt()
            > 0;
      } catch (Exception e) {
        return false;
      }
    }, 600_000L, "Failed to observe an indexSizeBreakdown contributed by a committed realtime segment");

    JsonNode tableSizeJson = JsonUtils.stringToJsonNode(sendGetRequest(
        "http://localhost:" + getControllerPort() + "/tables/" + getTableName()
            + "/size?includeIndexSizeStats=true"));

    JsonNode realtimeSegments = tableSizeJson.get("realtimeSegments");
    assertNotNull(realtimeSegments, "realtimeSegments should be present");

    JsonNode breakdown = realtimeSegments.get("indexSizeBreakdown");
    assertNotNull(breakdown, "indexSizeBreakdown should be present when includeIndexSizeStats=true");

    JsonNode forwardIndex = breakdown.get("forward_index");
    assertNotNull(forwardIndex, "forward_index must appear in the breakdown");
    assertTrue(forwardIndex.get("sizePerReplicaInBytes").asLong() > 0,
        "forward_index sizePerReplicaInBytes should be > 0, got: "
            + forwardIndex.get("sizePerReplicaInBytes").asLong());
    assertTrue(forwardIndex.get("segmentsWithStats").asInt() > 0,
        "forward_index segmentsWithStats should be > 0, since at least one segment has committed");
  }

  @Test
  public void testIndexSizeBreakdownIsOptInForRealtimeTable()
      throws Exception {
    JsonNode tableSizeJson = JsonUtils.stringToJsonNode(sendGetRequest(
        "http://localhost:" + getControllerPort() + "/tables/" + getTableName() + "/size"));

    JsonNode realtimeSegments = tableSizeJson.get("realtimeSegments");
    assertNotNull(realtimeSegments, "realtimeSegments should be present");
    assertFalse(realtimeSegments.has("indexSizeBreakdown"),
        "indexSizeBreakdown should be absent when includeIndexSizeStats is not requested");
  }
}
