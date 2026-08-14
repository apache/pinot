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
}
