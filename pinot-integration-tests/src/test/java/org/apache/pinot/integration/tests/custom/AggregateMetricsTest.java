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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Integration test that enables aggregate metrics for the LLC real-time table.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class AggregateMetricsTest extends CustomDataQueryClusterIntegrationTest {

  private static final long EXPECTED_SUM_AIR_TIME = -165429728L;
  private static final long EXPECTED_SUM_ARR_DELAY = -175625957L;

  @Override
  public String getTableName() {
    return "AggregateMetricsTest";
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension("Carrier", DataType.STRING)
        .addSingleValueDimension("Origin", DataType.STRING)
        .addMetric("AirTime", DataType.LONG)
        .addMetric("ArrDelay", DataType.DOUBLE)
        .addDateTime("DaysSinceEpoch", DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .build();
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
    indexingConfig.setSortedColumn(Collections.singletonList("Carrier"));
    indexingConfig.setInvertedIndexColumns(Collections.singletonList("Origin"));
    indexingConfig.setNoDictionaryColumns(Arrays.asList("AirTime", "ArrDelay"));
    indexingConfig.setRangeIndexColumns(Collections.singletonList("DaysSinceEpoch"));
    indexingConfig.setBloomFilterColumns(Collections.singletonList("Origin"));
    indexingConfig.setAggregateMetrics(true);
    return tableConfig;
  }

  @Nullable
  @Override
  protected String getSortedColumn() {
    return null;
  }

  @Override
  public String getTimeColumnName() {
    return "DaysSinceEpoch";
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs) {
    // For aggregate metrics, documents can be merged during ingestion, so we check aggregation results
    // instead of document count.
    String sql = "SELECT SUM(AirTime), SUM(ArrDelay) FROM " + getTableName();
    TestUtils.waitForCondition(aVoid -> {
      try {
        JsonNode queryResult = postQuery(sql);
        JsonNode aggregationResults = queryResult.get("resultTable").get("rows").get(0);
        return aggregationResults.get(0).asLong() == EXPECTED_SUM_AIR_TIME
            && aggregationResults.get(1).asLong() == EXPECTED_SUM_ARR_DELAY;
      } catch (Exception e) {
        return null;
      }
    }, 100L, timeoutMs, "Failed to load all documents");
  }

  @Test
  public void testAggregateMetricsQueries()
      throws Exception {
    // Test total aggregation
    JsonNode result = postQuery("SELECT SUM(AirTime), SUM(ArrDelay) FROM " + getTableName());
    JsonNode rows = result.get("resultTable").get("rows").get(0);
    assertEquals(rows.get(0).asLong(), EXPECTED_SUM_AIR_TIME);
    assertEquals(rows.get(1).asLong(), EXPECTED_SUM_ARR_DELAY);

    // Test group by with order
    result = postQuery("SELECT SUM(AirTime), DaysSinceEpoch FROM " + getTableName()
        + " GROUP BY DaysSinceEpoch ORDER BY SUM(AirTime) DESC LIMIT 1");
    assertEquals(result.get("exceptions").size(), 0);

    // Test filter with group by
    result = postQuery("SELECT Origin, SUM(ArrDelay) FROM " + getTableName()
        + " WHERE Carrier = 'AA' GROUP BY Origin ORDER BY Origin LIMIT 10");
    assertEquals(result.get("exceptions").size(), 0);
  }
}
