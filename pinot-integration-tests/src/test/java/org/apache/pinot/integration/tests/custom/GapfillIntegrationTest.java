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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


/**
 * Integration test that validates gapfill results against a small, deterministic dataset.
 *
 * <p>Not thread-safe.</p>
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class GapfillIntegrationTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "GapfillIntegrationTest";
  private static final String ENTITY_ID_COLUMN = "entityId";
  private static final String VALUE_COLUMN = "value";
  private static final int NUM_RECORDS = 5;

  private static final long BASE_MS = 1633078800000L;
  private static final long START_MS = BASE_MS + 1000L;
  private static final long END_MS = BASE_MS + 7000L;

  private static final long[][] RAW_ROWS = new long[][]{
      {BASE_MS + 1000L, 1L, 10L},
      {BASE_MS + 3000L, 1L, 30L},
      {BASE_MS + 6000L, 1L, 60L},
      {BASE_MS + 2000L, 2L, 20L},
      {BASE_MS + 5000L, 2L, 50L}
  };

  private static final long[][] EXPECTED_GAPFILL_ROWS = new long[][]{
      {BASE_MS + 1000L, 1L, 10L},
      {BASE_MS + 2000L, 1L, 10L},
      {BASE_MS + 3000L, 1L, 30L},
      {BASE_MS + 4000L, 1L, 30L},
      {BASE_MS + 5000L, 1L, 30L},
      {BASE_MS + 6000L, 1L, 60L},
      {BASE_MS + 1000L, 2L, 0L},
      {BASE_MS + 2000L, 2L, 20L},
      {BASE_MS + 3000L, 2L, 20L},
      {BASE_MS + 4000L, 2L, 20L},
      {BASE_MS + 5000L, 2L, 50L},
      {BASE_MS + 6000L, 2L, 50L}
  };

  private static final long[][] EXPECTED_GAPFILL_SUM_ROWS = new long[][]{
      {BASE_MS + 1000L, 10L},
      {BASE_MS + 2000L, 30L},
      {BASE_MS + 3000L, 50L},
      {BASE_MS + 4000L, 50L},
      {BASE_MS + 5000L, 80L},
      {BASE_MS + 6000L, 110L}
  };

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_RECORDS;
  }

  @Override
  public int getNumAvroFiles() {
    return 1;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addDateTime(TIMESTAMP_FIELD_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .addSingleValueDimension(ENTITY_ID_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(VALUE_COLUMN, FieldSpec.DataType.INT)
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    Map<String, ColumnPartitionConfig> partitionConfig = new HashMap<>();
    partitionConfig.put(ENTITY_ID_COLUMN, new ColumnPartitionConfig("Modulo", 1));
    SegmentPartitionConfig segmentPartitionConfig = new SegmentPartitionConfig(partitionConfig);
    RoutingConfig routingConfig =
        new RoutingConfig(null, null, RoutingConfig.STRICT_REPLICA_GROUP_INSTANCE_SELECTOR_TYPE, null);

    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setSortedColumn(getSortedColumn())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setRangeIndexColumns(getRangeIndexColumns())
        .setBloomFilterColumns(getBloomFilterColumns())
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
        .setRoutingConfig(routingConfig)
        .setSegmentPartitionConfig(segmentPartitionConfig)
        .setOptimizeNoDictStatsCollection(true)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("gapfillRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(TIMESTAMP_FIELD_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null),
        new org.apache.avro.Schema.Field(ENTITY_ID_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(VALUE_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null)
    ));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (long[] rawRow : RAW_ROWS) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(TIMESTAMP_FIELD_NAME, rawRow[0]);
        record.put(ENTITY_ID_COLUMN, (int) rawRow[1]);
        record.put(VALUE_COLUMN, (int) rawRow[2]);
        writers.get(0).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test(dataProvider = "useV2QueryEngine")
  public void testGapfillAggregateQueryV2(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT time_col, SUM(status) AS occupied_slots_count "
                + "FROM ("
                + "SELECT GapFill(time_col, '1:MILLISECONDS:EPOCH', '%d', '%d', '1000:MILLISECONDS', "
                + "FILL(status, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(entityId)) AS time_col, "
                + "entityId, status "
                + "FROM ("
                + "SELECT DATETIMECONVERT(ts, '1:MILLISECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1000:MILLISECONDS') "
                + "AS time_col, entityId, MAX(value) AS status "
                + "FROM %s WHERE ts >= %d AND ts <= %d "
                + "GROUP BY 1, 2)"
                + ") "
                + "GROUP BY 1 LIMIT 20",
            START_MS, END_MS, getTableName(), START_MS, END_MS - 1000);
    JsonNode response = postQuery(query);
    assertNoError(response);

    JsonNode rows = response.get("resultTable").get("rows");
    assertNotNull(rows);

    List<long[]> actualRows = new ArrayList<>(rows.size());
    for (JsonNode row : rows) {
      actualRows.add(new long[]{row.get(0).asLong(), row.get(1).asLong()});
    }

    List<long[]> expectedRows = new ArrayList<>(EXPECTED_GAPFILL_SUM_ROWS.length);
    for (long[] row : EXPECTED_GAPFILL_SUM_ROWS) {
      expectedRows.add(new long[]{row[0], row[1]});
    }

    Comparator<long[]> rowComparator = Comparator.comparingLong(row -> row[0]);
    actualRows.sort(rowComparator);
    expectedRows.sort(rowComparator);

    assertEquals(actualRows.size(), expectedRows.size());
    for (int i = 0; i < expectedRows.size(); i++) {
      assertEquals(actualRows.get(i)[0], expectedRows.get(i)[0]);
      assertEquals(actualRows.get(i)[1], expectedRows.get(i)[1]);
    }
  }


  @Test(dataProvider = "useBothQueryEngines")
  public void testSimpleGapfillQuery(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT "
                + "GapFill(ts, '1:MILLISECONDS:EPOCH', '%d', '%d', '1000:MILLISECONDS', "
                + "FILL(value, 'FILL_PREVIOUS_VALUE'), TIMESERIESON(entityId)) AS time_col, "
                + "entityId, value "
                + "FROM %s WHERE ts >= %d AND ts <= %d LIMIT 20",
            START_MS, END_MS, getTableName(), START_MS, END_MS - 1000);
    JsonNode response = postQuery(query);
    assertNoError(response);

    JsonNode rows = response.get("resultTable").get("rows");
    assertNotNull(rows);

    List<long[]> actualRows = new ArrayList<>(rows.size());
    for (JsonNode row : rows) {
      actualRows.add(new long[]{row.get(0).asLong(), row.get(1).asLong(), row.get(2).asLong()});
    }

    List<long[]> expectedRows = new ArrayList<>(EXPECTED_GAPFILL_ROWS.length);
    for (long[] row : EXPECTED_GAPFILL_ROWS) {
      expectedRows.add(new long[]{row[0], row[1], row[2]});
    }

    Comparator<long[]> rowComparator =
        Comparator.<long[]>comparingLong(row -> row[1]).thenComparingLong(row -> row[0]);
    actualRows.sort(rowComparator);
    expectedRows.sort(rowComparator);

    assertEquals(actualRows.size(), expectedRows.size());
    for (int i = 0; i < expectedRows.size(); i++) {
      assertEquals(actualRows.get(i)[0], expectedRows.get(i)[0]);
      assertEquals(actualRows.get(i)[1], expectedRows.get(i)[1]);
      assertEquals(actualRows.get(i)[2], expectedRows.get(i)[2]);
    }
  }
}
