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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.sampler.TableSamplerConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test(suiteName = "CustomClusterIntegrationTest")
public class TableSamplerIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private static final int DAYS = 7;
  private static final int SEGMENTS_PER_DAY = 10;
  private static final int RECORDS_PER_SEGMENT = 1;
  private static final int BASE_DAY = 20000;

  private static final String DAYS_SINCE_EPOCH_COL = "DaysSinceEpoch";
  private static final String PARTITION_KEY_COL = "PartitionKey";

  @Override
  public String getTableName() {
    return "TableSamplerIntegrationTest";
  }

  @Override
  protected long getCountStarResult() {
    return (long) DAYS * SEGMENTS_PER_DAY * RECORDS_PER_SEGMENT;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addDateTime(DAYS_SINCE_EPOCH_COL, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addSingleValueDimension(PARTITION_KEY_COL, FieldSpec.DataType.INT)
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    Map<String, ColumnPartitionConfig> columnPartitionConfigMap = new HashMap<>();
    columnPartitionConfigMap.put(PARTITION_KEY_COL, new ColumnPartitionConfig("Modulo", 2));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setTimeColumnName(DAYS_SINCE_EPOCH_COL)
        .setTimeType("DAYS")
        .setSegmentPartitionConfig(new SegmentPartitionConfig(columnPartitionConfigMap))
        .setRoutingConfig(
            new RoutingConfig(null, List.of(RoutingConfig.PARTITION_SEGMENT_PRUNER_TYPE), null, null))
        .build();
    tableConfig.setTableSamplers(List.of(
        new TableSamplerConfig("firstOnly", "firstN", Map.of("numSegments", "1")),
        new TableSamplerConfig("firstTwo", "firstN", Map.of("numSegments", "2"))));
    return tableConfig;
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    var fieldAssembler = SchemaBuilder.record("myRecord").fields();
    fieldAssembler.name(DAYS_SINCE_EPOCH_COL).type().intType().noDefault();
    fieldAssembler.name(PARTITION_KEY_COL).type().intType().noDefault();
    var avroSchema = fieldAssembler.endRecord();

    List<File> files = new ArrayList<>();
    for (int day = 0; day < DAYS; day++) {
      int dayValue = BASE_DAY + day;
      int partitionKey = day % 2;
      for (int seg = 0; seg < SEGMENTS_PER_DAY; seg++) {
        File avroFile = new File(_tempDir, "data_day_" + day + "_seg_" + seg + ".avro");
        try (DataFileWriter<GenericData.Record> fileWriter =
            new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
          fileWriter.create(avroSchema, avroFile);
          for (int docId = 0; docId < RECORDS_PER_SEGMENT; docId++) {
            GenericData.Record record = new GenericData.Record(avroSchema);
            record.put(DAYS_SINCE_EPOCH_COL, dayValue);
            record.put(PARTITION_KEY_COL, partitionKey);
            fileWriter.append(record);
          }
        }
        files.add(avroFile);
      }
    }
    return files;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFirstNSamplerForGroupByDay(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode full = postQuery("SELECT DaysSinceEpoch, COUNT(*) AS cnt FROM " + getTableName()
        + " GROUP BY DaysSinceEpoch ORDER BY DaysSinceEpoch");
    JsonNode fullRows = full.path("resultTable").path("rows");
    Assert.assertEquals(fullRows.size(), DAYS);
    for (int i = 0; i < DAYS; i++) {
      Assert.assertEquals(fullRows.get(i).get(0).asInt(), BASE_DAY + i);
      Assert.assertEquals(fullRows.get(i).get(1).asLong(), (long) SEGMENTS_PER_DAY * RECORDS_PER_SEGMENT);
    }
    Assert.assertEquals(full.path("numSegmentsQueried").asInt(), DAYS * SEGMENTS_PER_DAY);

    JsonNode sampled = postQuery(withSampler("SELECT DaysSinceEpoch, COUNT(*) AS cnt FROM " + getTableName()
        + " GROUP BY DaysSinceEpoch ORDER BY DaysSinceEpoch", "firstOnly"));
    JsonNode sampledRows = sampled.path("resultTable").path("rows");
    Assert.assertEquals(sampledRows.size(), 1);
    Assert.assertEquals(sampledRows.get(0).get(0).asInt(), BASE_DAY);
    Assert.assertEquals(sampledRows.get(0).get(1).asLong(), (long) RECORDS_PER_SEGMENT);
    Assert.assertEquals(sampled.path("numSegmentsQueried").asInt(), 1);

    JsonNode sampledTwo = postQuery(withSampler("SELECT DaysSinceEpoch, COUNT(*) AS cnt FROM " + getTableName()
        + " GROUP BY DaysSinceEpoch ORDER BY DaysSinceEpoch", "firstTwo"));
    JsonNode sampledTwoRows = sampledTwo.path("resultTable").path("rows");
    long sampledTwoCount = 0L;
    for (JsonNode row : sampledTwoRows) {
      sampledTwoCount += row.get(1).asLong();
    }
    Assert.assertEquals(sampledTwoCount, 2L * RECORDS_PER_SEGMENT);
    Assert.assertEquals(sampledTwo.path("numSegmentsQueried").asInt(), 2);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSamplerRoutingStillAppliesPartitionPruning(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    JsonNode full = postQuery("SELECT COUNT(*) AS cnt FROM " + getTableName() + " WHERE " + PARTITION_KEY_COL + " = 1");
    Assert.assertEquals(full.path("resultTable").path("rows").get(0).get(0).asLong(),
        3L * SEGMENTS_PER_DAY * RECORDS_PER_SEGMENT);

    JsonNode sampled = postQuery(
        withSampler("SELECT COUNT(*) AS cnt FROM " + getTableName() + " WHERE " + PARTITION_KEY_COL + " = 1",
            "firstTwo"));
    Assert.assertEquals(sampled.path("resultTable").path("rows").get(0).get(0).asLong(), 0L);
    if (!useMultiStageQueryEngine) {
      Assert.assertEquals(sampled.path("numSegmentsQueried").asInt(), 0);
    }
  }

  private static String withSampler(String query, String samplerName) {
    return "SET sampler='" + samplerName + "'; " + query;
  }
}
