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
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
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
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setTimeColumnName(DAYS_SINCE_EPOCH_COL)
        .setTimeType("DAYS")
        .build();
    tableConfig.setTableSamplers(List.of(
        new TableSamplerConfig("perDay", "timeBucket", Map.of("numSegmentsPerDay", "1"))));
    return tableConfig;
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(DAYS_SINCE_EPOCH_COL,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null)));

    List<File> files = new ArrayList<>();
    for (int day = 0; day < DAYS; day++) {
      int dayValue = BASE_DAY + day;
      for (int seg = 0; seg < SEGMENTS_PER_DAY; seg++) {
        File avroFile = new File(_tempDir, "data_day_" + day + "_seg_" + seg + ".avro");
        try (DataFileWriter<GenericData.Record> fileWriter =
            new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
          fileWriter.create(avroSchema, avroFile);
          for (int docId = 0; docId < RECORDS_PER_SEGMENT; docId++) {
            GenericData.Record record = new GenericData.Record(avroSchema);
            record.put(DAYS_SINCE_EPOCH_COL, dayValue);
            fileWriter.append(record);
          }
        }
        files.add(avroFile);
      }
    }
    return files;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testTimeBucketSamplerForGroupByDay(boolean useMultiStageQueryEngine)
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
    Assert.assertEquals(Integer.parseInt(full.path("numSegmentsQueried").asText()), DAYS * SEGMENTS_PER_DAY);

    JsonNode sampled = postQueryWithOptions("SELECT DaysSinceEpoch, COUNT(*) AS cnt FROM " + getTableName()
            + " GROUP BY DaysSinceEpoch ORDER BY DaysSinceEpoch",
        "tableSampler=perDay");
    JsonNode sampledRows = sampled.path("resultTable").path("rows");
    Assert.assertEquals(sampledRows.size(), DAYS);
    for (int i = 0; i < DAYS; i++) {
      Assert.assertEquals(sampledRows.get(i).get(0).asInt(), BASE_DAY + i);
      Assert.assertEquals(sampledRows.get(i).get(1).asLong(), (long) RECORDS_PER_SEGMENT);
    }
    Assert.assertEquals(Integer.parseInt(sampled.path("numSegmentsQueried").asText()), DAYS);
  }
}
