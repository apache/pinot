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
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.config.table.BloomFilterConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// End-to-end coverage for querying a UUID column backed by a Bloom filter. A single segment contains UUIDs ending in
/// `0000` and `0002`; the absent `0001` value is inside the segment min/max range, and the pruning metrics verify that
/// the Bloom value pruner eliminated it.
@Test(suiteName = "CustomClusterIntegrationTest")
public class UuidBloomFilterTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "UuidBloomFilterTest";
  private static final String UUID_COLUMN = "uuidColumn";
  private static final String UUID_0 = "550e8400-e29b-41d4-a716-446655440000";
  private static final String UUID_0_HEX = "550e8400e29b41d4a716446655440000";
  private static final String UUID_1_HEX = "550e8400e29b41d4a716446655440001";
  private static final String UUID_2 = "550e8400-e29b-41d4-a716-446655440002";

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return 2;
  }

  @Override
  public int getNumAvroFiles() {
    return 1;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName()).build();
    tableConfig.getIndexingConfig().setBloomFilterConfigs(
        Map.of(UUID_COLUMN, new BloomFilterConfig(1e-9, 0, false)));
    return tableConfig;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(UUID_COLUMN, DataType.UUID)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("uuidRecord", null, null, false);
    avroSchema.setFields(List.of(new org.apache.avro.Schema.Field(UUID_COLUMN,
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      DataFileWriter<GenericData.Record> writer = avroFilesAndWriters.getWriters().get(0);
      for (String uuid : List.of(UUID_0, UUID_2)) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(UUID_COLUMN, uuid);
        writer.append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test
  public void testUuidBloomFilterQueries()
      throws Exception {
    setUseMultiStageQueryEngine(false);

    assertCountAndPrunedSegments(
        String.format("SELECT COUNT(*) FROM %s WHERE %s = '%s'", getTableName(), UUID_COLUMN, UUID_0_HEX), 1, 1, 0);
    assertCountAndPrunedSegments(String.format(
        "SELECT COUNT(*) FROM %s WHERE %s = CAST('%s' AS UUID)", getTableName(), UUID_COLUMN, UUID_2), 1, 1, 0);
    assertCountAndPrunedSegments(
        String.format("SELECT COUNT(*) FROM %s WHERE %s = '%s'", getTableName(), UUID_COLUMN, UUID_1_HEX), 0, 0, 1);
  }

  private void assertCountAndPrunedSegments(String query, long expectedCount, int expectedProcessedSegments,
      int expectedPrunedSegments)
      throws Exception {
    JsonNode response = postQuery(query);
    assertTrue(response.path("exceptions").isEmpty(), response.toPrettyString());
    assertEquals(response.path("resultTable").path("rows").path(0).path(0).asLong(), expectedCount,
        response.toPrettyString());
    assertEquals(response.path("numSegmentsQueried").asInt(), 1, response.toPrettyString());
    assertEquals(response.path("numSegmentsProcessed").asInt(), expectedProcessedSegments, response.toPrettyString());
    assertEquals(response.path("numSegmentsPrunedByValue").asInt(), expectedPrunedSegments,
        response.toPrettyString());
  }
}
