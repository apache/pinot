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
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// End-to-end integration test for the `skipExpiredRecords` query option (issue #16689).
///
/// The option is honored by the single-stage engine only: when set, the broker appends a
/// `timeColumn >= (now - retention)` filter so records older than the table's retention window are excluded even though
/// their segment has not been deleted. Rows are split into "fresh" (within retention) and "expired" (well past
/// retention); the test asserts the counts with and without the option.
@Test(suiteName = "CustomClusterIntegrationTest")
public class SkipExpiredRecordsTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "SkipExpiredRecordsTest";
  private static final String ID_COLUMN = "id";
  // Retention is 5 days; a row this far in the past is out of retention.
  private static final long EXPIRED_TS = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30);
  // A row at "now" is always within retention.
  private static final long FRESH_TS = System.currentTimeMillis();
  private static final int NUM_FRESH_ROWS = 3;
  private static final int NUM_EXPIRED_ROWS = 5;

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    // waitForAllDocsLoaded queries without the option, so all rows must be present.
    return NUM_FRESH_ROWS + NUM_EXPIRED_ROWS;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(ID_COLUMN, FieldSpec.DataType.INT)
        .addDateTimeField(TIMESTAMP_FIELD_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setTimeColumnName(TIMESTAMP_FIELD_NAME)
        .setRetentionTimeUnit(TimeUnit.DAYS.name())
        .setRetentionTimeValue("5")
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("skipExpiredRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(ID_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field(TIMESTAMP_FIELD_NAME,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null)
    ));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      int id = 0;
      for (int i = 0; i < NUM_FRESH_ROWS; i++) {
        writers.get(id % getNumAvroFiles()).append(newRecord(avroSchema, id++, FRESH_TS));
      }
      for (int i = 0; i < NUM_EXPIRED_ROWS; i++) {
        writers.get(id % getNumAvroFiles()).append(newRecord(avroSchema, id++, EXPIRED_TS));
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  private static GenericData.Record newRecord(org.apache.avro.Schema avroSchema, int id, long ts) {
    GenericData.Record record = new GenericData.Record(avroSchema);
    record.put(ID_COLUMN, id);
    record.put(TIMESTAMP_FIELD_NAME, ts);
    return record;
  }

  @Test
  public void testWithoutOptionReturnsAllRows()
      throws Exception {
    // The option is single-stage-only; scope the whole test to SSE.
    setUseMultiStageQueryEngine(false);
    JsonNode response = postQuery("SELECT COUNT(*) FROM " + getTableName());
    assertCount(response, NUM_FRESH_ROWS + NUM_EXPIRED_ROWS);
  }

  @Test
  public void testSkipExpiredRecordsExcludesOutOfRetentionRows()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    JsonNode response =
        postQueryWithOptions("SELECT COUNT(*) FROM " + getTableName(), "skipExpiredRecords=true");
    assertCount(response, NUM_FRESH_ROWS);
  }

  @Test
  public void testSkipExpiredRecordsCombinesWithExistingFilter()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    // id 0 is a fresh row -> kept; the existing predicate AND the retention filter both hold.
    JsonNode keptResponse =
        postQueryWithOptions("SELECT COUNT(*) FROM " + getTableName() + " WHERE " + ID_COLUMN + " = 0",
            "skipExpiredRecords=true");
    assertCount(keptResponse, 1);

    // id = NUM_FRESH_ROWS is the first expired row -> excluded by the retention filter despite matching the predicate.
    JsonNode filteredResponse =
        postQueryWithOptions("SELECT COUNT(*) FROM " + getTableName() + " WHERE " + ID_COLUMN + " = " + NUM_FRESH_ROWS,
            "skipExpiredRecords=true");
    assertCount(filteredResponse, 0);
  }

  private void assertCount(JsonNode response, long expectedCount) {
    assertEquals(response.path("exceptions").size(), 0, response.toPrettyString());
    assertEquals(getType(response, 0), "LONG");
    assertEquals(getLongCellValue(response, 0, 0), expectedCount);
  }
}
