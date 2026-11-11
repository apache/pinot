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

import java.io.File;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.client.ResultSet;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.create;
import static org.testng.Assert.assertEquals;


/// Verifies realtime UUID ingestion and full-upsert deduplication with a UUID primary key.
@Test(suiteName = "CustomClusterIntegrationTest")
public class UuidUpsertRealtimeTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "UuidUpsertRealtimeTest";
  private static final String UUID_PK_COLUMN = "uuidPk";
  private static final long BASE_TIME_MS = 1_700_100_000_000L;
  private static final List<String> UUID_PK_VALUES = List.of(
      "550e8400-e29b-41d4-a716-446655440000",
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-e29b-41d4-a716-446655440000");

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  @Override
  protected void waitForAllDocsLoaded(long timeoutMs) {
    TestUtils.waitForCondition(() -> getPinotConnection().execute(
            "SELECT COUNT(*) FROM " + TABLE_NAME + " OPTION(skipUpsert=true)").getResultSet(0).getLong(0)
            == UUID_PK_VALUES.size(),
        100L, timeoutMs, "Failed to load all UUID upsert records", Duration.ofSeconds(5));
  }

  @Override
  public int getNumAvroFiles() {
    return 1;
  }

  @Override
  protected int getRealtimeSegmentFlushSize() {
    return 2;
  }

  @Override
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    return createUpsertTableConfig(sampleAvroFile, UUID_PK_COLUMN, null, getNumKafkaPartitions());
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(UUID_PK_COLUMN, DataType.UUID)
        .addDateTimeField(getTimeColumnName(), DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .setPrimaryKeyColumns(List.of(UUID_PK_COLUMN))
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("uuidUpsertRecord", null, null, false);
    avroSchema.setFields(List.of(
        new Field(UUID_PK_COLUMN, create(Type.BYTES), null, null),
        new Field(getTimeColumnName(), create(Type.LONG), null, null)));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      DataFileWriter<GenericData.Record> writer = avroFilesAndWriters.getWriters().get(0);
      for (int i = 0; i < UUID_PK_VALUES.size(); i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(UUID_PK_COLUMN, ByteBuffer.wrap(UuidUtils.toBytes(UUID_PK_VALUES.get(i))));
        record.put(getTimeColumnName(), BASE_TIME_MS + i);
        writer.append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test
  public void testUuidPrimaryKeyUpsert()
      throws Exception {
    ResultSet resultSet = getPinotConnection().execute(
        "SELECT " + UUID_PK_COLUMN + ", " + getTimeColumnName() + " FROM " + getTableName()
            + " ORDER BY " + UUID_PK_COLUMN).getResultSet(0);
    assertEquals(resultSet.getRowCount(), 2);
    assertRow(resultSet, 0, UUID_PK_VALUES.get(0), BASE_TIME_MS + 2);
    assertRow(resultSet, 1, UUID_PK_VALUES.get(1), BASE_TIME_MS + 1);
  }

  private static void assertRow(ResultSet resultSet, int row, String uuidPk, long timestamp) {
    assertEquals(resultSet.getString(row, 0), uuidPk);
    assertEquals(resultSet.getLong(row, 1), timestamp);
  }
}
