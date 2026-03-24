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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.apache.pinot.integration.tests.ClusterIntegrationTestUtils.getBrokerQueryApiUrl;


/**
 * Create many small segments with many columns to test metadata overhead.
 * Test is rather slow (1 minute+) and thus disabled by default.
 * Still, it can be useful to run it manually.
 */
@Test(suiteName = "CustomClusterIntegrationTest", enabled = false)
public class BigNumberOfSegmentsTest extends CustomDataQueryClusterIntegrationTest {

  static final int FILES_NO = 1000;
  static final int RECORDS_NO = 5;
  static final String INT_COL = "i";
  static final String LONG_COL = "j";
  static final String STR_COL_PREFIX = "s";
  static final String FLOAT_COL = "f";
  static final String DOUBLE_COL = "d";
  static final int STR_COL_NUM = 200;
  private static final String TIME_COL = "ts";

  @Override
  public String getTableName() {
    return "BigNumberOfSegmentsTest";
  }

  @Override
  public Schema createSchema() {
    Schema.SchemaBuilder builder = new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_COL, FieldSpec.DataType.LONG)
        .addSingleValueDimension(FLOAT_COL, FieldSpec.DataType.FLOAT)
        .addSingleValueDimension(DOUBLE_COL, FieldSpec.DataType.DOUBLE)
        .addDateTimeField(TIME_COL, FieldSpec.DataType.TIMESTAMP, "TIMESTAMP", "1:MILLISECONDS");

    for (int i = 0; i < STR_COL_NUM; i++) {
      builder.addSingleValueDimension(STR_COL_PREFIX + i, FieldSpec.DataType.STRING);
    }

    return builder.build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    return createAvroData(_tempDir);
  }

  @Override
  public int getNumAvroFiles() {
    return FILES_NO;
  }

  @Override
  protected long getCountStarResult() {
    return (long) FILES_NO * RECORDS_NO;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    Schema schema = createSchema();
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setTimeColumnName(TIME_COL)
        .setNumReplicas(getNumReplicas())
        .setBrokerTenant(getBrokerTenant())
        .setRetentionTimeUnit("DAYS")
        .setRetentionTimeValue("1")
        .setNoDictionaryColumns(new ArrayList<>(schema.getColumnNames()))
        .build();
  }

  // Too slow for CI (1+ minute)
  @Test(enabled = false)
  public void testCreateManySegments()
      throws Exception {
    JsonNode node = postQuery("SELECT sum(i) + sum(j) + sum(d), count(*) FROM " + getTableName(),
        getBrokerQueryApiUrl(getBrokerBaseApiUrl(), true), null, getExtraQueryProperties());
    assertEquals(node.get("exceptions").size(), 0);
  }

  private static void assertEquals(int actual, int expected) {
    org.testng.Assert.assertEquals(actual, expected);
  }

  private static List<File> createAvroData(File tempDir)
      throws IOException {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    List<Field> fields = new ArrayList<>();
    fields.add(new Field(INT_COL, create(Type.INT), null, null));
    fields.add(new Field(LONG_COL, create(Type.LONG), null, null));
    fields.add(new Field(FLOAT_COL, create(Type.FLOAT), null, null));
    fields.add(new Field(DOUBLE_COL, create(Type.DOUBLE), null, null));
    fields.add(new Field(TIME_COL, create(Type.LONG), null, null));
    for (int i = 0; i < STR_COL_NUM; i++) {
      fields.add(new Field(STR_COL_PREFIX + i, create(Type.STRING), null, null));
    }

    avroSchema.setFields(fields);

    StringBuilder sb = new StringBuilder("str");
    String[] strs = new String[RECORDS_NO];
    for (int i = 0; i < RECORDS_NO; i++) {
      sb.setLength(3);
      sb.append(i);
      strs[i] = sb.toString();
    }

    ReusableRecord record = new ReusableRecord(avroSchema.getFields().size());
    int r = 0;
    List<File> files = new ArrayList<>();
    for (int file = 0; file < FILES_NO; file++) {
      File avroFile = new File(tempDir, "data_" + file + ".avro");
      try (DataFileWriter<ReusableRecord> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        fileWriter.create(avroSchema, avroFile);

        for (int docId = 0; docId < RECORDS_NO; docId++) {
          record.put(0, file);
          record.put(1, docId);
          record.put(2, docId);
          record.put(3, docId);
          record.put(4, TimeUtils.VALID_MIN_TIME_MILLIS + r++ * 3600L);

          for (int si = 0; si < STR_COL_NUM; si++) {
            record.put(5 + si, strs[docId]);
          }

          fileWriter.append(record);
          record.clear();
        }
        files.add(avroFile);
      }
    }
    return files;
  }

  private static org.apache.avro.Schema create(Type type) {
    return org.apache.avro.Schema.create(type);
  }

  static class ReusableRecord implements IndexedRecord {
    Object[] _data;

    ReusableRecord(int fieldNum) {
      _data = new Object[fieldNum];
    }

    @Override
    public void put(int i, Object v) {
      _data[i] = v;
    }

    @Override
    public Object get(int i) {
      return _data[i];
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return null;
    }

    void clear() {
      Arrays.fill(_data, null);
    }
  }
}
