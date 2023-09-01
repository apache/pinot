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
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Integration test for floating point data type (float & double) filter queries.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class FloatingPointDataTypeTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "FloatingPointDataTypeTest";
  private static final int NUM_DOCS = 10;
  private static final String MET_DOUBLE_SORTED = "metDoubleSorted";
  private static final String MET_FLOAT_SORTED = "metFloatSorted";
  private static final String MET_DOUBLE_UNSORTED = "metDoubleUnsorted";
  private static final String MET_FLOAT_UNSORTED = "metFloatUnsorted";
  private static final String MET_DOUBLE_SORTED_NO_DIC = "metDoubleSortedNoDic";
  private static final String MET_FLOAT_SORTED_NO_DIC = "metFloatSortedNoDic";
  private static final String MET_DOUBLE_UNSORTED_NO_DIC = "metDoubleUnsortedNoDic";
  private static final String MET_FLOAT_UNSORTED_NO_DIC = "metFloatUnsortedNoDic";

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addMetric(MET_DOUBLE_SORTED, FieldSpec.DataType.DOUBLE)
        .addMetric(MET_FLOAT_SORTED, FieldSpec.DataType.FLOAT)
        .addMetric(MET_DOUBLE_UNSORTED, FieldSpec.DataType.DOUBLE)
        .addMetric(MET_FLOAT_UNSORTED, FieldSpec.DataType.FLOAT)
        .addMetric(MET_DOUBLE_SORTED_NO_DIC, FieldSpec.DataType.DOUBLE)
        .addMetric(MET_FLOAT_SORTED_NO_DIC, FieldSpec.DataType.FLOAT)
        .addMetric(MET_DOUBLE_UNSORTED_NO_DIC, FieldSpec.DataType.DOUBLE)
        .addMetric(MET_FLOAT_UNSORTED_NO_DIC, FieldSpec.DataType.FLOAT)
        .build();
  }

  @Override
  public File createAvroFile()
      throws IOException {

    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(MET_DOUBLE_SORTED,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        // Please do not use FLOAT type in Avro schema, it is lossy.
        // For example, 0.06 will be saved as 0.059999995 in Avro file when the data type is FLOAT.
        new org.apache.avro.Schema.Field(MET_FLOAT_SORTED,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(MET_DOUBLE_UNSORTED,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(MET_FLOAT_UNSORTED,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(MET_DOUBLE_SORTED_NO_DIC,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(MET_FLOAT_SORTED_NO_DIC,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(MET_DOUBLE_UNSORTED_NO_DIC,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(MET_FLOAT_UNSORTED_NO_DIC,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null)));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      double sortedValue = 0.0;
      double unsortedValue = 0.05;
      for (int i = 0; i < NUM_DOCS; i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(MET_DOUBLE_SORTED, sortedValue);
        record.put(MET_FLOAT_SORTED, sortedValue);
        record.put(MET_DOUBLE_UNSORTED, unsortedValue);
        record.put(MET_FLOAT_UNSORTED, unsortedValue);
        record.put(MET_DOUBLE_SORTED_NO_DIC, sortedValue);
        record.put(MET_FLOAT_SORTED_NO_DIC, sortedValue);
        record.put(MET_DOUBLE_UNSORTED_NO_DIC, unsortedValue);
        record.put(MET_FLOAT_UNSORTED_NO_DIC, unsortedValue);
        sortedValue += 0.01;
        unsortedValue += 0.01;
        if (unsortedValue > 0.09) {
          unsortedValue = 0.00;
        }

        // add avro record to file
        fileWriter.append(record);
      }
    }
    return avroFile;
  }

  @Override
  protected long getCountStarResult() {
    return NUM_DOCS;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setNoDictionaryColumns(getNoDictionaryColumns()).build();
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    return ImmutableList.of(MET_DOUBLE_SORTED_NO_DIC, MET_FLOAT_SORTED_NO_DIC, MET_DOUBLE_UNSORTED_NO_DIC,
        MET_FLOAT_UNSORTED_NO_DIC);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Choose 0.05 because if it's not converted correctly, float 0.05 will be converted to double 0.05000000074505806
    String[][] filterAndExpectedCount = {
        {MET_DOUBLE_SORTED + " > 0.05", "4"},
        {MET_DOUBLE_SORTED + " = 0.05", "1"},
        {MET_DOUBLE_SORTED + " < 0.05", "5"},
        {MET_FLOAT_SORTED + " > 0.05", "4"},
        // FIXME: V1 query engine fails with null pointer exception for this query
        //{MET_FLOAT_SORTED + " = 0.05", "1"},
        {MET_FLOAT_SORTED + " < 0.05", "5"},
        {MET_DOUBLE_UNSORTED + " > 0.05", "4"},
        {MET_DOUBLE_UNSORTED + " = 0.05", "1"},
        {MET_DOUBLE_UNSORTED + " < 0.05", "5"},
        {MET_FLOAT_UNSORTED + " > 0.05", "4"},
        // FIXME: V1 query engine fails with null pointer exception for this query
        //{MET_FLOAT_UNSORTED + " = 0.05", "1"},
        {MET_FLOAT_UNSORTED + " < 0.05", "5"},
        {MET_DOUBLE_SORTED_NO_DIC + " > 0.05", "4"},
        {MET_DOUBLE_SORTED_NO_DIC + " = 0.05", "1"},
        {MET_DOUBLE_SORTED_NO_DIC + " < 0.05", "5"},
        {MET_FLOAT_SORTED_NO_DIC + " > 0.05", "4"},
        // FIXME: V1 query engine fails with null pointer exception for this query
        // {MET_FLOAT_SORTED_NO_DIC + " = 0.05", "1"},
        {MET_FLOAT_SORTED_NO_DIC + " < 0.05", "5"},
        {MET_DOUBLE_UNSORTED_NO_DIC + " > 0.05", "4"},
        {MET_DOUBLE_UNSORTED_NO_DIC + " = 0.05", "1"},
        {MET_DOUBLE_UNSORTED_NO_DIC + " < 0.05", "5"},
        {MET_FLOAT_UNSORTED_NO_DIC + " > 0.05", "4"},
        // FIXME: V1 query engine fails with null pointer exception for this query
        // {MET_FLOAT_UNSORTED_NO_DIC + " = 0.05", "1"},
        {MET_FLOAT_UNSORTED_NO_DIC + " < 0.05", "5"},
    };
    for (String[] faec : filterAndExpectedCount) {
      String filter = faec[0];
      long expected = Long.parseLong(faec[1]);
      String query = String.format("SELECT COUNT(*) FROM %s WHERE %s", getTableName(), filter);
      JsonNode jsonNode = postQuery(query);
      assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).asLong(), expected);
    }
  }
}
