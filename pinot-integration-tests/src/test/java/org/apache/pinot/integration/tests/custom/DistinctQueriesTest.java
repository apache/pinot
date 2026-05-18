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
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/**
 * Integration test for distinct early termination query options. Uses SSE (v1) only because these options
 * are enforced in DistinctCombineOperator which is not used by the multi-stage query engine.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class DistinctQueriesTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "DistinctQueriesCustomTest";

  private static final String INT_COL = "intCol";
  private static final String STRING_COL = "stringCol";

  private static final int NUM_ROWS_PER_SEGMENT = 50_000;
  private static final int NUM_INT_VALUES = 5;
  private static final int NUM_STRING_VALUES = 4;
  // Use more segments than servers (cluster has 2 servers) so that each server
  // has multiple segments and the DistinctCombineOperator actually merges blocks,
  // which is where early termination logic is evaluated.
  private static final int NUM_SEGMENTS = 4;

  @Override
  protected long getCountStarResult() {
    return (long) NUM_ROWS_PER_SEGMENT * NUM_SEGMENTS;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema =
        SchemaBuilder.record("DistinctRecord").fields()
            .requiredInt(INT_COL)
            .requiredString(STRING_COL)
            .endRecord();

    List<File> files = new ArrayList<>();
    for (int i = 0; i < NUM_SEGMENTS; i++) {
      files.add(createAvroFile(avroSchema, new File(_tempDir, "distinct-data-" + i + ".avro")));
    }
    return files;
  }

  private File createAvroFile(org.apache.avro.Schema avroSchema, File file)
      throws Exception {
    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, file);
      for (int i = 0; i < NUM_ROWS_PER_SEGMENT; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(INT_COL, i % NUM_INT_VALUES);
        record.put(STRING_COL, "type_" + (i % NUM_STRING_VALUES));
        writer.append(record);
      }
    }
    return file;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    // Force raw (no-dictionary) columns so DistinctOperator is used instead of
    // DictionaryBasedDistinctOperator. The regular DistinctOperator scans actual rows
    // and sets numDocsScanned on the result block, enabling combine-level early termination.
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setNoDictionaryColumns(List.of(INT_COL, STRING_COL))
        .build();
  }

  @Override
  protected String getSortedColumn() {
    return null;
  }

  /**
   * Tests maxRowsInDistinct: after merging segments, accumulated numDocsScanned exceeds the budget
   * and the combine operator sets the early termination flag. LIMIT is set high so the query cannot
   * naturally satisfy (distinct values < LIMIT).
   */
  @Test
  public void testMaxRowsInDistinctEarlyTermination()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String sql = String.format(
        "SET maxRowsInDistinct=100; SELECT DISTINCT %s FROM %s LIMIT 10000",
        STRING_COL, getTableName());
    JsonNode response = postQuery(sql);
    assertTrue(response.path("maxRowsInDistinctReached").asBoolean(false),
        "expected maxRowsInDistinctReached flag. Response: " + response);
    assertTrue(response.path("partialResult").asBoolean(false),
        "partialResult should be true. Response: " + response);
  }

  /**
   * Tests maxRowsWithoutChangeInDistinct: when merging a segment adds no new distinct values, the
   * segment's numDocsScanned counts toward the no-change budget. With 2 identical segments, the
   * second segment's merge triggers the limit.
   */
  @Test
  public void testNoChangeEarlyTermination()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String sql = String.format(
        "SET maxRowsWithoutChangeInDistinct=1000; SELECT DISTINCT %s FROM %s LIMIT 10000",
        INT_COL, getTableName());
    JsonNode response = postQuery(sql);
    assertTrue(response.path("maxRowsWithoutChangeInDistinctReached").asBoolean(false),
        "expected no-change flag to be set. Response: " + response);
    assertTrue(response.path("partialResult").asBoolean(false),
        "partialResult should be true. Response: " + response);
  }
}
