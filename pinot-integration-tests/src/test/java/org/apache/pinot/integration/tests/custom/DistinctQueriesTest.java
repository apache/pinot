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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.integration.tests.ClusterIntegrationTestUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


@Test(suiteName = "CustomClusterIntegrationTest")
public class DistinctQueriesTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "DistinctQueriesCustomTest";

  private static final String INT_COL = "intCol";
  private static final String LONG_COL = "longCol";
  private static final String DOUBLE_COL = "doubleCol";
  private static final String STRING_COL = "stringCol";
  private static final String MV_INT_COL = "intArrayCol";
  private static final String MV_STRING_COL = "stringArrayCol";

  // Keep the dataset modest to avoid slowing down the suite while still exercising early termination.
  private static final int NUM_ROWS_PER_SEGMENT = 50_000;
  private static final int NUM_INT_VALUES = 5;
  private static final int NUM_LONG_VALUES = 5;
  private static final int NUM_DOUBLE_VALUES = 3;
  private static final int NUM_STRING_VALUES = 4;
  private static final int NUM_MV_INT_VALUES = 3;
  private static final long LONG_BASE_VALUE = 1_000L;
  private static final double DOUBLE_OFFSET = 0.25d;
  private static final int MV_OFFSET = 50;
  private static final int NUM_SEGMENTS = 2;
  private static final int MAX_ROWS_IN_DISTINCT = 3;

  @Override
  protected long getCountStarResult() {
    return NUM_ROWS_PER_SEGMENT * 2;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(INT_COL, FieldSpec.DataType.INT)
        .addSingleValueDimension(LONG_COL, FieldSpec.DataType.LONG)
        .addSingleValueDimension(DOUBLE_COL, FieldSpec.DataType.DOUBLE)
        .addSingleValueDimension(STRING_COL, FieldSpec.DataType.STRING)
        .addMultiValueDimension(MV_INT_COL, FieldSpec.DataType.INT)
        .addMultiValueDimension(MV_STRING_COL, FieldSpec.DataType.STRING)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema =
        SchemaBuilder.record("DistinctRecord").fields()
            .requiredInt(INT_COL)
            .requiredLong(LONG_COL)
            .requiredDouble(DOUBLE_COL)
            .requiredString(STRING_COL)
            .name(MV_INT_COL).type().array().items().intType().noDefault()
            .name(MV_STRING_COL).type().array().items().stringType().noDefault()
            .endRecord();

    File avroFile = new File(_tempDir, "distinct-data.avro");
    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, avroFile);
      for (int i = 0; i < NUM_ROWS_PER_SEGMENT; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(INT_COL, getIntValue(i));
        record.put(LONG_COL, getLongValue(i));
        record.put(DOUBLE_COL, getDoubleValue(i));
        record.put(STRING_COL, getStringValue(i));
        record.put(MV_INT_COL, List.of(getMultiValueBase(i), getMultiValueBase(i) + MV_OFFSET));
        record.put(MV_STRING_COL, List.of(getStringValue(i), getStringValue(i + MV_OFFSET)));
        writer.append(record);
      }
    }
    File avroFile1 = new File(_tempDir, "distinct-data-1.avro");
    try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, avroFile1);
      for (int i = 0; i < NUM_ROWS_PER_SEGMENT; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(INT_COL, getIntValue(i));
        record.put(LONG_COL, getLongValue(i));
        record.put(DOUBLE_COL, getDoubleValue(i));
        record.put(STRING_COL, getStringValue(i));
        record.put(MV_INT_COL, List.of(getMultiValueBase(i), getMultiValueBase(i) + MV_OFFSET));
        record.put(MV_STRING_COL, List.of(getStringValue(i), getStringValue(i + MV_OFFSET)));
        writer.append(record);
      }
    }
    return List.of(avroFile, avroFile1);
  }

  @Override
  protected String getSortedColumn() {
    return null;
  }

  private int getIntValue(int recordId) {
    return recordId % NUM_INT_VALUES;
  }

  private long getLongValue(int recordId) {
    return LONG_BASE_VALUE + (recordId % NUM_LONG_VALUES);
  }

  private double getDoubleValue(int recordId) {
    return (recordId % NUM_DOUBLE_VALUES) + DOUBLE_OFFSET;
  }

  private String getStringValue(int recordId) {
    return "type_" + (recordId % NUM_STRING_VALUES);
  }

  private int getMultiValueBase(int recordId) {
    return recordId % NUM_MV_INT_VALUES;
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctSingleValuedColumns(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertDistinctColumnValues(INT_COL, getExpectedIntValues(), JsonNode::asInt);
    assertDistinctColumnValues(LONG_COL, getExpectedLongValues(), JsonNode::asLong);
    assertDistinctColumnValues(DOUBLE_COL, getExpectedDoubleValues(), JsonNode::asDouble);
    assertDistinctColumnValues(STRING_COL, getExpectedStringValues(), JsonNode::textValue);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctMultiValueColumn(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = useMultiStageQueryEngine ? String.format("SELECT ARRAY_TO_MV(%s) FROM %s GROUP BY 1", MV_INT_COL,
        getTableName()) : String.format("SELECT DISTINCT %s FROM %s", MV_INT_COL, getTableName());
    JsonNode result = postQuery(query);
    JsonNode rows = result.get("resultTable").get("rows");
    Set<Integer> actual = new HashSet<>();
    for (JsonNode row : rows) {
      actual.add(row.get(0).asInt());
    }
    assertEquals(actual, getExpectedMultiValueEntries());
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctMultipleColumns(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query =
        String.format("SELECT DISTINCT %s, %s FROM %s ORDER BY %s, %s LIMIT 1000", INT_COL, STRING_COL, getTableName(),
            INT_COL, STRING_COL);
    JsonNode rows = postQuery(query).get("resultTable").get("rows");
    List<String> actual = new ArrayList<>(rows.size());
    for (JsonNode row : rows) {
      actual.add(row.get(0).asInt() + "|" + row.get(1).textValue());
    }
    assertEquals(actual, getExpectedIntStringPairs());
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testMaxRowsInDistinctEarlyTermination(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String sql = String.format("SELECT DISTINCT %s FROM %s WHERE rand() < 1.0 LIMIT 100", STRING_COL, getTableName());
    JsonNode response = postQueryWithOptions(sql, useMultiStageQueryEngine,
        Map.of(QueryOptionKey.MAX_ROWS_IN_DISTINCT, Integer.toString(MAX_ROWS_IN_DISTINCT)));
    assertTrue(response.path("maxRowsInDistinctReached").asBoolean(false),
        "expected maxRowsInDistinctReached flag. Response: " + response);
    assertTrue(response.path("partialResult").asBoolean(false), "partialResult should be true. Response: " + response);
    assertEquals(response.get("resultTable").get("rows").size(), MAX_ROWS_IN_DISTINCT,
        "row count should honor threshold");
    long numDocsScanned = response.path("numDocsScanned").asLong();
    assertTrue(numDocsScanned >= MAX_ROWS_IN_DISTINCT,
        "expected at least one segment to scan the full budget. Response: " + response);
    assertTrue(numDocsScanned <= (long) MAX_ROWS_IN_DISTINCT * NUM_SEGMENTS,
        "expected scan budget to cap at maxRowsInDistinct per segment. Response: " + response);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testNoChangeEarlyTermination(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String sql = String.format("SELECT DISTINCT %s FROM %s WHERE rand() < 1.0 LIMIT 100", INT_COL, getTableName());
    JsonNode response = postQueryWithOptions(sql, useMultiStageQueryEngine,
        Map.of(QueryOptionKey.NUM_ROWS_WITHOUT_CHANGE_IN_DISTINCT, "1000"));
    assertTrue(response.path("numRowsWithoutChangeInDistinctReached").asBoolean(false),
        "expected no-change flag to be set. Response: " + response);
    assertTrue(response.path("partialResult").asBoolean(false), "partialResult should be true. Response: " + response);
  }

  private JsonNode postQueryWithOptions(String sql, boolean useMultiStageQueryEngine, Map<String, String> queryOptions)
      throws Exception {
    String optionsString =
        queryOptions.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(";"));
    return postQuery(sql,
        ClusterIntegrationTestUtils.getBrokerQueryApiUrl(getBrokerBaseApiUrl(), useMultiStageQueryEngine), null,
        Map.of("queryOptions", optionsString));
  }

  private <T> void assertDistinctColumnValues(String column, List<T> expected,
      java.util.function.Function<JsonNode, T> parser)
      throws Exception {
    String query = String.format("SELECT DISTINCT %s FROM %s ORDER BY %s", column, getTableName(), column);
    JsonNode rows = postQuery(query).get("resultTable").get("rows");
    List<T> actual = new ArrayList<>(rows.size());
    for (JsonNode row : rows) {
      actual.add(parser.apply(row.get(0)));
    }
    assertEquals(actual, expected);
  }

  private List<Integer> getExpectedIntValues() {
    return IntStream.range(0, NUM_INT_VALUES).boxed().collect(Collectors.toList());
  }

  private List<Long> getExpectedLongValues() {
    return IntStream.range(0, NUM_LONG_VALUES).mapToObj(i -> LONG_BASE_VALUE + i).collect(Collectors.toList());
  }

  private List<Double> getExpectedDoubleValues() {
    return IntStream.range(0, NUM_DOUBLE_VALUES).mapToObj(i -> i + DOUBLE_OFFSET).collect(Collectors.toList());
  }

  private List<String> getExpectedStringValues() {
    return IntStream.range(0, NUM_STRING_VALUES).mapToObj(i -> "type_" + i).collect(Collectors.toList());
  }

  private Set<Integer> getExpectedMultiValueEntries() {
    Set<Integer> entries = new HashSet<>();
    for (int i = 0; i < NUM_MV_INT_VALUES; i++) {
      entries.add(i);
      entries.add(i + MV_OFFSET);
    }
    return entries;
  }

  private List<String> getExpectedIntStringPairs() {
    List<String> pairs = new ArrayList<>();
    for (int intVal : getExpectedIntValues()) {
      for (String stringVal : getExpectedStringValues()) {
        pairs.add(intVal + "|" + stringVal);
      }
    }
    return pairs;
  }
}
