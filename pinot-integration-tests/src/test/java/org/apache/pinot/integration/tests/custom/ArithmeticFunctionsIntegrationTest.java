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
import java.math.BigDecimal;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * End-to-end integration tests for polymorphic arithmetic scalar functions over a custom Pinot cluster.
 *
 * <p>This test validates broker/server query execution and result typing for arithmetic scalar functions that now
 * support multiple numeric types, including BIG_DECIMAL. Test methods mutate query-engine mode and are not
 * thread-safe.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class ArithmeticFunctionsIntegrationTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "ArithmeticFunctionsIntegrationTest";
  private static final String ID_COLUMN = "id";
  private static final String INT_VALUE_COLUMN = "intValue";
  private static final String INT_DIVISOR_COLUMN = "intDivisor";
  private static final String NEGATIVE_INT_DIVISOR_COLUMN = "negativeIntDivisor";
  private static final String ZERO_INT_DIVISOR_COLUMN = "zeroIntDivisor";
  private static final String LONG_VALUE_COLUMN = "longValue";
  private static final String FLOAT_VALUE_COLUMN = "floatValue";
  private static final String FLOAT_DIVISOR_COLUMN = "floatDivisor";
  private static final String DOUBLE_VALUE_COLUMN = "doubleValue";
  private static final String DOUBLE_DIVISOR_COLUMN = "doubleDivisor";
  private static final String BIG_DECIMAL_VALUE_COLUMN = "bigDecimalValue";
  private static final String BIG_DECIMAL_DIVISOR_COLUMN = "bigDecimalDivisor";
  private static final String NEGATIVE_BIG_DECIMAL_DIVISOR_COLUMN = "negativeBigDecimalDivisor";
  private static final String ZERO_BIG_DECIMAL_DIVISOR_COLUMN = "zeroBigDecimalDivisor";

  private static final List<RowData> ROWS = List.of(
      new RowData(0, -9, 5, -5, 0, -9L, -9.5F, 5F, -9.5D, -5D, new BigDecimal("-9.0"), new BigDecimal("5.0"),
          new BigDecimal("-5.0"), BigDecimal.ZERO),
      new RowData(1, 9, 5, -5, 0, 9L, 9.5F, 5F, 9.5D, 5D, new BigDecimal("9.0"), new BigDecimal("5.0"),
          new BigDecimal("-5.0"), BigDecimal.ZERO)
  );

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  protected long getCountStarResult() {
    return ROWS.size();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(ID_COLUMN, FieldSpec.DataType.INT)
        .addMetric(INT_VALUE_COLUMN, FieldSpec.DataType.INT)
        .addMetric(INT_DIVISOR_COLUMN, FieldSpec.DataType.INT)
        .addMetric(NEGATIVE_INT_DIVISOR_COLUMN, FieldSpec.DataType.INT)
        .addMetric(ZERO_INT_DIVISOR_COLUMN, FieldSpec.DataType.INT)
        .addMetric(LONG_VALUE_COLUMN, FieldSpec.DataType.LONG)
        .addMetric(FLOAT_VALUE_COLUMN, FieldSpec.DataType.FLOAT)
        .addMetric(FLOAT_DIVISOR_COLUMN, FieldSpec.DataType.FLOAT)
        .addMetric(DOUBLE_VALUE_COLUMN, FieldSpec.DataType.DOUBLE)
        .addMetric(DOUBLE_DIVISOR_COLUMN, FieldSpec.DataType.DOUBLE)
        .addMetric(BIG_DECIMAL_VALUE_COLUMN, FieldSpec.DataType.BIG_DECIMAL)
        .addMetric(BIG_DECIMAL_DIVISOR_COLUMN, FieldSpec.DataType.BIG_DECIMAL)
        .addMetric(NEGATIVE_BIG_DECIMAL_DIVISOR_COLUMN, FieldSpec.DataType.BIG_DECIMAL)
        .addMetric(ZERO_BIG_DECIMAL_DIVISOR_COLUMN, FieldSpec.DataType.BIG_DECIMAL)
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("arithmeticRecord", null, null, false);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(ID_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field(INT_VALUE_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(INT_DIVISOR_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(NEGATIVE_INT_DIVISOR_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(ZERO_INT_DIVISOR_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT), null, null),
        new org.apache.avro.Schema.Field(LONG_VALUE_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG), null, null),
        // Use DOUBLE in Avro for FLOAT Pinot columns to avoid Avro float precision loss.
        new org.apache.avro.Schema.Field(FLOAT_VALUE_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(FLOAT_DIVISOR_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(DOUBLE_VALUE_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(DOUBLE_DIVISOR_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE), null, null),
        new org.apache.avro.Schema.Field(BIG_DECIMAL_VALUE_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(BIG_DECIMAL_DIVISOR_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(NEGATIVE_BIG_DECIMAL_DIVISOR_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null),
        new org.apache.avro.Schema.Field(ZERO_BIG_DECIMAL_DIVISOR_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING), null, null)
    ));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < ROWS.size(); i++) {
        RowData rowData = ROWS.get(i);
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID_COLUMN, rowData._id);
        record.put(INT_VALUE_COLUMN, rowData._intValue);
        record.put(INT_DIVISOR_COLUMN, rowData._intDivisor);
        record.put(NEGATIVE_INT_DIVISOR_COLUMN, rowData._negativeIntDivisor);
        record.put(ZERO_INT_DIVISOR_COLUMN, rowData._zeroIntDivisor);
        record.put(LONG_VALUE_COLUMN, rowData._longValue);
        record.put(FLOAT_VALUE_COLUMN, (double) rowData._floatValue);
        record.put(FLOAT_DIVISOR_COLUMN, (double) rowData._floatDivisor);
        record.put(DOUBLE_VALUE_COLUMN, rowData._doubleValue);
        record.put(DOUBLE_DIVISOR_COLUMN, rowData._doubleDivisor);
        record.put(BIG_DECIMAL_VALUE_COLUMN, rowData._bigDecimalValue.toPlainString());
        record.put(BIG_DECIMAL_DIVISOR_COLUMN, rowData._bigDecimalDivisor.toPlainString());
        record.put(NEGATIVE_BIG_DECIMAL_DIVISOR_COLUMN, rowData._negativeBigDecimalDivisor.toPlainString());
        record.put(ZERO_BIG_DECIMAL_DIVISOR_COLUMN, rowData._zeroBigDecimalDivisor.toPlainString());
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testUnaryArithmeticFunctions(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT COUNT(*) FROM %s WHERE %s = 0 "
            + "AND abs(%s) = 9 "
            + "AND abs(%s) = 9 "
            + "AND abs(%s) = 9.5 "
            + "AND abs(%s) = 9.5 "
            + "AND abs(%s) = 9.0 "
            + "AND negate(%s) = 9 "
            + "AND negate(%s) = 9 "
            + "AND negate(%s) = 9.5 "
            + "AND negate(%s) = 9.5 "
            + "AND negate(%s) = 9.0",
        getTableName(), ID_COLUMN,
        INT_VALUE_COLUMN,
        LONG_VALUE_COLUMN,
        FLOAT_VALUE_COLUMN,
        DOUBLE_VALUE_COLUMN,
        BIG_DECIMAL_VALUE_COLUMN,
        INT_VALUE_COLUMN,
        LONG_VALUE_COLUMN,
        FLOAT_VALUE_COLUMN,
        DOUBLE_VALUE_COLUMN,
        BIG_DECIMAL_VALUE_COLUMN);
    JsonNode response = postQuery(query);
    assertCount(response, 1L);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testModuloAndPositiveModuloFunctions(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT COUNT(*) FROM %s WHERE %s = 0 "
            + "AND mod(%s, %s) = -4 "
            + "AND mod(%s, %s) = -4 "
            + "AND mod(%s, %s) = -4.5 "
            + "AND mod(%s, %s) = -4.5 "
            + "AND mod(%s, %s) = -4.0 "
            + "AND positiveModulo(%s, %s) = 1 "
            + "AND positiveModulo(%s, %s) = 1 "
            + "AND positiveModulo(%s, %s) = 0.5 "
            + "AND positiveModulo(%s, %s) = 1.0",
        getTableName(), ID_COLUMN,
        INT_VALUE_COLUMN, INT_DIVISOR_COLUMN,
        LONG_VALUE_COLUMN, INT_DIVISOR_COLUMN,
        FLOAT_VALUE_COLUMN, FLOAT_DIVISOR_COLUMN,
        DOUBLE_VALUE_COLUMN, DOUBLE_DIVISOR_COLUMN,
        BIG_DECIMAL_VALUE_COLUMN, BIG_DECIMAL_DIVISOR_COLUMN,
        INT_VALUE_COLUMN, INT_DIVISOR_COLUMN,
        INT_VALUE_COLUMN, NEGATIVE_INT_DIVISOR_COLUMN,
        FLOAT_VALUE_COLUMN, FLOAT_DIVISOR_COLUMN,
        BIG_DECIMAL_VALUE_COLUMN, NEGATIVE_BIG_DECIMAL_DIVISOR_COLUMN);
    JsonNode response = postQuery(query);
    assertCount(response, 1L);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testModuloOrZeroFunctions(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT COUNT(*) FROM %s WHERE %s = 0 "
            + "AND moduloOrZero(%s, %s) = 0 "
            + "AND moduloOrZero(%s, %s) = 0",
        getTableName(), ID_COLUMN,
        INT_VALUE_COLUMN, ZERO_INT_DIVISOR_COLUMN,
        BIG_DECIMAL_VALUE_COLUMN, ZERO_BIG_DECIMAL_DIVISOR_COLUMN);
    JsonNode response = postQuery(query);
    assertCount(response, 1L);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testLeastGreatestFunctions(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format("SELECT COUNT(*) FROM %s WHERE %s = 0 "
            + "AND least(%s, %s) = %s "
            + "AND greatest(%s, %s) = %s "
            + "AND least(%s, %s) = %s "
            + "AND greatest(%s, %s) = %s "
            + "AND least(%s, %s) = %s "
            + "AND greatest(%s, %s) = %s",
        getTableName(), ID_COLUMN,
        LONG_VALUE_COLUMN, INT_DIVISOR_COLUMN, LONG_VALUE_COLUMN,
        FLOAT_VALUE_COLUMN, INT_DIVISOR_COLUMN, INT_DIVISOR_COLUMN,
        BIG_DECIMAL_VALUE_COLUMN, DOUBLE_VALUE_COLUMN, DOUBLE_VALUE_COLUMN,
        BIG_DECIMAL_VALUE_COLUMN, DOUBLE_VALUE_COLUMN, BIG_DECIMAL_VALUE_COLUMN,
        BIG_DECIMAL_VALUE_COLUMN, BIG_DECIMAL_DIVISOR_COLUMN, BIG_DECIMAL_VALUE_COLUMN,
        BIG_DECIMAL_VALUE_COLUMN, BIG_DECIMAL_DIVISOR_COLUMN, BIG_DECIMAL_DIVISOR_COLUMN);
    JsonNode response = postQuery(query);
    assertCount(response, 1L);
  }

  private void assertCount(JsonNode response, long expectedCount) {
    assertEquals(response.path("exceptions").size(), 0, response.toPrettyString());
    assertEquals(getType(response, 0), "LONG");
    assertEquals(getLongCellValue(response, 0, 0), expectedCount);
  }

  private static final class RowData {
    private final int _id;
    private final int _intValue;
    private final int _intDivisor;
    private final int _negativeIntDivisor;
    private final int _zeroIntDivisor;
    private final long _longValue;
    private final float _floatValue;
    private final float _floatDivisor;
    private final double _doubleValue;
    private final double _doubleDivisor;
    private final BigDecimal _bigDecimalValue;
    private final BigDecimal _bigDecimalDivisor;
    private final BigDecimal _negativeBigDecimalDivisor;
    private final BigDecimal _zeroBigDecimalDivisor;

    private RowData(int id, int intValue, int intDivisor, int negativeIntDivisor, int zeroIntDivisor, long longValue,
        float floatValue, float floatDivisor, double doubleValue, double doubleDivisor, BigDecimal bigDecimalValue,
        BigDecimal bigDecimalDivisor, BigDecimal negativeBigDecimalDivisor, BigDecimal zeroBigDecimalDivisor) {
      _id = id;
      _intValue = intValue;
      _intDivisor = intDivisor;
      _negativeIntDivisor = negativeIntDivisor;
      _zeroIntDivisor = zeroIntDivisor;
      _longValue = longValue;
      _floatValue = floatValue;
      _floatDivisor = floatDivisor;
      _doubleValue = doubleValue;
      _doubleDivisor = doubleDivisor;
      _bigDecimalValue = bigDecimalValue;
      _bigDecimalDivisor = bigDecimalDivisor;
      _negativeBigDecimalDivisor = negativeBigDecimalDivisor;
      _zeroBigDecimalDivisor = zeroBigDecimalDivisor;
    }
  }
}
