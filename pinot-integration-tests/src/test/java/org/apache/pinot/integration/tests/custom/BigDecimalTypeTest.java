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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// End-to-end coverage for BIG_DECIMAL as a dimension, both single-valued and multi-valued. Metric-side BIG_DECIMAL
/// coverage lives in [SumPrecisionTest] and [ArithmeticFunctionsIntegrationTest]; this test focuses on
/// ingestion, projection, filtering, aggregation, and cast of BIG_DECIMAL dimensions — in particular, the MV path that
/// was previously unsupported.
///
/// Each BIG_DECIMAL column appears in two variants — dictionary-encoded (default) and raw (no-dictionary) — so the
/// tests exercise both encodings. The raw variants are configured via [#getNoDictionaryColumns()].
@Test(suiteName = "CustomClusterIntegrationTest")
public class BigDecimalTypeTest extends CustomDataQueryClusterIntegrationTest {
  private static final String DEFAULT_TABLE_NAME = "BigDecimalTypeTest";
  private static final int NUM_DOCS = 100;
  private static final int MV_LENGTH = 3;

  private static final String ID_COLUMN = "id";
  // Dictionary-encoded variants
  private static final String SV_COLUMN = "bigDecimalSV";
  private static final String MV_COLUMN = "bigDecimalMV";
  // Raw (no-dictionary) variants
  private static final String RAW_SV_COLUMN = "rawBigDecimalSV";
  private static final String RAW_MV_COLUMN = "rawBigDecimalMV";
  // Used to exercise CAST(... AS BIG_DECIMAL_ARRAY) and implicit conversion paths.
  private static final String DOUBLE_MV_COLUMN = "doubleMV";

  private static final String[] SV_COLUMNS = {SV_COLUMN, RAW_SV_COLUMN};
  private static final String[] MV_COLUMNS = {MV_COLUMN, RAW_MV_COLUMN};

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
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
    return List.of(RAW_SV_COLUMN, RAW_MV_COLUMN);
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder()
        .setSchemaName(getTableName())
        .addSingleValueDimension(ID_COLUMN, DataType.INT)
        .addSingleValueDimension(SV_COLUMN, DataType.BIG_DECIMAL)
        .addSingleValueDimension(RAW_SV_COLUMN, DataType.BIG_DECIMAL)
        .addMultiValueDimension(MV_COLUMN, DataType.BIG_DECIMAL)
        .addMultiValueDimension(RAW_MV_COLUMN, DataType.BIG_DECIMAL)
        .addMultiValueDimension(DOUBLE_MV_COLUMN, DataType.DOUBLE)
        .build();
  }

  /**
   * For doc {@code i}, SV = {@code i.5} and MV = {@code [i.0, i.25, i.5]}. The values are chosen so that min/max and
   * sum-style aggregations yield exact BigDecimal results without double-precision rounding.
   */
  private static BigDecimal svValue(int i) {
    return new BigDecimal(i + ".5");
  }

  private static BigDecimal[] mvValues(int i) {
    return new BigDecimal[]{
        new BigDecimal(i + ".00"),
        new BigDecimal(i + ".25"),
        new BigDecimal(i + ".50")
    };
  }

  @Override
  public List<File> createAvroFiles()
      throws IOException {
    // BIG_DECIMAL ingestion goes through string → BigDecimal conversion in the transform pipeline, so we encode
    // BigDecimal values as Avro STRING / STRING_ARRAY rather than fighting with Avro's logical DECIMAL type.
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    org.apache.avro.Schema stringSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING);
    org.apache.avro.Schema doubleSchema = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.DOUBLE);
    avroSchema.setFields(List.of(
        new org.apache.avro.Schema.Field(ID_COLUMN, org.apache.avro.Schema.create(org.apache.avro.Schema.Type.INT),
            null, null),
        new org.apache.avro.Schema.Field(SV_COLUMN, stringSchema, null, null),
        new org.apache.avro.Schema.Field(RAW_SV_COLUMN, stringSchema, null, null),
        new org.apache.avro.Schema.Field(MV_COLUMN, org.apache.avro.Schema.createArray(stringSchema), null, null),
        new org.apache.avro.Schema.Field(RAW_MV_COLUMN, org.apache.avro.Schema.createArray(stringSchema), null, null),
        new org.apache.avro.Schema.Field(DOUBLE_MV_COLUMN, org.apache.avro.Schema.createArray(doubleSchema), null, null)
    ));

    try (AvroFilesAndWriters avroFilesAndWriters = createAvroFilesAndWriters(avroSchema)) {
      List<DataFileWriter<GenericData.Record>> writers = avroFilesAndWriters.getWriters();
      for (int i = 0; i < NUM_DOCS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(ID_COLUMN, i);
        String svText = svValue(i).toPlainString();
        record.put(SV_COLUMN, svText);
        record.put(RAW_SV_COLUMN, svText);
        List<String> mvStrings = new ArrayList<>(MV_LENGTH);
        List<Double> mvDoubles = new ArrayList<>(MV_LENGTH);
        for (BigDecimal v : mvValues(i)) {
          mvStrings.add(v.toPlainString());
          mvDoubles.add(v.doubleValue());
        }
        record.put(MV_COLUMN, mvStrings);
        record.put(RAW_MV_COLUMN, mvStrings);
        record.put(DOUBLE_MV_COLUMN, mvDoubles);
        writers.get(i % getNumAvroFiles()).append(record);
      }
      return avroFilesAndWriters.getAvroFiles();
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSvProjection(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (String svCol : SV_COLUMNS) {
      String query = String.format("SELECT %s, %s FROM %s WHERE %s = 42 LIMIT 1", ID_COLUMN, svCol, getTableName(),
          ID_COLUMN);
      JsonNode rows = postQuery(query).get("resultTable").get("rows");
      assertEquals(rows.size(), 1);
      JsonNode row = rows.get(0);
      assertEquals(row.get(0).asInt(), 42);
      assertEquals(new BigDecimal(row.get(1).asText()), svValue(42));
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testMvProjection(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    BigDecimal[] expected = mvValues(7);
    for (String mvCol : MV_COLUMNS) {
      String query = String.format("SELECT %s FROM %s WHERE %s = 7 LIMIT 1", mvCol, getTableName(), ID_COLUMN);
      JsonNode rows = postQuery(query).get("resultTable").get("rows");
      assertEquals(rows.size(), 1);
      JsonNode mv = rows.get(0).get(0);
      assertEquals(mv.size(), MV_LENGTH);
      // Use compareTo (scale-insensitive) because BIG_DECIMAL columns strip trailing zeros by default; "7.00" comes
      // back as "7" and "7.50" as "7.5". Stored-vs-expected equivalence is numeric, not textual.
      for (int i = 0; i < MV_LENGTH; i++) {
        assertEquals(new BigDecimal(mv.get(i).asText()).compareTo(expected[i]), 0);
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSvFilter(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Match docs where the SV BigDecimal exceeds 10.5, which is i >= 11.
    for (String svCol : SV_COLUMNS) {
      String query = String.format("SELECT count(*) FROM %s WHERE %s > 10.5", getTableName(), svCol);
      JsonNode rows = postQuery(query).get("resultTable").get("rows");
      assertEquals(rows.get(0).get(0).asLong(), NUM_DOCS - 11);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testMvFilter(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // MV predicate matches a doc if any element of the array satisfies. For doc i, mvValues contain i.0/i.25/i.5, so
    // the largest element is i.5; filter bigDecimalMV >= 10.5 matches i >= 10. MSE requires ARRAY_TO_MV() to compare
    // an MV column to a scalar; SSE accepts the direct MV comparison.
    for (String mvCol : MV_COLUMNS) {
      String mvExpr = useMultiStageQueryEngine ? String.format("ARRAY_TO_MV(%s)", mvCol) : mvCol;
      String query = String.format("SELECT count(*) FROM %s WHERE %s >= 10.5", getTableName(), mvExpr);
      JsonNode rows = postQuery(query).get("resultTable").get("rows");
      assertEquals(rows.get(0).get(0).asLong(), NUM_DOCS - 10);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSvAggregation(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    BigDecimal expectedSum = BigDecimal.ZERO;
    for (int i = 0; i < NUM_DOCS; i++) {
      expectedSum = expectedSum.add(svValue(i));
    }
    for (String svCol : SV_COLUMNS) {
      String query = String.format("SELECT min(%s), max(%s), sumPrecision(%s) FROM %s",
          svCol, svCol, svCol, getTableName());
      JsonNode row = postQuery(query).get("resultTable").get("rows").get(0);
      // Stored values are 0.5, 1.5, ..., 99.5
      assertEquals(new BigDecimal(row.get(0).asText()), svValue(0));
      assertEquals(new BigDecimal(row.get(1).asText()), svValue(NUM_DOCS - 1));
      assertEquals(new BigDecimal(row.get(2).asText()), expectedSum);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testMvMinMax(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (String mvCol : MV_COLUMNS) {
      String query = String.format("SELECT minMV(%s), maxMV(%s) FROM %s", mvCol, mvCol, getTableName());
      JsonNode row = postQuery(query).get("resultTable").get("rows").get(0);
      // minMV picks the smallest element across all (doc, element) pairs => 0.00 (doc 0, first element).
      // maxMV picks the largest => 99.50 (doc 99, last element).
      assertEquals(new BigDecimal(row.get(0).asText()).compareTo(new BigDecimal("0.00")), 0);
      assertEquals(new BigDecimal(row.get(1).asText()).compareTo(new BigDecimal("99.50")), 0);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testMvCardinality(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // CARDINALITY on an MV column returns the number of elements per row.
    for (String mvCol : MV_COLUMNS) {
      String query = String.format("SELECT cardinality(%s) FROM %s WHERE %s = 0 LIMIT 1", mvCol, getTableName(),
          ID_COLUMN);
      JsonNode rows = postQuery(query).get("resultTable").get("rows");
      assertEquals(rows.get(0).get(0).asInt(), MV_LENGTH);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGroupBySv(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (String svCol : SV_COLUMNS) {
      String query =
          String.format("SELECT %s, count(*) FROM %s GROUP BY %s ORDER BY %s LIMIT %d", svCol, getTableName(),
              svCol, svCol, NUM_DOCS);
      JsonNode rows = postQuery(query).get("resultTable").get("rows");
      assertEquals(rows.size(), NUM_DOCS);
      // Each SV value is unique, so every group has exactly one row.
      for (int i = 0; i < NUM_DOCS; i++) {
        assertEquals(new BigDecimal(rows.get(i).get(0).asText()), svValue(i));
        assertEquals(rows.get(i).get(1).asLong(), 1L);
      }
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testCastMvToBigDecimal(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // MSE uses standard Calcite `TYPE ARRAY` syntax; SSE uses Pinot's underscored `BIG_DECIMAL_ARRAY` identifier
    // understood by CastTransformFunction.
    String castTargetType = useMultiStageQueryEngine ? "DECIMAL ARRAY" : "BIG_DECIMAL_ARRAY";
    String query = String.format(
        "SELECT cardinality(cast(%s as %s)) FROM %s WHERE %s = 0 LIMIT 1",
        DOUBLE_MV_COLUMN, castTargetType, getTableName(), ID_COLUMN);
    JsonNode rows = postQuery(query).get("resultTable").get("rows");
    assertEquals(rows.get(0).get(0).asInt(), MV_LENGTH);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testSelectStar(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // A plain select-all to verify schema reporting for BIG_DECIMAL / BIG_DECIMAL_ARRAY columns round-trips cleanly.
    String query = String.format("SELECT * FROM %s WHERE %s = 0 LIMIT 1", getTableName(), ID_COLUMN);
    JsonNode result = postQuery(query).get("resultTable");
    JsonNode rows = result.get("rows");
    assertEquals(rows.size(), 1);
    assertTrue(result.get("dataSchema").get("columnDataTypes").toString().contains("BIG_DECIMAL"));
  }
}
