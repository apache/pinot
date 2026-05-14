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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Integration test that verifies a column with RAW forward index encoding plus an explicitly configured dictionary
 * returns identical query results to a baseline column that uses the default dictionary-encoded forward index.
 * Three column shapes are exercised so the predicate-evaluator selection logic is covered for each:
 * <ul>
 *   <li>{@code rawDictDim}: RAW forward + explicit dictionary, no secondary index — exercises the scan path that
 *       requires dropping the dictionary so the raw-value evaluator is used.</li>
 *   <li>{@code rawDictInvDim}: RAW forward + explicit dictionary + inverted index — exercises the inverted-index
 *       path that requires keeping the dictionary so dict IDs can be looked up.</li>
 *   <li>{@code dictDim}: regular dictionary-encoded baseline.</li>
 * </ul>
 * All three columns get identical values so query outputs must match exactly across filters, aggregations,
 * GROUP BY, DISTINCT, IN, and REGEXP_LIKE predicates.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class RawForwardIndexWithDictionaryTest extends CustomDataQueryClusterIntegrationTest {
  private static final String TABLE_NAME = "RawForwardIndexWithDictionaryTest";
  private static final String DICT_DIMENSION = "dictDim";
  private static final String RAW_DICT_DIMENSION = "rawDictDim";
  private static final String RAW_DICT_INV_DIMENSION = "rawDictInvDim";
  private static final String DICT_INT_DIMENSION = "dictIntDim";
  private static final String RAW_DICT_INT_DIMENSION = "rawDictIntDim";
  private static final String RAW_DICT_INV_INT_DIMENSION = "rawDictInvIntDim";
  private static final String RAW_DICT_RANGE_INT_DIMENSION = "rawDictRangeIntDim";
  private static final String METRIC_COLUMN = "metric";
  private static final int ROW_COUNT = 1000;
  private static final int UNIQUE_DIMENSION_VALUES = 20;

  @Override
  protected long getCountStarResult() {
    return ROW_COUNT;
  }

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(getTableName())
        .setTimeColumnName(getTimeColumnName())
        .setNoDictionaryColumns(getNoDictionaryColumns())
        .setInvertedIndexColumns(getInvertedIndexColumns())
        .setRangeIndexColumns(getRangeIndexColumns())
        .setFieldConfigList(getFieldConfigs())
        .setNumReplicas(getNumReplicas())
        .setSegmentVersion(getSegmentVersion())
        .setLoadMode(getLoadMode())
        .setBrokerTenant(getBrokerTenant())
        .setServerTenant(getServerTenant())
        .setIngestionConfig(getIngestionConfig())
        .setQueryConfig(getQueryConfig())
        .setNullHandlingEnabled(getNullHandlingEnabled())
        .build();
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(DICT_DIMENSION, FieldSpec.DataType.STRING)
        .addSingleValueDimension(RAW_DICT_DIMENSION, FieldSpec.DataType.STRING)
        .addSingleValueDimension(RAW_DICT_INV_DIMENSION, FieldSpec.DataType.STRING)
        .addSingleValueDimension(DICT_INT_DIMENSION, FieldSpec.DataType.INT)
        .addSingleValueDimension(RAW_DICT_INT_DIMENSION, FieldSpec.DataType.INT)
        .addSingleValueDimension(RAW_DICT_INV_INT_DIMENSION, FieldSpec.DataType.INT)
        .addSingleValueDimension(RAW_DICT_RANGE_INT_DIMENSION, FieldSpec.DataType.INT)
        .addMetric(METRIC_COLUMN, FieldSpec.DataType.LONG)
        .addDateTime(TIMESTAMP_FIELD_NAME, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
  }

  @Override
  public List<File> createAvroFiles()
      throws Exception {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("RawForwardIndexWithDictionaryRecord").fields()
        .requiredString(DICT_DIMENSION)
        .requiredString(RAW_DICT_DIMENSION)
        .requiredString(RAW_DICT_INV_DIMENSION)
        .requiredInt(DICT_INT_DIMENSION)
        .requiredInt(RAW_DICT_INT_DIMENSION)
        .requiredInt(RAW_DICT_INV_INT_DIMENSION)
        .requiredInt(RAW_DICT_RANGE_INT_DIMENSION)
        .requiredLong(METRIC_COLUMN)
        .requiredLong(TIMESTAMP_FIELD_NAME)
        .endRecord();

    File avroFile = new File(_tempDir, "raw-forward-with-dictionary.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      Random random = new Random(1234);
      long currentTimeMillis = System.currentTimeMillis();
      for (int i = 0; i < ROW_COUNT; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);
        String stringValue = "value-" + (i % UNIQUE_DIMENSION_VALUES);
        int intValue = i % UNIQUE_DIMENSION_VALUES;
        // All three string columns and four int columns get IDENTICAL values so query results must match exactly.
        record.put(DICT_DIMENSION, stringValue);
        record.put(RAW_DICT_DIMENSION, stringValue);
        record.put(RAW_DICT_INV_DIMENSION, stringValue);
        record.put(DICT_INT_DIMENSION, intValue);
        record.put(RAW_DICT_INT_DIMENSION, intValue);
        record.put(RAW_DICT_INV_INT_DIMENSION, intValue);
        record.put(RAW_DICT_RANGE_INT_DIMENSION, intValue);
        record.put(METRIC_COLUMN, random.nextInt(10_000));
        record.put(TIMESTAMP_FIELD_NAME, currentTimeMillis + i);
        fileWriter.append(record);
      }
    }
    return List.of(avroFile);
  }

  @Override
  protected List<String> getNoDictionaryColumns() {
    // Forward-index encoding for the raw-dict columns is set via FieldConfig (RAW + explicit dictionary).
    return List.of();
  }

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    ObjectNode dictionaryIndex = JsonUtils.newObjectNode();
    dictionaryIndex.set("dictionary", JsonUtils.newObjectNode());
    FieldConfig rawDictString =
        new FieldConfig(RAW_DICT_DIMENSION, FieldConfig.EncodingType.RAW, null, null, null, null, dictionaryIndex,
            null, null);
    FieldConfig rawDictInvString =
        new FieldConfig(RAW_DICT_INV_DIMENSION, FieldConfig.EncodingType.RAW, null, null, null, null,
            dictionaryIndex.deepCopy(), null, null);
    FieldConfig rawDictInt =
        new FieldConfig(RAW_DICT_INT_DIMENSION, FieldConfig.EncodingType.RAW, null, null, null, null,
            dictionaryIndex.deepCopy(), null, null);
    FieldConfig rawDictInvInt =
        new FieldConfig(RAW_DICT_INV_INT_DIMENSION, FieldConfig.EncodingType.RAW, null, null, null, null,
            dictionaryIndex.deepCopy(), null, null);
    FieldConfig rawDictRangeInt =
        new FieldConfig(RAW_DICT_RANGE_INT_DIMENSION, FieldConfig.EncodingType.RAW, null, null, null, null,
            dictionaryIndex.deepCopy(), null, null);
    return List.of(rawDictString, rawDictInvString, rawDictInt, rawDictInvInt, rawDictRangeInt);
  }

  @Override
  protected List<String> getInvertedIndexColumns() {
    return List.of(RAW_DICT_INV_DIMENSION, RAW_DICT_INV_INT_DIMENSION);
  }

  @Override
  protected List<String> getRangeIndexColumns() {
    return List.of(RAW_DICT_RANGE_INT_DIMENSION);
  }

  @Test
  public void testSegmentMetadataHasDictionaryAndRawForwardIndex()
      throws IOException {
    JsonNode segmentsMetadata = JsonUtils.stringToJsonNode(sendGetRequest(_controllerRequestURLBuilder
        .forSegmentsMetadataFromServer(getTableName(),
            List.of(RAW_DICT_DIMENSION, RAW_DICT_INV_DIMENSION, RAW_DICT_INT_DIMENSION,
                RAW_DICT_INV_INT_DIMENSION, RAW_DICT_RANGE_INT_DIMENSION))));
    assertFalse(segmentsMetadata.isEmpty());
    for (String column : List.of(RAW_DICT_DIMENSION, RAW_DICT_INV_DIMENSION, RAW_DICT_INT_DIMENSION,
        RAW_DICT_INV_INT_DIMENSION, RAW_DICT_RANGE_INT_DIMENSION)) {
      JsonNode columnMetadata = findColumnMetadata(segmentsMetadata, column);
      assertNotNull(columnMetadata, "Metadata for " + column + " not found");
      assertTrue(columnMetadata.get("hasDictionary").asBoolean(),
          "Dictionary should be built for explicitly configured dictionary on raw-encoded column: " + column);
    }
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testEqualityFilterReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (int i = 0; i < UNIQUE_DIMENSION_VALUES; i++) {
      String value = "value-" + i;
      assertScalarLongMatches(
          String.format("SELECT COUNT(*) FROM %s WHERE %s = '%s'", getTableName(), DICT_DIMENSION, value),
          String.format("SELECT COUNT(*) FROM %s WHERE %s = '%s'", getTableName(), RAW_DICT_DIMENSION, value));
      // RAW + dictionary + inverted index: dictionary must be preserved so the inverted-index path works.
      assertScalarLongMatches(
          String.format("SELECT COUNT(*) FROM %s WHERE %s = '%s'", getTableName(), DICT_DIMENSION, value),
          String.format("SELECT COUNT(*) FROM %s WHERE %s = '%s'", getTableName(), RAW_DICT_INV_DIMENSION, value));
      assertScalarLongMatches(
          String.format("SELECT COUNT(*) FROM %s WHERE %s = %d", getTableName(), DICT_INT_DIMENSION, i),
          String.format("SELECT COUNT(*) FROM %s WHERE %s = %d", getTableName(), RAW_DICT_INT_DIMENSION, i));
    }
  }

  @Test
  public void testEqualityFilterWithSkipInvertedIndex()
      throws Exception {
    // Force scan path for the inverted-indexed column. The planner must recognize that the inverted index is disabled
    // for this query and fall back to the raw-value evaluator instead of the dict-based one.
    setUseMultiStageQueryEngine(false);
    String prefix = "SET skipIndexes='" + RAW_DICT_INV_DIMENSION + "=inverted'; ";
    assertScalarLongMatches(
        prefix + String.format("SELECT COUNT(*) FROM %s WHERE %s = 'value-3'", getTableName(), DICT_DIMENSION),
        prefix + String.format("SELECT COUNT(*) FROM %s WHERE %s = 'value-3'", getTableName(), RAW_DICT_INV_DIMENSION));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testRegexpLikeReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // REGEXP_LIKE goes through a separate code path in FilterPlanNode and must also drop the dict for raw forward.
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE REGEXP_LIKE(%s, 'value-1.*')", getTableName(), DICT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE REGEXP_LIKE(%s, 'value-1.*')", getTableName(),
            RAW_DICT_DIMENSION));
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE REGEXP_LIKE(%s, 'value-1.*')", getTableName(), DICT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE REGEXP_LIKE(%s, 'value-1.*')", getTableName(),
            RAW_DICT_INV_DIMENSION));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testInequalityFilterReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE %s != 'value-3'", getTableName(), DICT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s != 'value-3'", getTableName(), RAW_DICT_DIMENSION));
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE %s > 5 AND %s < 15", getTableName(), DICT_INT_DIMENSION,
            DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s > 5 AND %s < 15", getTableName(), RAW_DICT_INT_DIMENSION,
            RAW_DICT_INT_DIMENSION));
  }

  /**
   * Regression test: a column with dict + inverted + RAW forward index used to crash on RANGE predicates.
   * The planner kept the dictionary because an inverted index existed, but RANGE predicates don't use the
   * inverted index — they fall through to the scan operator, which then called {@code applySV(rawValue)} on
   * the dict-based range evaluator and threw {@code UnsupportedOperationException}. The fix drops the dict
   * per-predicate-type so RANGE on RAW forward gets a raw-value evaluator.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testRangeOnRawDictInvertedColumnReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    // Column has: dict + inverted + RAW forward; query is RANGE -> must use scan with raw-value evaluator.
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE %s > 5 AND %s < 15", getTableName(), DICT_INT_DIMENSION,
            DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s > 5 AND %s < 15", getTableName(), RAW_DICT_INV_INT_DIMENSION,
            RAW_DICT_INV_INT_DIMENSION));
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE %s >= 10", getTableName(), DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s >= 10", getTableName(), RAW_DICT_INV_INT_DIMENSION));
  }

  /**
   * Mixed-predicate query on the same column: combines an EQ that uses the inverted index (dict path) with a
   * RANGE that scans (raw-value path). Each predicate gets its own evaluator, so dict-drop must be decided
   * per predicate, not per column.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testMixedInvertedEqAndRangeOnSameColumnReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE %s = 7 OR (%s > 12 AND %s < 18)", getTableName(),
            DICT_INT_DIMENSION, DICT_INT_DIMENSION, DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s = 7 OR (%s > 12 AND %s < 18)", getTableName(),
            RAW_DICT_INV_INT_DIMENSION, RAW_DICT_INV_INT_DIMENSION, RAW_DICT_INV_INT_DIMENSION));
  }

  /**
   * Comprehensive mixed-predicate matrix on the column with RAW forward + dictionary + inverted index.
   *
   * <p>Exercises every predicate type that takes the dict-consuming inverted-index path together with a RANGE
   * predicate that drops the dict and scans raw values.
   * {@code PredicateEvaluatorProvider#getDictionaryUsableForFiltering} decides per-predicate-type whether the
   * dictionary is preserved or dropped, so each combination below routes one
   * predicate through {@code InvertedIndexBasedFilterOperator} (with dict IDs) and another through
   * {@code ScanBasedFilterOperator} (with raw values) on the same physical column.
   *
   * <p>Predicate-to-operator mapping for the int column ({@link #RAW_DICT_INV_INT_DIMENSION}) used here:
   * <ul>
   *   <li>{@code EQ}, {@code NOT_EQ}, {@code IN}, {@code NOT_IN} → inverted index, dict preserved</li>
   *   <li>{@code RANGE} → scan, dict dropped, raw-value evaluator</li>
   * </ul>
   * Results are compared against the dictionary-encoded baseline column, which always uses the dict path.
   * The deterministic dataset has values {@code 0..19}, each appearing 50 times in 1000 rows. Each sub-case
   * also asserts an explicit non-zero expected count to defuse vacuous-pass risk if both sides silently
   * returned 0 (e.g. due to a planner short-circuit on a contradictory predicate).
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testAllPredicateTypesMixedWithRangeOnSameColumnReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);

    // EQ (inverted, dict) AND RANGE (scan, raw): {7} satisfies both -> 50 rows.
    assertScalarLongMatchesExpected(
        String.format("SELECT COUNT(*) FROM %s WHERE %s = 7 AND %s > 5", getTableName(),
            DICT_INT_DIMENSION, DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s = 7 AND %s > 5", getTableName(),
            RAW_DICT_INV_INT_DIMENSION, RAW_DICT_INV_INT_DIMENSION),
        50);

    // IN (inverted, dict) OR RANGE (scan, raw): {1,3,5} union {15,16,17,18} = 7 values * 50 = 350 rows.
    assertScalarLongMatchesExpected(
        String.format("SELECT COUNT(*) FROM %s WHERE %s IN (1, 3, 5) OR (%s >= 15 AND %s < 19)",
            getTableName(), DICT_INT_DIMENSION, DICT_INT_DIMENSION, DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s IN (1, 3, 5) OR (%s >= 15 AND %s < 19)",
            getTableName(), RAW_DICT_INV_INT_DIMENSION, RAW_DICT_INV_INT_DIMENSION, RAW_DICT_INV_INT_DIMENSION),
        350);

    // NOT_EQ (inverted, dict) AND RANGE (scan, raw): not-{9} intersect {7..12} = {7,8,10,11,12} -> 5 * 50 = 250 rows.
    assertScalarLongMatchesExpected(
        String.format("SELECT COUNT(*) FROM %s WHERE %s != 9 AND %s BETWEEN 7 AND 12", getTableName(),
            DICT_INT_DIMENSION, DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s != 9 AND %s BETWEEN 7 AND 12", getTableName(),
            RAW_DICT_INV_INT_DIMENSION, RAW_DICT_INV_INT_DIMENSION),
        250);

    // NOT_IN (inverted, dict) AND RANGE (scan, raw):
    //   not-{2,4,6} intersect {0..10} = {0,1,3,5,7,8,9,10} -> 8 * 50 = 400 rows.
    assertScalarLongMatchesExpected(
        String.format("SELECT COUNT(*) FROM %s WHERE %s NOT IN (2, 4, 6) AND %s <= 10", getTableName(),
            DICT_INT_DIMENSION, DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s NOT IN (2, 4, 6) AND %s <= 10", getTableName(),
            RAW_DICT_INV_INT_DIMENSION, RAW_DICT_INV_INT_DIMENSION),
        400);

    // Three-way mix on the same column: EQ via inverted, NOT_IN via inverted, RANGE via scan.
    //   ({0} union not-{1,2,3}) intersect {5..19} = {5..19} -> 15 * 50 = 750 rows.
    assertScalarLongMatchesExpected(
        String.format("SELECT COUNT(*) FROM %s WHERE (%s = 0 OR %s NOT IN (1, 2, 3)) AND %s > 4",
            getTableName(), DICT_INT_DIMENSION, DICT_INT_DIMENSION, DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE (%s = 0 OR %s NOT IN (1, 2, 3)) AND %s > 4",
            getTableName(), RAW_DICT_INV_INT_DIMENSION, RAW_DICT_INV_INT_DIMENSION, RAW_DICT_INV_INT_DIMENSION),
        750);
  }

  /**
   * Mixed-predicate query on the STRING column with RAW forward + dictionary + inverted index. Validates that
   * REGEXP_LIKE (inverted-index, dict-id evaluator) and string EQ on the same column both return correct results
   * and can be combined in a single query.
   *
   * <p>Note: there is no string analogue of RANGE that drops the dict in this column shape (no raw-value evaluator
   * is exercised here), so this is a sanity check for the inverted-index path on strings — including the
   * REGEXP_LIKE-on-RAW-forward path that the dict-drop logic enables.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testMixedRegexpAndEqOnStringRawDictInvertedColumnReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE %s = 'value-1' OR REGEXP_LIKE(%s, 'value-1.*')",
            getTableName(), DICT_DIMENSION, DICT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s = 'value-1' OR REGEXP_LIKE(%s, 'value-1.*')",
            getTableName(), RAW_DICT_INV_DIMENSION, RAW_DICT_INV_DIMENSION));
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE %s != 'value-3' AND REGEXP_LIKE(%s, 'value-1.*')",
            getTableName(), DICT_DIMENSION, DICT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s != 'value-3' AND REGEXP_LIKE(%s, 'value-1.*')",
            getTableName(), RAW_DICT_INV_DIMENSION, RAW_DICT_INV_DIMENSION));
  }

  /**
   * Cross-column mixed-predicate query: the predicates target different physical columns but each routes through
   * a different operator within the same query plan. Validates that the planner does not entangle the per-column
   * dict-drop decisions: the EQ on the inverted+dict+raw column takes the inverted-index path (dict preserved),
   * while the two RANGE predicates on a raw+dict column with and without an inverted index both take the scan
   * path (dict dropped). The dict-encoded baseline column is run as a separate query and supplies the expected
   * value of 50 rows ({@code dim = 7} matching once per appearance).
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testMixedPredicatesAcrossDifferentColumnShapesReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertScalarLongMatchesExpected(
        String.format("SELECT COUNT(*) FROM %s WHERE %s = 7 AND %s > 3 AND %s < 17",
            getTableName(), DICT_INT_DIMENSION, DICT_INT_DIMENSION, DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s = 7 AND %s > 3 AND %s < 17",
            getTableName(), RAW_DICT_INV_INT_DIMENSION, RAW_DICT_INV_INT_DIMENSION, RAW_DICT_INT_DIMENSION),
        50);
  }

  /**
   * Column with dict + RAW forward + range index. {@code RangeIndexType#createIndexCreator} chooses the
   * dict-id-based range index when a dictionary exists at segment build time, so the planner must keep the dict
   * for RANGE predicates and the range-index path is used.
   *
   * <p>Note: this test covers the healthy case where dict and range-index were built together. The pre-existing
   * {@link org.apache.pinot.segment.local.segment.index.readers.BitSlicedRangeIndexReader} assumes dict ↔ dict-based
   * range; a segment built with raw-value range index that later gains a dictionary (e.g. via reload without range
   * rebuild) is not a supported state and falls outside this fix's scope.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testRangeOnRawDictRangeColumnReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE %s > 5 AND %s < 15", getTableName(), DICT_INT_DIMENSION,
            DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s > 5 AND %s < 15", getTableName(),
            RAW_DICT_RANGE_INT_DIMENSION, RAW_DICT_RANGE_INT_DIMENSION));
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE %s >= 10", getTableName(), DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s >= 10", getTableName(), RAW_DICT_RANGE_INT_DIMENSION));
  }

  /**
   * Equality on a range-indexed column. Whether range index serves EQ depends on
   * {@link org.apache.pinot.segment.spi.index.reader.RangeIndexReader#isExact()}; the planner reasons over that
   * directly. Either path (range-index or scan) must produce results identical to the baseline.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testEqualityOnRawDictRangeColumnReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    for (int i = 0; i < UNIQUE_DIMENSION_VALUES; i++) {
      assertScalarLongMatches(
          String.format("SELECT COUNT(*) FROM %s WHERE %s = %d", getTableName(), DICT_INT_DIMENSION, i),
          String.format("SELECT COUNT(*) FROM %s WHERE %s = %d", getTableName(), RAW_DICT_RANGE_INT_DIMENSION, i));
    }
  }

  /**
   * Range index disabled via {@code skipIndexes}. The planner must drop the dictionary so the scan path uses the
   * raw-value evaluator instead of the dict-based one.
   */
  @Test
  public void testRangeOnRawDictRangeColumnWithSkipRangeIndex()
      throws Exception {
    setUseMultiStageQueryEngine(false);
    String prefix = "SET skipIndexes='" + RAW_DICT_RANGE_INT_DIMENSION + "=range'; ";
    assertScalarLongMatches(
        prefix + String.format("SELECT COUNT(*) FROM %s WHERE %s > 5 AND %s < 15", getTableName(), DICT_INT_DIMENSION,
            DICT_INT_DIMENSION),
        prefix + String.format("SELECT COUNT(*) FROM %s WHERE %s > 5 AND %s < 15", getTableName(),
            RAW_DICT_RANGE_INT_DIMENSION, RAW_DICT_RANGE_INT_DIMENSION));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testInPredicateReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE %s IN ('value-1','value-7','value-13')", getTableName(),
            DICT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s IN ('value-1','value-7','value-13')", getTableName(),
            RAW_DICT_DIMENSION));
    assertScalarLongMatches(
        String.format("SELECT COUNT(*) FROM %s WHERE %s IN (2, 4, 6, 8)", getTableName(), DICT_INT_DIMENSION),
        String.format("SELECT COUNT(*) FROM %s WHERE %s IN (2, 4, 6, 8)", getTableName(), RAW_DICT_INT_DIMENSION));
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testGroupByReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode dictRows = postQuery(
        String.format("SELECT %s, COUNT(*) FROM %s GROUP BY %s ORDER BY %s", DICT_DIMENSION, getTableName(),
            DICT_DIMENSION, DICT_DIMENSION)).get("resultTable").get("rows");
    JsonNode rawRows = postQuery(
        String.format("SELECT %s, COUNT(*) FROM %s GROUP BY %s ORDER BY %s", RAW_DICT_DIMENSION, getTableName(),
            RAW_DICT_DIMENSION, RAW_DICT_DIMENSION)).get("resultTable").get("rows");
    assertEquals(rawRows, dictRows, "GROUP BY rows must match between dictionary-only and raw+dictionary columns");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testDistinctReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode dictRows = postQuery(
        String.format("SELECT DISTINCT %s FROM %s ORDER BY %s", DICT_DIMENSION, getTableName(), DICT_DIMENSION))
        .get("resultTable").get("rows");
    JsonNode rawRows = postQuery(
        String.format("SELECT DISTINCT %s FROM %s ORDER BY %s", RAW_DICT_DIMENSION, getTableName(),
            RAW_DICT_DIMENSION)).get("resultTable").get("rows");
    assertEquals(rawRows, dictRows, "DISTINCT rows must match between dictionary-only and raw+dictionary columns");
  }

  /**
   * Multi-column GROUP BY that mixes a dict-encoded column with a RAW+dictionary column. This forces the executor
   * onto the {@code NoDictionaryMultiColumnGroupKeyGenerator} path. The per-column branch there must check the
   * forward-index encoding in addition to {@code ColumnContext#getDictionary} — otherwise it keeps the dictionary
   * for any column that has a dict file and calls {@code BlockValSet#getDictionaryIdsSV()} on it, which routes to
   * {@code ForwardIndexReader#readDictIds} and throws {@code UnsupportedOperationException} on a RAW forward index.
   */
  @Test(dataProvider = "useBothQueryEngines")
  public void testMultiColumnGroupByWithRawDictColumnReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode dictRows = postQuery(
        String.format("SELECT %s, %s, COUNT(*) FROM %s GROUP BY %s, %s ORDER BY %s, %s",
            DICT_DIMENSION, DICT_INT_DIMENSION, getTableName(),
            DICT_DIMENSION, DICT_INT_DIMENSION, DICT_DIMENSION, DICT_INT_DIMENSION))
        .get("resultTable").get("rows");
    JsonNode rawRows = postQuery(
        String.format("SELECT %s, %s, COUNT(*) FROM %s GROUP BY %s, %s ORDER BY %s, %s",
            RAW_DICT_DIMENSION, RAW_DICT_INT_DIMENSION, getTableName(),
            RAW_DICT_DIMENSION, RAW_DICT_INT_DIMENSION, RAW_DICT_DIMENSION, RAW_DICT_INT_DIMENSION))
        .get("resultTable").get("rows");
    assertEquals(rawRows, dictRows,
        "Multi-column GROUP BY rows must match between dictionary-only and raw+dictionary columns");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testAggregationWithGroupByOnRawDictColumnReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode dictRows = postQuery(
        String.format("SELECT %s, SUM(%s), MIN(%s), MAX(%s) FROM %s GROUP BY %s ORDER BY %s", DICT_DIMENSION,
            METRIC_COLUMN, METRIC_COLUMN, METRIC_COLUMN, getTableName(), DICT_DIMENSION, DICT_DIMENSION))
        .get("resultTable").get("rows");
    JsonNode rawRows = postQuery(
        String.format("SELECT %s, SUM(%s), MIN(%s), MAX(%s) FROM %s GROUP BY %s ORDER BY %s", RAW_DICT_DIMENSION,
            METRIC_COLUMN, METRIC_COLUMN, METRIC_COLUMN, getTableName(), RAW_DICT_DIMENSION, RAW_DICT_DIMENSION))
        .get("resultTable").get("rows");
    assertEquals(rawRows, dictRows,
        "Aggregations grouped by raw+dictionary column must match the dictionary-only baseline");
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testProjectionReturnsSameResults(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    JsonNode dictRows = postQuery(
        String.format("SELECT %s FROM %s ORDER BY %s, %s LIMIT 100", DICT_DIMENSION, getTableName(),
            TIMESTAMP_FIELD_NAME, DICT_DIMENSION)).get("resultTable").get("rows");
    JsonNode rawRows = postQuery(
        String.format("SELECT %s FROM %s ORDER BY %s, %s LIMIT 100", RAW_DICT_DIMENSION, getTableName(),
            TIMESTAMP_FIELD_NAME, RAW_DICT_DIMENSION)).get("resultTable").get("rows");
    assertEquals(rawRows.size(), dictRows.size(), "Projection row counts must match");
    for (int i = 0; i < dictRows.size(); i++) {
      assertEquals(rawRows.get(i).get(0).asText(), dictRows.get(i).get(0).asText(),
          "Projected value at row " + i + " must match between dictionary-only and raw+dictionary columns");
    }
  }

  private void assertScalarLongMatches(String dictQuery, String rawQuery)
      throws Exception {
    long dictResult = scalarLong(dictQuery);
    long rawResult = scalarLong(rawQuery);
    assertEquals(rawResult, dictResult,
        "Result mismatch.\n  dict query: " + dictQuery + " => " + dictResult + "\n  raw query:  " + rawQuery
            + " => " + rawResult);
  }

  /// Like {@link #assertScalarLongMatches} but additionally pins the expected count, so the assertion does not
  /// pass vacuously if both sides silently return 0 (e.g. due to a planner short-circuit on a
  /// contradictory predicate).
  private void assertScalarLongMatchesExpected(String dictQuery, String rawQuery, long expected)
      throws Exception {
    long dictResult = scalarLong(dictQuery);
    long rawResult = scalarLong(rawQuery);
    assertEquals(dictResult, expected,
        "Dict baseline does not match expected count.\n  dict query: " + dictQuery + " => " + dictResult
            + " (expected " + expected + ")");
    assertEquals(rawResult, expected,
        "Raw result does not match expected count.\n  raw query:  " + rawQuery + " => " + rawResult
            + " (expected " + expected + ")");
  }

  private long scalarLong(String query)
      throws Exception {
    JsonNode response = postQuery(query);
    JsonNode resultTable = response.get("resultTable");
    if (resultTable == null) {
      throw new AssertionError("Query failed: " + query + "\nResponse: " + response.toString());
    }
    return resultTable.get("rows").get(0).get(0).asLong();
  }

  private static JsonNode findColumnMetadata(JsonNode segmentsMetadata, String column) {
    for (JsonNode segmentMetadata : segmentsMetadata) {
      JsonNode columnsMetadata = segmentMetadata.get("columns");
      if (columnsMetadata == null) {
        continue;
      }
      for (JsonNode columnMetadata : columnsMetadata) {
        if (column.equals(columnMetadata.get("columnName").asText())) {
          return columnMetadata;
        }
      }
    }
    return null;
  }
}
