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
package org.apache.pinot.queries;

import static org.apache.pinot.core.query.aggregation.function.DistinctCountThetaSketchAggregationFunction.MergeFunction;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import joptsimple.internal.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link org.apache.pinot.core.query.aggregation.function.DistinctCountThetaSketchAggregationFunction}.
 * <ul>
 *   <li> Generates a segment with 3 dimension columns with low cardinality (to increase distinct count). </li>
 *   <li> Runs various queries and compares result of distinctCountThetaSketch against distinctCount function. </li>
 * </ul>
 */
public class DistinctCountThetaSketchTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DistinctCountThetaSketchTest");
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String THETA_SKETCH_COLUMN = "colTS";
  private static final String DISTINCT_COLUMN = "distinctColumn";

  private static final int NUM_ROWS = 1001;
  private static final int MAX_CARDINALITY = 5; // 3 columns will lead to at most 125 groups

  private static final long RANDOM_SEED = System.nanoTime();
  private static final Random RANDOM = new Random(RANDOM_SEED);

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @BeforeClass
  public void setup()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    File segmentFile = buildSegment(buildSchema());
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(segmentFile, ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void testAggregationPql() {
    testThetaSketches(false, false);
  }

  @Test
  public void testAggregationSql() {
    testThetaSketches(false, true);
  }

  @Test
  public void testGroupByPql() {
    testThetaSketches(true, false);
  }

  @Test
  public void testGroupBySql() {
    testThetaSketches(true, true);
  }

  @Test(expectedExceptions = BadQueryRequestException.class, dataProvider = "badQueries")
  public void testInvalidNoPredicates(final String query) {
    getBrokerResponseForSqlQuery(query);
  }

  @DataProvider(name = "badQueries")
  public Object[][] badQueries() {
    return new Object[][] {
        // need at least 4 arguments in agg func
        {"select distinctCountThetaSketch(colTS, 'nominalEntries=123', '$0') from testTable"},
        // substitution arguments should start at $1
        {"select distinctCountThetaSketch(colTS, 'nominalEntries=123', 'colA = 1', '$0') from testTable"},
        // substituting variable has numeric value higher than the number of predicates provided
        {"select distinctCountThetaSketch(colTS, 'nominalEntries=123', 'colA = 1', '$5') from testTable"},
        // SET_DIFF requires exactly 2 arguments
        {"select distinctCountThetaSketch(colTS, 'nominalEntries=123', 'colA = 1', 'SET_DIFF($1)') from testTable"},
        // invalid merging function
        {"select distinctCountThetaSketch(colTS, 'nominalEntries=123', 'colA = 1', 'asdf') from testTable"},
        // union with < 2 arguments
        {"select distinctCountThetaSketch(colTS, 'nominalEntries=123', 'colA = 1', 'SET_UNION($1)')"},
        // intersect with < 2 arguments
        {"select distinctCountThetaSketch(colTS, 'nominalEntries=123', 'colA = 1', 'SET_INTERSECT($1)')"}
    };
  }

  private void testThetaSketches(boolean groupBy, boolean sql) {
    String tsQuery, distinctQuery;
    String thetaSketchParams = "nominalEntries=1001";

    List<String> predicateStrings = Collections.singletonList("colA = 1");
    String substitution = "$1";
    String whereClause = Strings.join(predicateStrings, " or ");
    tsQuery = buildQuery(whereClause, thetaSketchParams, predicateStrings, substitution, groupBy, false);
    distinctQuery = buildQuery(whereClause, null, null, null, groupBy, false);
    testQuery(tsQuery, distinctQuery, groupBy, sql, false);

    tsQuery = buildQuery(whereClause, thetaSketchParams, predicateStrings, substitution, groupBy, true);
    testQuery(tsQuery, distinctQuery, groupBy, sql, true);

    // Test Intersection (AND)
    predicateStrings = Arrays.asList("colA = 1", "colB >= 2.0", "colC <> 'colC_1'");
    substitution = "SET_INTERSECT($1, $2, $3)";
    whereClause = Strings.join(predicateStrings, " and ");
    tsQuery = buildQuery(whereClause, thetaSketchParams, predicateStrings, substitution, groupBy, false);
    distinctQuery = buildQuery(whereClause, null, null, null, groupBy, false);
    testQuery(tsQuery, distinctQuery, groupBy, sql, false);

    tsQuery = buildQuery(whereClause, thetaSketchParams, predicateStrings, substitution, groupBy, true);
    testQuery(tsQuery, distinctQuery, groupBy, sql, true);

    // Test Union (OR)
    predicateStrings = Arrays.asList("colA = 1", "colB = 1.9");
    substitution = "SET_UNION($1, $2)";
    whereClause = Strings.join(predicateStrings, " or ");
    tsQuery = buildQuery(whereClause, thetaSketchParams, predicateStrings, substitution, groupBy, false);
    distinctQuery = buildQuery(whereClause, null, null, null, groupBy, false);
    testQuery(tsQuery, distinctQuery, groupBy, sql, false);

    tsQuery = buildQuery(whereClause, thetaSketchParams, predicateStrings, substitution, groupBy, true);
    testQuery(tsQuery, distinctQuery, groupBy, sql, true);

    // Test complex predicates
    predicateStrings = Arrays.asList("colA in (1, 2)", "colB not in (3.0)", "colC between 'colC_1' and 'colC_5'");
    // operator precedence. ORs are evaluated after ANDs
    substitution = "SET_UNION(SET_INTERSECT($1, $2), SET_INTERSECT($1, $3))";
    whereClause =
        predicateStrings.get(0) + " and " + predicateStrings.get(1) + " or " + predicateStrings.get(0) + " and "
            + predicateStrings.get(2);
    tsQuery = buildQuery(whereClause, thetaSketchParams, predicateStrings, substitution, groupBy, false);
    distinctQuery = buildQuery(whereClause, null, null, null, groupBy, false);
    testQuery(tsQuery, distinctQuery, groupBy, sql, false);

    tsQuery = buildQuery(whereClause, thetaSketchParams, predicateStrings, substitution, groupBy, true);
    testQuery(tsQuery, distinctQuery, groupBy, sql, true);
  }

  private void testQuery(String tsQuery, String distinctQuery, boolean groupBy, boolean sql, boolean raw) {
    BrokerResponseNative actualResponse =
        (sql) ? getBrokerResponseForSqlQuery(tsQuery) : getBrokerResponseForPqlQuery(tsQuery);

    BrokerResponseNative expectedResponse =
        (sql) ? getBrokerResponseForSqlQuery(distinctQuery) : getBrokerResponseForPqlQuery(distinctQuery);

    if (groupBy) {
      compareGroupBy(actualResponse, expectedResponse, sql, raw);
    } else {
      compareAggregation(actualResponse, expectedResponse, sql, raw);
    }
  }

  private void compareAggregation(BrokerResponseNative actualResponse, BrokerResponseNative expectedResponse,
      boolean sql, boolean raw) {
    if (sql) {
      compareSql(actualResponse, expectedResponse, raw);
    } else {
      compareAggregationPql(actualResponse, expectedResponse, raw);
    }
  }

  private void compareGroupBy(BrokerResponseNative actualResponse, BrokerResponseNative expectedResponse, boolean sql,
      boolean raw) {
    if (sql) {
      compareSql(actualResponse, expectedResponse, raw);
    } else {
      compareGroupByPql(actualResponse, expectedResponse, raw);
    }
  }

  private void compareAggregationPql(BrokerResponseNative actualResponse, BrokerResponseNative expectedResponse,
      boolean raw) {
    List<AggregationResult> actualResults = actualResponse.getAggregationResults();
    Assert.assertEquals(actualResults.size(), 1);
    double actual = getSketchValue((String) actualResults.get(0).getValue(), raw);

    List<AggregationResult> expectedResults = expectedResponse.getAggregationResults();
    double expected = Double.parseDouble((String) expectedResults.get(0).getValue());

    Assert.assertEquals(actual, expected, (expected * 0.1), // Allow for 10 % error.
        "Distinct count mismatch: actual: " + actual + "expected: " + expected + "seed:" + RANDOM_SEED);
  }

  private void compareSql(BrokerResponseNative actualResponse, BrokerResponseNative expectedResponse, boolean raw) {
    List<Object[]> actualRows = actualResponse.getResultTable().getRows();
    List<Object[]> expectedRows = expectedResponse.getResultTable().getRows();

    Assert.assertEquals(actualRows.size(), expectedRows.size());

    for (int i = 0; i < actualRows.size(); i++) {
      double actual = getSketchValue(actualRows.get(i)[0].toString(), raw);
      double expected = (Integer) expectedRows.get(i)[0];
      Assert.assertEquals(actual, expected);
    }
  }

  private void compareGroupByPql(BrokerResponseNative actualResponse, BrokerResponseNative expectedResponse,
      boolean raw) {
    AggregationResult actualResult = actualResponse.getAggregationResults().get(0);
    List<GroupByResult> actualGroupBy = actualResult.getGroupByResult();

    AggregationResult expectedResult = expectedResponse.getAggregationResults().get(0);
    List<GroupByResult> expectedGroupBy = expectedResult.getGroupByResult();

    Assert.assertEquals(actualGroupBy.size(), expectedGroupBy.size());
    for (int i = 0; i < actualGroupBy.size(); i++) {
      double actual = getSketchValue((String) actualGroupBy.get(i).getValue(), raw);
      double expected = Double.parseDouble((String) expectedGroupBy.get(i).getValue());

      Assert.assertEquals(actual, expected, (expected * 0.1), // Allow for 10 % error.
          "Distinct count mismatch: actual: " + actual + "expected: " + expected + "seed:" + RANDOM_SEED);
    }
  }

  private double getSketchValue(String value, boolean raw) {
    if (!raw) {
      return Double.parseDouble(value);
    }

    byte[] bytes = BytesUtils.toBytes(value);
    return Sketch.wrap(Memory.wrap(bytes)).getEstimate();
  }

  private String buildQuery(String whereClause, String thetaSketchParams, List<String> thetaSketchPredicates,
      String postAggregationExpression, boolean groupBy, boolean raw) {
    String column;
    String aggrFunction;
    boolean thetaSketch = (postAggregationExpression != null);

    if (thetaSketch) {
      aggrFunction = (raw) ? AggregationFunctionType.DISTINCTCOUNTRAWTHETASKETCH.getName()
          : AggregationFunctionType.DISTINCTCOUNTTHETASKETCH.getName();
      column = THETA_SKETCH_COLUMN;
    } else {
      aggrFunction = AggregationFunctionType.DISTINCTCOUNT.getName();
      column = DISTINCT_COLUMN;
    }

    StringBuilder sb = new StringBuilder("select ");
    sb.append(aggrFunction);
    sb.append("(");
    sb.append(column);

    if (thetaSketch) {
      sb.append(", ");

      sb.append("'");
      if (thetaSketchParams != null) {
        sb.append(thetaSketchParams.replace("'", "''"));
      }
      sb.append("', ");

      for (String predicate : thetaSketchPredicates) {
        sb.append('\'');
        sb.append(predicate.replace("'", "''"));
        sb.append('\'');
        sb.append(", ");
      }

      sb.append('\'');
      sb.append(postAggregationExpression.replace("'", "''"));
      sb.append('\'');
    }

    sb.append(") from ");

    sb.append(TABLE_NAME);
    sb.append(" where ");
    sb.append(whereClause);

    if (groupBy) {
      sb.append(" group by colA, colB");
    }

    return sb.toString();
  }

  @Override
  protected String getFilter() {
    return ""; // No filters required for this test.
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  protected File buildSegment(Schema schema)
      throws Exception {

    StringBuilder stringBuilder = new StringBuilder();
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);

    for (int i = 0; i < NUM_ROWS; i++) {
      stringBuilder.setLength(0);
      HashMap<String, Object> valueMap = new HashMap<>();

      int value = (i % (1 + RANDOM.nextInt(MAX_CARDINALITY)));
      valueMap.put("colA", value);
      stringBuilder.append(value);

      value = (i % (1 + RANDOM.nextInt(MAX_CARDINALITY)));
      valueMap.put("colB", (i % (1 + RANDOM.nextInt(MAX_CARDINALITY))));
      stringBuilder.append(value);

      String sValue = "colC" + "_" + (i % (1 + RANDOM.nextInt(MAX_CARDINALITY)));
      valueMap.put("colC", sValue);
      stringBuilder.append(sValue);

      String distinctValue = stringBuilder.toString();
      valueMap.put(DISTINCT_COLUMN, distinctValue);

      UpdateSketch sketch = new UpdateSketchBuilder().build();
      sketch.update(distinctValue);
      valueMap.put(THETA_SKETCH_COLUMN, sketch.compact().toByteArray());

      GenericRow genericRow = new GenericRow();
      genericRow.init(valueMap);
      rows.add(genericRow);
    }

    SegmentGeneratorConfig config =
        new SegmentGeneratorConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build(), schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    config.setRawIndexCreationColumns(Collections.singletonList(THETA_SKETCH_COLUMN));
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();

    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }

    return driver.getOutputDirectory();
  }

  private Schema buildSchema() {
    Schema schema = new Schema();

    schema.addField(new DimensionFieldSpec("colA", FieldSpec.DataType.INT, true));
    schema.addField(new DimensionFieldSpec("colB", FieldSpec.DataType.DOUBLE, true));
    schema.addField(new DimensionFieldSpec("colC", FieldSpec.DataType.STRING, true));

    schema.addField(new DimensionFieldSpec(DISTINCT_COLUMN, FieldSpec.DataType.STRING, true));
    schema.addField(new MetricFieldSpec(THETA_SKETCH_COLUMN, FieldSpec.DataType.BYTES));
    return schema;
  }
}
