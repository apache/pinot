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
package org.apache.pinot.core.query.aggregation;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.request.context.ExpressionContext;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for DefaultAggregationExecutor class.
 * - Builds a segment with random data.
 * - Uses DefaultAggregationExecutor class to perform various aggregations on the
 *   data.
 * - Also computes those aggregations itself.
 * - Asserts that the aggregation results returned by the class are the same as
 *   returned by the local computations.
 *
 * Currently tests 'sum', 'min' & 'max' functions, and can be easily extended to
 * test other functions as well.
 * Asserts that aggregation results returned by the executor are as expected.
 */
@SuppressWarnings("rawtypes")
public class DefaultAggregationExecutorTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DefaultAggregationExecutorTest");
  private static final String SEGMENT_NAME = "TestAggregation";

  private static final String METRIC_PREFIX = "metric_";
  private static final String[] AGGREGATION_FUNCTIONS = {"sum", "max", "min"};

  private static final int NUM_METRIC_COLUMNS = AGGREGATION_FUNCTIONS.length;
  private static final double MAX_VALUE = Integer.MAX_VALUE;
  private static final int NUM_ROWS = 1000;

  public static IndexSegment _indexSegment;
  private Random _random;
  private String[] _columns;
  private QueryContext _queryContext;
  private double[][] _inputData;

  /**
   * Initializations prior to the test:
   * - Build a segment with metric columns (that will be aggregated) containing
   *  randomly generated data.
   *
   * @throws Exception
   */
  @BeforeClass
  public void setUp()
      throws Exception {
    _random = new Random(System.currentTimeMillis());

    int numColumns = AGGREGATION_FUNCTIONS.length;
    _inputData = new double[numColumns][NUM_ROWS];

    _columns = new String[numColumns];
    setupSegment();

    StringBuilder queryBuilder = new StringBuilder("SELECT");
    for (int i = 0; i < numColumns; i++) {
      queryBuilder.append(String.format(" %s(%s)", AGGREGATION_FUNCTIONS[i], _columns[i]));
      if (i != numColumns - 1) {
        queryBuilder.append(',');
      }
    }
    queryBuilder.append(" FROM testTable");
    _queryContext = QueryContextConverterUtils.getQueryContextFromSQL(queryBuilder.toString());
  }

  /**
   * Runs 'sum', 'min' & 'max' aggregation functions on the DefaultAggregationExecutor.
   * Asserts that the aggregation results returned by the executor are as expected.
   */
  @Test
  void testAggregation() {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    List<ExpressionContext> expressions = new ArrayList<>();
    for (String column : _indexSegment.getPhysicalColumnNames()) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
      expressions.add(ExpressionContext.forIdentifier(column));
    }
    int totalDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
    MatchAllFilterOperator matchAllFilterOperator = new MatchAllFilterOperator(totalDocs);
    DocIdSetOperator docIdSetOperator = new DocIdSetOperator(matchAllFilterOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    ProjectionOperator projectionOperator = new ProjectionOperator(dataSourceMap, docIdSetOperator);
    TransformOperator transformOperator = new TransformOperator(projectionOperator, expressions);
    TransformBlock transformBlock = transformOperator.nextBlock();
    AggregationFunction[] aggregationFunctions = _queryContext.getAggregationFunctions();
    assert aggregationFunctions != null;
    AggregationExecutor aggregationExecutor = new DefaultAggregationExecutor(aggregationFunctions);
    aggregationExecutor.aggregate(transformBlock);
    List<Object> result = aggregationExecutor.getResult();
    for (int i = 0; i < result.size(); i++) {
      double actual = (double) result.get(i);
      double expected = computeAggregation(AGGREGATION_FUNCTIONS[i], _inputData[i]);
      Assert.assertEquals(actual, expected,
          "Aggregation mis-match for function " + AGGREGATION_FUNCTIONS[i] + ", Expected: " + expected + " Actual: "
              + actual);
    }
  }

  /**
   * Helper method to setup the index segment on which to perform aggregation tests.
   * - Generates a segment with {@link #NUM_METRIC_COLUMNS} and {@link #NUM_ROWS}
   * - Random 'double' data filled in the metric columns. The data is also populated
   *   into the _inputData[], so it can be used to test the results.
   *
   * @throws Exception
   */
  private void setupSegment()
      throws Exception {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    SegmentGeneratorConfig config =
        new SegmentGeneratorConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build(),
            buildSchema());
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getAbsolutePath());

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();

      for (int j = 0; j < _columns.length; j++) {
        String metricName = _columns[j];
        double value = _random.nextDouble() * MAX_VALUE;
        _inputData[j][i] = value;
        map.put(metricName, value);
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.heap);
  }

  /**
   * Helper method to build schema for the segment on which aggregation tests will be run.
   *
   * @return
   */
  private Schema buildSchema() {
    Schema schema = new Schema();

    for (int i = 0; i < NUM_METRIC_COLUMNS; i++) {
      String metricName = METRIC_PREFIX + i;
      MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, FieldSpec.DataType.DOUBLE);
      schema.addField(metricFieldSpec);
      _columns[i] = metricName;
    }
    return schema;
  }

  /**
   * Helper method to compute aggregation on a given array of values.
   * @param functionName
   * @param values
   * @return
   */
  private double computeAggregation(String functionName, double[] values) {
    switch (functionName.toLowerCase()) {
      case "sum":
        return computeSum(values);

      case "max":
        return computeMax(values);

      case "min":
        return computeMin(values);

      default:
        throw new RuntimeException("Unsupported function " + functionName);
    }
  }

  /**
   * Helper method to compute sum of a given array of values.
   * @param values
   * @return
   */
  private double computeSum(double[] values) {
    double sum = 0.0;
    for (double value : values) {
      sum += value;
    }
    return sum;
  }

  /**
   * Helper method to compute max of a given array of values.
   * @param values
   * @return
   */
  private double computeMax(double[] values) {
    double max = Double.NEGATIVE_INFINITY;
    for (double value : values) {
      max = Math.max(max, value);
    }
    return max;
  }

  /**
   * Helper method to compute min of a given array of values.
   * @param values
   * @return
   */
  private double computeMin(double[] values) {
    double min = Double.POSITIVE_INFINITY;
    for (double value : values) {
      min = Math.min(min, value);
    }
    return min;
  }

  @AfterClass
  void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
