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
package org.apache.pinot.query.aggregation;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.common.request.AggregationInfo;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.plan.AggregationFunctionInitializer;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.AggregationExecutor;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.DefaultAggregationExecutor;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
public class DefaultAggregationExecutorTest {
  protected static Logger LOGGER = LoggerFactory.getLogger(DefaultAggregationExecutorTest.class);
  private static File INDEX_DIR = new File(FileUtils.getTempDirectory() + File.separator + "AggregationExecutorTest");
  private static final String SEGMENT_NAME = "TestAggregation";

  private static final String METRIC_PREFIX = "metric_";
  private static final String[] AGGREGATION_FUNCTIONS = {"sum", "max", "min"};

  private static final int NUM_METRIC_COLUMNS = AGGREGATION_FUNCTIONS.length;
  private static final double MAX_VALUE = Integer.MAX_VALUE;
  private static final int NUM_ROWS = 1000;

  public static IndexSegment _indexSegment;
  private Random _random;
  private List<AggregationInfo> _aggregationInfoList;
  private String[] _columns;
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

    _aggregationInfoList = new ArrayList<>();

    for (int i = 0; i < _columns.length; i++) {
      AggregationInfo aggregationInfo = new AggregationInfo();
      aggregationInfo.setAggregationType(AGGREGATION_FUNCTIONS[i]);

      Map<String, String> params = new HashMap<>();
      params.put("column", _columns[i]);

      aggregationInfo.setAggregationParams(params);
      _aggregationInfoList.add(aggregationInfo);
    }
  }

  /**
   * Runs 'sum', 'min' & 'max' aggregation functions on the DefaultAggregationExecutor.
   * Asserts that the aggregation results returned by the executor are as expected.
   */
  @Test
  void testAggregation() {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    Set<TransformExpressionTree> expressionTrees = new HashSet<>();
    for (String column : _indexSegment.getPhysicalColumnNames()) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
      expressionTrees.add(TransformExpressionTree.compileToExpressionTree(column));
    }
    int totalRawDocs = _indexSegment.getSegmentMetadata().getTotalRawDocs();
    MatchAllFilterOperator matchAllFilterOperator = new MatchAllFilterOperator(totalRawDocs);
    DocIdSetOperator docIdSetOperator = new DocIdSetOperator(matchAllFilterOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    ProjectionOperator projectionOperator = new ProjectionOperator(dataSourceMap, docIdSetOperator);
    TransformOperator transformOperator = new TransformOperator(projectionOperator, expressionTrees);
    TransformBlock transformBlock = transformOperator.nextBlock();
    int numAggFuncs = _aggregationInfoList.size();
    AggregationFunctionContext[] aggrFuncContextArray = new AggregationFunctionContext[numAggFuncs];
    AggregationFunctionInitializer aggFuncInitializer =
        new AggregationFunctionInitializer(_indexSegment.getSegmentMetadata());
    for (int i = 0; i < numAggFuncs; i++) {
      AggregationInfo aggregationInfo = _aggregationInfoList.get(i);
      aggrFuncContextArray[i] = AggregationFunctionUtils.getAggregationFunctionContext(aggregationInfo);
      aggrFuncContextArray[i].getAggregationFunction().accept(aggFuncInitializer);
    }
    AggregationExecutor aggregationExecutor = new DefaultAggregationExecutor(aggrFuncContextArray);
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

    SegmentGeneratorConfig config = new SegmentGeneratorConfig();
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getAbsolutePath());

    Schema schema = buildSchema();
    config.setSchema(schema);

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
    driver.init(config, new GenericRowRecordReader(rows, schema));
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
