/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.query.aggregation;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.operator.transform.TransformExpressionOperator;
import com.linkedin.pinot.core.plan.AggregationFunctionInitializer;
import com.linkedin.pinot.core.query.aggregation.AggregationExecutor;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.DefaultAggregationExecutor;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
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
@Test
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
  private int[] _docIdSet;
  private double[][] _inputData;

  /**
   * Initializations prior to the test:
   * - Build a segment with metric columns (that will be aggregated) containing
   *  randomly generated data.
   *
   * @throws Exception
   */
  @BeforeSuite
  void init()
      throws Exception {

    _random = new Random(System.currentTimeMillis());
    _docIdSet = new int[NUM_ROWS];

    int numColumns = AGGREGATION_FUNCTIONS.length;
    _inputData = new double[numColumns][NUM_ROWS];

    _columns = new String[numColumns];
    setupSegment();

    _aggregationInfoList = new ArrayList<>();

    for (int i = 0; i < _columns.length; i++) {
      AggregationInfo aggregationInfo = new AggregationInfo();
      aggregationInfo.setAggregationType(AGGREGATION_FUNCTIONS[i]);

      Map<String, String> params = new HashMap<String, String>();
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
    Map<String, BaseOperator> dataSourceMap = new HashMap<>();
    for (String column : _indexSegment.getColumnNames()) {
      dataSourceMap.put(column, _indexSegment.getDataSource(column));
    }
    int totalRawDocs = _indexSegment.getSegmentMetadata().getTotalRawDocs();
    MatchEntireSegmentOperator matchEntireSegmentOperator = new MatchEntireSegmentOperator(totalRawDocs);
    BReusableFilteredDocIdSetOperator docIdSetOperator =
        new BReusableFilteredDocIdSetOperator(matchEntireSegmentOperator, totalRawDocs, 10000);
    MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);
    TransformExpressionOperator transformOperator =
        new TransformExpressionOperator(projectionOperator, Collections.<TransformExpressionTree>emptyList());
    TransformBlock transformBlock = (TransformBlock) transformOperator.nextBlock();
    int numAggFuncs = _aggregationInfoList.size();
    AggregationFunctionContext[] aggrFuncContextArray = new AggregationFunctionContext[numAggFuncs];
    AggregationFunctionInitializer aggFuncInitializer =
        new AggregationFunctionInitializer(_indexSegment.getSegmentMetadata());
    for (int i = 0; i < numAggFuncs; i++) {
      AggregationInfo aggregationInfo = _aggregationInfoList.get(i);
      aggrFuncContextArray[i] = AggregationFunctionContext.instantiate(aggregationInfo);
      aggrFuncContextArray[i].getAggregationFunction().accept(aggFuncInitializer);
    }
    AggregationExecutor aggregationExecutor = new DefaultAggregationExecutor(aggrFuncContextArray);
    aggregationExecutor.init();
    aggregationExecutor.aggregate(transformBlock);
    aggregationExecutor.finish();

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
   * Clean up the temporary data (segment).
   *
   */
  @AfterSuite
  void tearDown() {
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    if (_indexSegment != null) {
      _indexSegment.destroy();
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
      Map<String, Object> map = new HashMap<String, Object>();

      for (int j = 0; j < _columns.length; j++) {
        String metricName = _columns[j];
        double value = _random.nextDouble() * MAX_VALUE;
        _inputData[j][i] = value;
        map.put(metricName, value);
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
      _docIdSet[i] = i;
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows, schema));
    driver.build();

    _indexSegment = Loaders.IndexSegment.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.heap);
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
    for (int i = 0; i < values.length; i++) {
      sum += values[i];
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
    for (int i = 0; i < values.length; i++) {
      max = Math.max(max, values[i]);
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
    for (int i = 0; i < values.length; i++) {
      min = Math.min(min, values[i]);
    }
    return min;
  }
}
