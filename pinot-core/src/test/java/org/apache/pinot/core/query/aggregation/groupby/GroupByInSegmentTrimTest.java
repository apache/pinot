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
package org.apache.pinot.core.query.aggregation.groupby;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOrderByOperator;
import org.apache.pinot.core.plan.AggregationGroupByOrderByPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.Pair;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static java.lang.Math.max;
import static org.testng.Assert.assertEquals;


/**
 * Unit test for GroupBy Trim functionalities.
 * - Builds a segment with random data.
 * - Uses AggregationGroupByOrderByPlanNode class to construct an AggregationGroupByOrderByOperator
 * - Perform aggregationGroupBy and OrderBy on the data
 * - Also computes those results itself.
 * - Asserts that the aggregation results returned by the class are the same as
 *   returned by the local computations.
 *
 * Currently tests 'max' functions, and can be easily extended to
 * test other conditions such as GroupBy without OrderBy
 */
public class GroupByInSegmentTrimTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "GroupByInSegmentTrimTest");
  private static final String SEGMENT_NAME = "TestGroupByInSegment";

  private static final String METRIC_PREFIX = "metric_";
  private static final int NUM_ROWS = 1000;
  private static final int NUM_COLUMN = 2;
  private static final int MAX_INITIAL_RESULT_HOLDER_CAPACITY = 10_000;
  private static final int NUM_GROUPS_LIMIT = 100_000;
  private static IndexSegment _indexSegment;
  private static String[] _columns;
  private static double[][] _inputData;
  private static Map<Double, Double> _resultMap;

  /**
   * Initializations prior to the test:
   * - Build a segment with metric columns (that will be aggregated and grouped) containing
   *  randomly generated data.
   *
   * @throws Exception
   */
  @BeforeClass
  public void setUp()
      throws Exception {
    _resultMap = new HashMap<>();
    // Current Schema: Columns: metrics_0(double), metrics_1(double)
    _inputData = new double[NUM_COLUMN][NUM_ROWS];
    _columns = new String[NUM_COLUMN];
    setupSegment();
  }

  /**
   * Test the GroupBy OrderBy query and compute the expected results to match
   */
  @Test(dataProvider = "QueryDataProvider")
  void TestGroupByOrderByOperator(int trimSize, List<Pair<Double, Double>> expectedResult, QueryContext queryContext) {
    // Create a query plan
    AggregationGroupByOrderByPlanNode aggregationGroupByOrderByPlanNode =
        new AggregationGroupByOrderByPlanNode(_indexSegment, queryContext, MAX_INITIAL_RESULT_HOLDER_CAPACITY,
            NUM_GROUPS_LIMIT, trimSize);

    // Get the query executor
    AggregationGroupByOrderByOperator aggregationGroupByOrderByOperator = aggregationGroupByOrderByPlanNode.run();

    // Extract the execution result
    IntermediateResultsBlock resultsBlock = aggregationGroupByOrderByOperator.nextBlock();
    ArrayList<Pair<Double, Double>> extractedResult = extractTestResult(resultsBlock);

    assertEquals(extractedResult, expectedResult);
  }

  /**
   * Helper method to setup the index segment on which to perform aggregation tests.
   * - Generates a segment with {@link #NUM_COLUMN} and {@link #NUM_ROWS}
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

    // Segment Config
    SegmentGeneratorConfig config =
        new SegmentGeneratorConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build(),
            buildSchema());
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getAbsolutePath());

    // Fill the data table
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    int step = 10;
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow genericRow = new GenericRow();

      for (int j = 0; j < _columns.length; j++) {
        String metricName = _columns[j];
        double value = step + i + j;
        _inputData[j][i] = value;
        genericRow.putValue(metricName, value);
      }
      // Compute the max result and insert into a grouped map
      computeMaxResult(_inputData[0][i], _inputData[1][i]);
      rows.add(genericRow);
      step += 1;
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    _indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, driver.getSegmentName()), ReadMode.heap);
  }

  /**
   * Helper method to build schema for the segment on which aggregation tests will be run.
   *
   * @return table schema
   */
  private Schema buildSchema() {
    Schema schema = new Schema();

    for (int i = 0; i < NUM_COLUMN; i++) {
      String metricName = METRIC_PREFIX + i;
      MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, FieldSpec.DataType.DOUBLE);
      schema.addField(metricFieldSpec);
      _columns[i] = metricName;
    }
    return schema;
  }

  /**
   * Helper method to compute the aggregation result grouped by the key
   *
   */
  private void computeMaxResult(Double key, Double result) {
    if (_resultMap.get(key) == null || _resultMap.get(key) < result) {
      _resultMap.put(key, result);
    }
  }

  /**
   * Helper method to extract the result from IntermediateResultsBlock
   *
   * @return A list of expected results
   */
  private ArrayList<Pair<Double, Double>> extractTestResult(IntermediateResultsBlock resultsBlock) {
    AggregationGroupByResult result = resultsBlock.getAggregationGroupByResult();
    if (result != null) {
      // No trim
      return extractAggregationResult(result);
    } else {
      // In case of trim
      return extractIntermediateResult(resultsBlock.getIntermediateRecords());
    }
  }

  /**
   * Helper method to extract the result from AggregationGroupByResult
   *
   * @return A list of expected results
   */
  private ArrayList<Pair<Double, Double>> extractAggregationResult(AggregationGroupByResult aggregationGroupByResult) {
    ArrayList<Pair<Double, Double>> result = new ArrayList<>();
    Iterator<GroupKeyGenerator.GroupKey> iterator = aggregationGroupByResult.getGroupKeyIterator();
    int i = 0;
    while (iterator.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = iterator.next();
      Double key = (Double) groupKey._keys[0];
      Double value = (Double) aggregationGroupByResult.getResultForGroupId(i, groupKey._groupId);
      result.add(new Pair<>(key, value));
    }
    result.sort((o1, o2) -> (int) (o2.getSecond() - o1.getSecond()));
    return result;
  }

  /**
   * Helper method to extract the result from Collection<IntermediateRecord>
   *
   * @return A list of expected results
   */
  private ArrayList<Pair<Double, Double>> extractIntermediateResult(Collection<IntermediateRecord> intermediateRecord) {
    ArrayList<Pair<Double, Double>> result = new ArrayList<>();
    PriorityQueue<IntermediateRecord> resultPQ = new PriorityQueue<>(intermediateRecord);
    while (!resultPQ.isEmpty()) {
      IntermediateRecord head = resultPQ.poll();
      result.add(new Pair<>((Double) head._record.getValues()[0], (Double) head._record.getValues()[1]));
    }
    Collections.reverse(result);
    return result;
  }

  @DataProvider
  public static Object[][] QueryDataProvider() {
    List<Object[]> data = new ArrayList<>();
    ArrayList<Pair<Double, Double>> expectedResult = computeExpectedResult();
    // Testcase1: low limit + high trim size
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(
        "SELECT metric_0, max(metric_1) FROM testTable GROUP BY metric_0 ORDER BY max(metric_1) DESC LIMIT 1");
    int trimSize = 100;
    int expectedSize = max(trimSize, 5 * queryContext.getLimit());
    data.add(new Object[]{trimSize, expectedResult.subList(0, expectedSize), queryContext});
    // Testcase2: high limit + low trim size
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(
        "SELECT metric_0, max(metric_1) FROM testTable GROUP BY metric_0 ORDER BY max(metric_1) DESC LIMIT 50");
    trimSize = 10;
    expectedSize = max(trimSize, 5 * queryContext.getLimit());
    data.add(new Object[]{trimSize, expectedResult.subList(0, expectedSize), queryContext});
    // Testcase3: high limit + high trim size (No trim)
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(
        "SELECT metric_0, max(metric_1) FROM testTable GROUP BY metric_0 ORDER BY max(metric_1) DESC LIMIT 500");
    trimSize = 1000;
    expectedSize = 1000;
    data.add(new Object[]{trimSize, expectedResult.subList(0, expectedSize), queryContext});

    return data.toArray(new Object[data.size()][]);
  }

  /**
   * Helper method to compute the expected result
   *
   * @return A list of expected results
   */
  private static ArrayList<Pair<Double, Double>> computeExpectedResult() {
    ArrayList<Pair<Double, Double>> result = new ArrayList<>();
    for (Map.Entry<Double, Double> entry : _resultMap.entrySet()) {
      result.add(new Pair<>(entry.getKey(), entry.getValue()));
    }
    result.sort((o1, o2) -> (int) (o2.getSecond() - o1.getSecond()));
    return result;
  }
}
