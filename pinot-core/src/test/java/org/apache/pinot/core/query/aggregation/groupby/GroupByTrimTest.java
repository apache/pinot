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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.combine.GroupByOrderByCombineOperator;
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
import org.apache.pinot.spi.data.DataTable;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
public class GroupByTrimTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "GroupByTrimTest");
  private static final String SEGMENT_NAME = "testSegment";
  private static final String METRIC_PREFIX = "metric_";
  private static final int NUM_COLUMNS = 2;
  private static final int NUM_ROWS = 10000;

  private final ExecutorService _executorService = Executors.newCachedThreadPool();
  private IndexSegment _indexSegment;
  private String[] _columns;
  private double[][] _inputData;
  private Map<Double, Double> _resultMap;

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
    FileUtils.deleteQuietly(INDEX_DIR);
    _resultMap = new HashMap<>();
    // Current Schema: Columns: metrics_0(double), metrics_1(double)
    _inputData = new double[NUM_COLUMNS][NUM_ROWS];
    _columns = new String[NUM_COLUMNS];
    setupSegment();
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    _executorService.shutdown();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  /**
   * Test the GroupBy OrderBy query and compute the expected results to match
   */
  @Test(dataProvider = "groupByTrimTestDataProvider")
  void testGroupByTrim(QueryContext queryContext, int minSegmentGroupTrimSize, int minServerGroupTrimSize,
      List<Pair<Double, Double>> expectedResult)
      throws Exception {
    queryContext.setEndTimeMs(System.currentTimeMillis() + CommonConstants.Server.DEFAULT_QUERY_EXECUTOR_TIMEOUT_MS);
    queryContext.setMinSegmentGroupTrimSize(minSegmentGroupTrimSize);
    queryContext.setMinServerGroupTrimSize(minServerGroupTrimSize);

    // Create a query operator
    AggregationGroupByOrderByOperator groupByOperator =
        new AggregationGroupByOrderByPlanNode(_indexSegment, queryContext).run();
    GroupByOrderByCombineOperator combineOperator =
        new GroupByOrderByCombineOperator(Collections.singletonList(groupByOperator), queryContext, _executorService);

    // Execute the query
    IntermediateResultsBlock resultsBlock = combineOperator.nextBlock();

    // Extract the execution result
    List<Pair<Double, Double>> extractedResult = extractTestResult(resultsBlock);

    assertEquals(extractedResult, expectedResult);
  }

  /**
   * Helper method to setup the index segment on which to perform aggregation tests.
   * - Generates a segment with {@link #NUM_COLUMNS} and {@link #NUM_ROWS}
   * - Random 'double' data filled in the metric columns. The data is also populated
   *   into the _inputData[], so it can be used to test the results.
   *
   * @throws Exception
   */
  private void setupSegment()
      throws Exception {
    // Segment Config
    SegmentGeneratorConfig config =
        new SegmentGeneratorConfig(new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build(),
            buildSchema());
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(INDEX_DIR.getAbsolutePath());

    // Fill the data table
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    int baseValue = 10;
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow genericRow = new GenericRow();

      for (int j = 0; j < NUM_COLUMNS; j++) {
        double value = baseValue + i + j;
        _inputData[j][i] = value;
        genericRow.putValue(_columns[j], value);
      }
      // Compute the max result and insert into a grouped map
      computeMaxResult(_inputData[0][i], _inputData[1][i]);
      rows.add(genericRow);
      baseValue += 10;
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

    for (int i = 0; i < NUM_COLUMNS; i++) {
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
  private void computeMaxResult(double key, double value) {
    Double currentValue = _resultMap.get(key);
    if (currentValue == null || currentValue < value) {
      _resultMap.put(key, value);
    }
  }

  /**
   * Helper method to extract the result from IntermediateResultsBlock
   *
   * @return A list of expected results
   */
  private List<Pair<Double, Double>> extractTestResult(IntermediateResultsBlock resultsBlock)
      throws Exception {
    DataTable dataTable = resultsBlock.getDataTable();
    int numRows = dataTable.getNumberOfRows();
    List<Pair<Double, Double>> result = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      result.add(Pair.of(dataTable.getDouble(i, 0), dataTable.getDouble(i, 1)));
    }
    result.sort((o1, o2) -> Double.compare(o2.getRight(), o1.getRight()));
    return result;
  }

  @DataProvider
  public Object[][] groupByTrimTestDataProvider() {
    List<Object[]> data = new ArrayList<>();
    List<Pair<Double, Double>> expectedResult = computeExpectedResult();

    // Testcase1: low limit + high min trim size
    QueryContext queryContext = QueryContextConverterUtils.getQueryContextFromSQL(
        "SELECT metric_0, max(metric_1) FROM testTable GROUP BY metric_0 ORDER BY max(metric_1) DESC LIMIT 1");
    List<Pair<Double, Double>> top100 = expectedResult.subList(0, 100);
    data.add(new Object[]{queryContext, 100, 5000, top100});
    data.add(new Object[]{queryContext, 100, -1, top100});
    data.add(new Object[]{queryContext, -1, 100, top100});
    data.add(new Object[]{queryContext, 5000, 100, top100});

    // Testcase2: high limit + low min trim size
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(
        "SELECT metric_0, max(metric_1) FROM testTable GROUP BY metric_0 ORDER BY max(metric_1) DESC LIMIT 50");
    List<Pair<Double, Double>> top250 = expectedResult.subList(0, 250);
    data.add(new Object[]{queryContext, 50, 5000, top250});
    data.add(new Object[]{queryContext, 200, -1, top250});
    data.add(new Object[]{queryContext, -1, 150, top250});
    data.add(new Object[]{queryContext, 5000, 10, top250});
    data.add(new Object[]{queryContext, 20, 30, top250});

    // Testcase3: disable trim
    queryContext = QueryContextConverterUtils.getQueryContextFromSQL(
        "SELECT metric_0, max(metric_1) FROM testTable GROUP BY metric_0 ORDER BY max(metric_1) DESC LIMIT 10");
    data.add(new Object[]{queryContext, -1, -1, expectedResult});

    return data.toArray(new Object[data.size()][]);
  }

  /**
   * Helper method to compute the expected result
   *
   * @return A list of expected results
   */
  private List<Pair<Double, Double>> computeExpectedResult() {
    List<Pair<Double, Double>> result = new ArrayList<>(_resultMap.size());
    for (Map.Entry<Double, Double> entry : _resultMap.entrySet()) {
      result.add(Pair.of(entry.getKey(), entry.getValue()));
    }
    result.sort((o1, o2) -> Double.compare(o2.getRight(), o1.getRight()));
    return result;
  }
}
