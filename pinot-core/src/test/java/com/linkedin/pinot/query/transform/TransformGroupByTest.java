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
package com.linkedin.pinot.query.transform;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.IntermediateResultsBlock;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.operator.query.AggregationGroupByOperator;
import com.linkedin.pinot.core.operator.transform.TransformExpressionOperator;
import com.linkedin.pinot.core.operator.transform.function.TimeConversionTransform;
import com.linkedin.pinot.core.operator.transform.function.TransformFunction;
import com.linkedin.pinot.core.operator.transform.function.TransformFunctionFactory;
import com.linkedin.pinot.core.plan.AggregationFunctionInitializer;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.plan.TransformPlanNode;
import com.linkedin.pinot.core.query.aggregation.AggregationFunctionContext;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for transforms on group by columns.
 */
public class TransformGroupByTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(TransformExpressionOperatorTest.class);

  private static final String SEGMENT_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "xformGroupBy";
  private static final String SEGMENT_NAME = "xformGroupBySeg";
  private static final String TABLE_NAME = "xformGroupByTable";

  private static final long RANDOM_SEED = System.nanoTime();
  private static final int NUM_ROWS = DocIdSetPlanNode.MAX_DOC_PER_CALL;
  private static final double EPSILON = 1e-5;
  private static final String DIMENSION_NAME = "dimension";
  private static final String TIME_COLUMN_NAME = "millisSinceEpoch";
  private static final String METRIC_NAME = "metric";
  private static final String[] _dimensionValues = new String[]{"abcd", "ABCD", "bcde", "BCDE", "cdef", "CDEF"};

  private IndexSegment _indexSegment;
  private RecordReader _recordReader;

  @BeforeClass
  public void setup()
      throws Exception {
    TransformFunctionFactory.init(new String[]{ToUpper.class.getName(), TimeConversionTransform.class.getName()});

    Schema schema = buildSchema();
    _recordReader = buildSegment(SEGMENT_DIR_NAME, SEGMENT_NAME, schema);
    _indexSegment = Loaders.IndexSegment.load(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), ReadMode.heap);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(new File(SEGMENT_DIR_NAME));
  }

  /**
   * Test for group-by with transformed string dimension column.
   */
  @Test
  public void testGroupByString()
      throws Exception {
    String query = String.format("select sum(%s) from xformSegTable group by ToUpper(%s)", METRIC_NAME, DIMENSION_NAME);
    AggregationGroupByResult groupByResult = executeGroupByQuery(_indexSegment, query);
    Assert.assertNotNull(groupByResult);

    // Compute the expected answer for the query.
    Map<String, Double> expectedValuesMap = new HashMap<>();
    _recordReader.rewind();
    for (int row = 0; row < NUM_ROWS; row++) {
      GenericRow genericRow = _recordReader.next();
      String key = ((String) genericRow.getValue(DIMENSION_NAME)).toUpperCase();
      Double value = (Double) genericRow.getValue(METRIC_NAME);
      Double prevValue = expectedValuesMap.get(key);

      if (prevValue == null) {
        expectedValuesMap.put(key, value);
      } else {
        expectedValuesMap.put(key, prevValue + value);
      }
    }

    compareGroupByResults(groupByResult, expectedValuesMap);
  }

  /**
   * Test for group-by with transformed time column from millis to days.
   *
   * @throws Exception
   */
  @Test
  public void testTimeRollUp()
      throws Exception {
    String query =
        String.format("select sum(%s) from xformSegTable group by timeConvert(%s, 'MILLISECONDS', 'DAYS')", METRIC_NAME,
            TIME_COLUMN_NAME);

    AggregationGroupByResult groupByResult = executeGroupByQuery(_indexSegment, query);
    Assert.assertNotNull(groupByResult);

    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = groupByResult.getGroupKeyIterator();
    Assert.assertNotNull(groupKeyIterator);

    // Compute the expected answer for the query.
    Map<String, Double> expectedValuesMap = new HashMap<>();
    _recordReader.rewind();
    for (int row = 0; row < NUM_ROWS; row++) {
      GenericRow genericRow = _recordReader.next();
      long daysSinceEpoch =
          TimeUnit.DAYS.convert(((Long) genericRow.getValue(TIME_COLUMN_NAME)), TimeUnit.MILLISECONDS);

      Double value = (Double) genericRow.getValue(METRIC_NAME);
      String key = String.valueOf(daysSinceEpoch);
      Double prevValue = expectedValuesMap.get(key);

      if (prevValue == null) {
        expectedValuesMap.put(key, value);
      } else {
        expectedValuesMap.put(key, prevValue + value);
      }
    }

    compareGroupByResults(groupByResult, expectedValuesMap);
  }

  /**
   * Helper method that executes the group by query on the index and returns the group by result.
   *
   * @param query Query to execute
   * @return Group by result
   */
  private AggregationGroupByResult executeGroupByQuery(IndexSegment indexSegment, String query) {
    Operator filterOperator = new MatchEntireSegmentOperator(indexSegment.getSegmentMetadata().getTotalDocs());
    final BReusableFilteredDocIdSetOperator docIdSetOperator =
        new BReusableFilteredDocIdSetOperator(filterOperator, indexSegment.getSegmentMetadata().getTotalDocs(),
            NUM_ROWS);

    final Map<String, BaseOperator> dataSourceMap = buildDataSourceMap(indexSegment.getSegmentMetadata().getSchema());
    final MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);

    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(query);

    List<AggregationInfo> aggregationsInfo = brokerRequest.getAggregationsInfo();
    int numAggFunctions = aggregationsInfo.size();

    AggregationFunctionContext[] aggrFuncContextArray = new AggregationFunctionContext[numAggFunctions];
    AggregationFunctionInitializer aggFuncInitializer =
        new AggregationFunctionInitializer(indexSegment.getSegmentMetadata());
    for (int i = 0; i < numAggFunctions; i++) {
      AggregationInfo aggregationInfo = aggregationsInfo.get(i);
      aggrFuncContextArray[i] = AggregationFunctionContext.instantiate(aggregationInfo);
      aggrFuncContextArray[i].getAggregationFunction().accept(aggFuncInitializer);
    }

    GroupBy groupBy = brokerRequest.getGroupBy();
    Set<String> expressions = new HashSet<>(groupBy.getExpressions());

    TransformExpressionOperator transformOperator = new TransformExpressionOperator(projectionOperator,
        TransformPlanNode.buildTransformExpressionTrees(expressions));

    AggregationGroupByOperator groupByOperator =
        new AggregationGroupByOperator(aggrFuncContextArray, groupBy, 10_000, Integer.MAX_VALUE, transformOperator,
            NUM_ROWS);

    IntermediateResultsBlock block = (IntermediateResultsBlock) groupByOperator.nextBlock();
    return block.getAggregationGroupByResult();
  }

  /**
   * Helper method to build a segment with one dimension column containing values
   * from {@link #_dimensionValues}, and one metric column.
   *
   * Also builds the expected group by result as it builds the segments.
   *
   * @param segmentDirName Name of segment directory
   * @param segmentName Name of segment
   * @param schema Schema for segment
   * @return Schema built for the segment
   * @throws Exception
   */
  private RecordReader buildSegment(String segmentDirName, String segmentName, Schema schema)
      throws Exception {

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setOutDir(segmentDirName);
    config.setFormat(FileFormat.AVRO);
    config.setTableName(TABLE_NAME);
    config.setSegmentName(segmentName);

    Random random = new Random(RANDOM_SEED);
    long currentTimeMillis = System.currentTimeMillis();

    // Divide the day into fixed parts, and decrement time column value by this delta, so as to get
    // continuous days in the input. This gives about 10 days per 10k rows.
    long timeDelta = TimeUnit.MILLISECONDS.convert(1, TimeUnit.DAYS) / 1000;

    int numDimValues = _dimensionValues.length;

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      map.put(DIMENSION_NAME, _dimensionValues[random.nextInt(numDimValues)]);
      map.put(METRIC_NAME, random.nextDouble());

      map.put(TIME_COLUMN_NAME, currentTimeMillis);
      currentTimeMillis -= timeDelta;

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    RecordReader recordReader = new GenericRowRecordReader(rows, schema);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();

    LOGGER.info("Built segment {} at {}", segmentName, segmentDirName);
    return recordReader;
  }

  /**
   * Helper method to build a schema with one string dimension, and one double metric columns.
   */
  private static Schema buildSchema() {
    Schema schema = new Schema();
    DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(DIMENSION_NAME, FieldSpec.DataType.STRING, true);
    schema.addField(dimensionFieldSpec);

    MetricFieldSpec metricFieldSpec = new MetricFieldSpec(METRIC_NAME, FieldSpec.DataType.DOUBLE);
    schema.addField(metricFieldSpec);

    TimeFieldSpec timeFieldSpec = new TimeFieldSpec(TIME_COLUMN_NAME, FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS);
    schema.addField(timeFieldSpec);
    return schema;
  }

  /**
   * Helper method to build data source map for all the metric columns.
   *
   * @param schema Schema for the index segment
   * @return Map of metric name to its data source.
   */
  private Map<String, BaseOperator> buildDataSourceMap(Schema schema) {
    final Map<String, BaseOperator> dataSourceMap = new HashMap<>();
    for (String metricName : schema.getColumnNames()) {
      dataSourceMap.put(metricName, _indexSegment.getDataSource(metricName));
    }
    return dataSourceMap;
  }

  /**
   * Helper method to compare group by result from query execution against a map of group keys and values.
   *
   * @param groupByResult Group by result from query
   * @param expectedValuesMap Map of expected keys and values
   */
  private void compareGroupByResults(AggregationGroupByResult groupByResult, Map<String, Double> expectedValuesMap) {
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = groupByResult.getGroupKeyIterator();
    Assert.assertNotNull(groupKeyIterator);

    int numGroupKeys = 0;
    while (groupKeyIterator.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Double actual = (Double) groupByResult.getResultForKey(groupKey, 0 /* aggregation function index */);

      String stringKey = groupKey._stringKey;
      Double expected = expectedValuesMap.get(stringKey);
      Assert.assertNotNull(expected, "Unexpected key in actual result: " + stringKey);
      Assert.assertEquals(actual, expected, EPSILON);
      numGroupKeys++;
    }

    Assert.assertEquals(numGroupKeys, expectedValuesMap.size(), "Mis-match in number of group keys");
  }

  /**
   * Implementation of TransformFunction that converts strings to upper case.
   */
  public static class ToUpper implements TransformFunction {
    @Override
    public String[] transform(int length, BlockValSet... input) {
      String[] inputStrings = input[0].getStringValuesSV();
      String[] outputStrings = new String[length];

      for (int i = 0; i < length; i++) {
        outputStrings[i] = inputStrings[i].toUpperCase();
      }
      return outputStrings;
    }

    @Override
    public FieldSpec.DataType getOutputType() {
      return FieldSpec.DataType.STRING;
    }

    @Override
    public String getName() {
      return "ToUpper";
    }
  }
}
