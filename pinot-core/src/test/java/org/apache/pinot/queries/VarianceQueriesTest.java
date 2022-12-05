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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.commons.math3.util.Precision;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.segment.local.customobject.VarianceTuple;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class VarianceQueriesTest extends BaseQueriesTest {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "VarianceQueriesTest");

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 2000;
  private static final int NUM_GROUPS = 10;
  private static final int MAX_VALUE = 500;
  private static final double RELATIVE_EPSILON = 0.0001;
  private static final double DELTA = 0.0001;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String GROUP_BY_COLUMN = "groupByColumn";

  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
          .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
          .addSingleValueDimension(FLOAT_COLUMN, FieldSpec.DataType.FLOAT)
          .addSingleValueDimension(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE)
          .addSingleValueDimension(GROUP_BY_COLUMN, FieldSpec.DataType.DOUBLE).build();

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  int[] _intValues = new int[NUM_RECORDS];
  long[] _longValues = new long[NUM_RECORDS];
  float[] _floatValues = new float[NUM_RECORDS];
  double[] _doubleValues = new double[NUM_RECORDS];

  @Override
  protected String getFilter() {
    // filter out half of the rows based on group id
    return " WHERE groupByColumn < " + (NUM_GROUPS / 2);
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    Random random = new Random();
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);

    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      int intValue = -MAX_VALUE + random.nextInt() * 2 * MAX_VALUE;
      long longValue = -MAX_VALUE + random.nextLong() * 2 * MAX_VALUE;
      float floatValue = -MAX_VALUE + random.nextFloat() * 2 * MAX_VALUE;
      double doubleValue = -MAX_VALUE + random.nextDouble() * 2 * MAX_VALUE;

      _intValues[i] = intValue;
      _longValues[i] = longValue;
      _floatValues[i] = floatValue;
      _doubleValues[i] = doubleValue;

      record.putValue(INT_COLUMN, _intValues[i]);
      record.putValue(LONG_COLUMN, _longValues[i]);
      record.putValue(FLOAT_COLUMN, _floatValues[i]);
      record.putValue(DOUBLE_COLUMN, _doubleValues[i]);
      record.putValue(GROUP_BY_COLUMN, Math.floor(i / (NUM_RECORDS / NUM_GROUPS)));
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testVarianceAggregationOnly() {
    // Compute the expected values
    Variance[] expectedVariances = new Variance[8];
    for (int i = 0; i < 8; i++) {
      if (i < 4) {
        expectedVariances[i] = new Variance(false);
      } else {
        expectedVariances[i] = new Variance(true);
      }
    }
    for (int i = 0; i < NUM_RECORDS; i++) {
      expectedVariances[0].increment(_intValues[i]);
      expectedVariances[1].increment(_longValues[i]);
      expectedVariances[2].increment(_floatValues[i]);
      expectedVariances[3].increment(_doubleValues[i]);
      expectedVariances[4].increment(_intValues[i]);
      expectedVariances[5].increment(_longValues[i]);
      expectedVariances[6].increment(_floatValues[i]);
      expectedVariances[7].increment(_doubleValues[i]);
    }
    double expectedIntSum = Arrays.stream(_intValues).asDoubleStream().sum();
    double expectedLongSum = Arrays.stream(_longValues).asDoubleStream().sum();
    double expectedFloatSum = 0.0;
    for (int i = 0; i < _floatValues.length; i++) {
      expectedFloatSum += _floatValues[i];
    }
    double expectedDoubleSum = Arrays.stream(_doubleValues).sum();

    // Compute the query
    String query = "SELECT VAR_POP(intColumn), VAR_POP(longColumn), VAR_POP(floatColumn), VAR_POP(doubleColumn),"
        + "VAR_SAMP(intColumn), VAR_SAMP(longColumn), VAR_SAMP(floatColumn), VAR_SAMP(doubleColumn) FROM testTable";
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 4, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();

    // Validate the aggregation results
    checkWithPrecisionForVariance((VarianceTuple) aggregationResult.get(0), NUM_RECORDS, expectedIntSum,
        expectedVariances[0].getResult(), false);
    checkWithPrecisionForVariance((VarianceTuple) aggregationResult.get(1), NUM_RECORDS, expectedLongSum,
        expectedVariances[1].getResult(), false);
    checkWithPrecisionForVariance((VarianceTuple) aggregationResult.get(2), NUM_RECORDS, expectedFloatSum,
        expectedVariances[2].getResult(), false);
    checkWithPrecisionForVariance((VarianceTuple) aggregationResult.get(3), NUM_RECORDS, expectedDoubleSum,
        expectedVariances[3].getResult(), false);
    checkWithPrecisionForVariance((VarianceTuple) aggregationResult.get(4), NUM_RECORDS, expectedIntSum,
        expectedVariances[4].getResult(), true);
    checkWithPrecisionForVariance((VarianceTuple) aggregationResult.get(5), NUM_RECORDS, expectedLongSum,
        expectedVariances[5].getResult(), true);
    checkWithPrecisionForVariance((VarianceTuple) aggregationResult.get(6), NUM_RECORDS, expectedFloatSum,
        expectedVariances[6].getResult(), true);
    checkWithPrecisionForVariance((VarianceTuple) aggregationResult.get(7), NUM_RECORDS, expectedDoubleSum,
        expectedVariances[7].getResult(), true);

    // Update the expected result by 3 more times (broker query will compute 4 identical segments)
    for (int i = 0; i < NUM_RECORDS * 3; i++) {
      int pos = i % NUM_RECORDS;
      expectedVariances[0].increment(_intValues[pos]);
      expectedVariances[1].increment(_longValues[pos]);
      expectedVariances[2].increment(_floatValues[pos]);
      expectedVariances[3].increment(_doubleValues[pos]);
      expectedVariances[4].increment(_intValues[pos]);
      expectedVariances[5].increment(_longValues[pos]);
      expectedVariances[6].increment(_floatValues[pos]);
      expectedVariances[7].increment(_doubleValues[pos]);
    }

    // Validate the response
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    brokerResponse.getResultTable();
    Object[] results = brokerResponse.getResultTable().getRows().get(0);
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[0], expectedVariances[0].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[1], expectedVariances[1].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[2], expectedVariances[2].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[3], expectedVariances[3].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[4], expectedVariances[4].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[5], expectedVariances[5].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[6], expectedVariances[6].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[7], expectedVariances[7].getResult(), RELATIVE_EPSILON));

    // Validate the response for a query with a filter
    query = "SELECT VAR_POP(intColumn) from testTable" + getFilter();
    brokerResponse = getBrokerResponse(query);
    brokerResponse.getResultTable();
    results = brokerResponse.getResultTable().getRows().get(0);
    Variance filterExpectedVariance = new Variance(false);
    for (int i = 0; i < NUM_RECORDS / 2; i++) {
      filterExpectedVariance.increment(_intValues[i]);
    }
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[0], filterExpectedVariance.getResult(),
        RELATIVE_EPSILON));
  }

  @Test
  public void testVarianceAggregationGroupBy() {
    // Compute expected group results
    Variance[] expectedGroupByResult = new Variance[NUM_GROUPS];
    double[] expectedSum = new double[NUM_GROUPS];

    for (int i = 0; i < NUM_GROUPS; i++) {
      expectedGroupByResult[i] = new Variance(false);
    }
    for (int j = 0; j < NUM_RECORDS; j++) {
      int pos = j / (NUM_RECORDS / NUM_GROUPS);
      expectedGroupByResult[pos].increment(_intValues[j]);
      expectedSum[pos] += _intValues[j];
    }

    String query = "SELECT VAR_POP(intColumn) FROM testTable GROUP BY groupByColumn ORDER BY groupByColumn";
    GroupByOperator groupByOperator = getOperator(query);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 2, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    for (int i = 0; i < NUM_GROUPS; i++) {

      VarianceTuple actualVarianceTuple = (VarianceTuple) aggregationGroupByResult.getResultForGroupId(0, i);
      checkWithPrecisionForVariance(actualVarianceTuple, NUM_RECORDS / NUM_GROUPS, expectedSum[i],
          expectedGroupByResult[i].getResult(), false);
    }
  }

  @Test
  public void testStandardDeviationAggregationOnly() {
    // Compute the expected values
    StandardDeviation[] expectedStdDevs = new StandardDeviation[8];
    for (int i = 0; i < 8; i++) {
      if (i < 4) {
        expectedStdDevs[i] = new StandardDeviation(false);
      } else {
        expectedStdDevs[i] = new StandardDeviation(true);
      }
    }
    for (int i = 0; i < NUM_RECORDS; i++) {
      expectedStdDevs[0].increment(_intValues[i]);
      expectedStdDevs[1].increment(_longValues[i]);
      expectedStdDevs[2].increment(_floatValues[i]);
      expectedStdDevs[3].increment(_doubleValues[i]);
      expectedStdDevs[4].increment(_intValues[i]);
      expectedStdDevs[5].increment(_longValues[i]);
      expectedStdDevs[6].increment(_floatValues[i]);
      expectedStdDevs[7].increment(_doubleValues[i]);
    }

    double expectedIntSum = Arrays.stream(_intValues).asDoubleStream().sum();
    double expectedLongSum = Arrays.stream(_longValues).asDoubleStream().sum();
    double expectedFloatSum = 0.0;
    for (int i = 0; i < _floatValues.length; i++) {
      expectedFloatSum += _floatValues[i];
    }
    double expectedDoubleSum = Arrays.stream(_doubleValues).sum();

    // Compute the query
    String query =
        "SELECT STDDEV_POP(intColumn), STDDEV_POP(longColumn), STDDEV_POP(floatColumn), STDDEV_POP(doubleColumn),"
            + "STDDEV_SAMP(intColumn), STDDEV_SAMP(longColumn), STDDEV_SAMP(floatColumn), STDDEV_SAMP(doubleColumn) "
            + "FROM testTable";
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 4, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();

    // Validate the aggregation results
    checkWithPrecisionForStandardDeviation((VarianceTuple) aggregationResult.get(0), NUM_RECORDS, expectedIntSum,
        expectedStdDevs[0].getResult(), false);
    checkWithPrecisionForStandardDeviation((VarianceTuple) aggregationResult.get(1), NUM_RECORDS, expectedLongSum,
        expectedStdDevs[1].getResult(), false);
    checkWithPrecisionForStandardDeviation((VarianceTuple) aggregationResult.get(2), NUM_RECORDS, expectedFloatSum,
        expectedStdDevs[2].getResult(), false);
    checkWithPrecisionForStandardDeviation((VarianceTuple) aggregationResult.get(3), NUM_RECORDS, expectedDoubleSum,
        expectedStdDevs[3].getResult(), false);
    checkWithPrecisionForStandardDeviation((VarianceTuple) aggregationResult.get(4), NUM_RECORDS, expectedIntSum,
        expectedStdDevs[4].getResult(), true);
    checkWithPrecisionForStandardDeviation((VarianceTuple) aggregationResult.get(5), NUM_RECORDS, expectedLongSum,
        expectedStdDevs[5].getResult(), true);
    checkWithPrecisionForStandardDeviation((VarianceTuple) aggregationResult.get(6), NUM_RECORDS, expectedFloatSum,
        expectedStdDevs[6].getResult(), true);
    checkWithPrecisionForStandardDeviation((VarianceTuple) aggregationResult.get(7), NUM_RECORDS, expectedDoubleSum,
        expectedStdDevs[7].getResult(), true);

    // Update the expected result by 3 more times (broker query will compute 4 identical segments)
    for (int i = 0; i < NUM_RECORDS * 3; i++) {
      int pos = i % NUM_RECORDS;
      expectedStdDevs[0].increment(_intValues[pos]);
      expectedStdDevs[1].increment(_longValues[pos]);
      expectedStdDevs[2].increment(_floatValues[pos]);
      expectedStdDevs[3].increment(_doubleValues[pos]);
      expectedStdDevs[4].increment(_intValues[pos]);
      expectedStdDevs[5].increment(_longValues[pos]);
      expectedStdDevs[6].increment(_floatValues[pos]);
      expectedStdDevs[7].increment(_doubleValues[pos]);
    }

    // Validate the response
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    brokerResponse.getResultTable();
    Object[] results = brokerResponse.getResultTable().getRows().get(0);
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[0], expectedStdDevs[0].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[1], expectedStdDevs[1].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[2], expectedStdDevs[2].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[3], expectedStdDevs[3].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[4], expectedStdDevs[4].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[5], expectedStdDevs[5].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[6], expectedStdDevs[6].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[7], expectedStdDevs[7].getResult(), RELATIVE_EPSILON));

    // Validate the response for a query with a filter
    query = "SELECT STDDEV_POP(intColumn) from testTable" + getFilter();
    brokerResponse = getBrokerResponse(query);
    brokerResponse.getResultTable();
    results = brokerResponse.getResultTable().getRows().get(0);
    StandardDeviation filterExpectedStdDev = new StandardDeviation(false);
    for (int i = 0; i < NUM_RECORDS / 2; i++) {
      filterExpectedStdDev.increment(_intValues[i]);
    }
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[0], filterExpectedStdDev.getResult(), RELATIVE_EPSILON));
  }

  @Test
  public void testStandardDeviationAggreagtionGroupBy() {
    // Compute expected group results
    StandardDeviation[] expectedGroupByResult = new StandardDeviation[NUM_GROUPS];
    double[] expectedSum = new double[NUM_GROUPS];

    for (int i = 0; i < NUM_GROUPS; i++) {
      expectedGroupByResult[i] = new StandardDeviation(false);
    }
    for (int j = 0; j < NUM_RECORDS; j++) {
      int pos = j / (NUM_RECORDS / NUM_GROUPS);
      expectedGroupByResult[pos].increment(_intValues[j]);
      expectedSum[pos] += _intValues[j];
    }

    String query = "SELECT STDDEV_POP(intColumn) FROM testTable GROUP BY groupByColumn ORDER BY groupByColumn";
    GroupByOperator groupByOperator = getOperator(query);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 2, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    for (int i = 0; i < NUM_GROUPS; i++) {
      VarianceTuple actualVarianceTuple = (VarianceTuple) aggregationGroupByResult.getResultForGroupId(0, i);
      checkWithPrecisionForStandardDeviation(actualVarianceTuple, NUM_RECORDS / NUM_GROUPS, expectedSum[i],
          expectedGroupByResult[i].getResult(), false);
    }
  }

  private void checkWithPrecisionForVariance(VarianceTuple tuple, int expectedCount, double expectedSum,
      double expectedVariance, boolean isBiasCorrected) {
    assertEquals(tuple.getCount(), expectedCount);
    assertTrue(Precision.equalsWithRelativeTolerance(tuple.getSum(), expectedSum, RELATIVE_EPSILON));
    if (!isBiasCorrected) {
      assertTrue(
          Precision.equalsWithRelativeTolerance(tuple.getM2(), expectedVariance * expectedCount, RELATIVE_EPSILON));
    } else {
      assertTrue(Precision.equalsWithRelativeTolerance(tuple.getM2(), expectedVariance * (expectedCount - 1),
          RELATIVE_EPSILON));
    }
  }

  private void checkWithPrecisionForStandardDeviation(VarianceTuple tuple, int expectedCount, double expectedSum,
      double expectedStdDev, boolean isBiasCorrected) {
    assertEquals(tuple.getCount(), expectedCount);
    assertTrue(Precision.equalsWithRelativeTolerance(tuple.getSum(), expectedSum, RELATIVE_EPSILON));
    if (!isBiasCorrected) {
      assertTrue(Precision.equalsWithRelativeTolerance(tuple.getM2(), expectedStdDev * expectedStdDev * expectedCount,
          RELATIVE_EPSILON));
    } else {
      assertTrue(
          Precision.equalsWithRelativeTolerance(tuple.getM2(), expectedStdDev * expectedStdDev * (expectedCount - 1),
              RELATIVE_EPSILON));
    }
  }
}
