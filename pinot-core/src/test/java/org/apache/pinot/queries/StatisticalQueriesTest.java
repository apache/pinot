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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.commons.math3.stat.descriptive.moment.Kurtosis;
import org.apache.commons.math3.stat.descriptive.moment.Skewness;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.moment.Variance;
import org.apache.commons.math3.util.Precision;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTableRows;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.segment.local.customobject.CovarianceTuple;
import org.apache.pinot.segment.local.customobject.PinotFourthMoment;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for statistical queries (i.e Variance, Covariance, Standard Deviation etc)
 */
public class StatisticalQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "CovarianceQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  // test segments 1-4 evenly divide testSegment into 4 distinct segments
  private static final String SEGMENT_NAME_1 = "testSegment1";
  private static final String SEGMENT_NAME_2 = "testSegment2";
  private static final String SEGMENT_NAME_3 = "testSegment3";
  private static final String SEGMENT_NAME_4 = "testSegment4";

  private static final int NUM_RECORDS = 2000;
  private static final int NUM_GROUPS = 10;
  private static final int MAX_VALUE = 500;
  private static final double RELATIVE_EPSILON = 0.0001;
  private static final double DELTA = 0.0001;

  private static final String INT_COLUMN_X = "intColumnX";
  private static final String INT_COLUMN_Y = "intColumnY";
  private static final String DOUBLE_COLUMN_X = "doubleColumnX";
  private static final String DOUBLE_COLUMN_Y = "doubleColumnY";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String GROUP_BY_COLUMN = "groupByColumn";

  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN_X, FieldSpec.DataType.INT)
          .addSingleValueDimension(INT_COLUMN_Y, FieldSpec.DataType.INT)
          .addSingleValueDimension(DOUBLE_COLUMN_X, FieldSpec.DataType.DOUBLE)
          .addSingleValueDimension(DOUBLE_COLUMN_Y, FieldSpec.DataType.DOUBLE)
          .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
          .addSingleValueDimension(FLOAT_COLUMN, FieldSpec.DataType.FLOAT)
          .addSingleValueDimension(GROUP_BY_COLUMN, FieldSpec.DataType.DOUBLE).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private List<List<IndexSegment>> _distinctInstances;
  private int _sumIntX = 0;
  private int _sumIntY = 0;
  private int _sumIntXY = 0;

  private double _sumDoubleX = 0;
  private double _sumDoubleY = 0;
  private double _sumDoubleXY = 0;

  private long _sumLong = 0L;
  private double _sumFloat = 0;

  private double _sumIntDouble = 0;
  private long _sumIntLong = 0L;
  private double _sumIntFloat = 0;
  private double _sumDoubleLong = 0;
  private double _sumDoubleFloat = 0;
  private double _sumLongFloat = 0;

  private double _expectedCovIntXY;
  private double _expectedCovDoubleXY;
  private double _expectedCovIntDouble;
  private double _expectedCovIntLong;
  private double _expectedCovIntFloat;
  private double _expectedCovDoubleLong;
  private double _expectedCovDoubleFloat;
  private double _expectedCovLongFloat;

  private double _expectedCovWithFilter;

  private final CovarianceTuple[] _expectedGroupByResultVer1 = new CovarianceTuple[NUM_GROUPS];
  private final CovarianceTuple[] _expectedGroupByResultVer2 = new CovarianceTuple[NUM_GROUPS];
  private final double[] _expectedFinalResultVer1 = new double[NUM_GROUPS];
  private final double[] _expectedFinalResultVer2 = new double[NUM_GROUPS];

  private boolean _useIdenticalSegment = false;

  int[] _intColX = new int[NUM_RECORDS];
  int[] _intColY = new int[NUM_RECORDS];
  long[] _longCol = new long[NUM_RECORDS];
  double[] _floatCol = new double[NUM_RECORDS];
  double[] _doubleColX = new double[NUM_RECORDS];
  double[] _doubleColY = new double[NUM_RECORDS];
  double[] _groupByCol = new double[NUM_RECORDS];

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

  @Override
  protected List<List<IndexSegment>> getDistinctInstances() {
    if (_useIdenticalSegment) {
      return Collections.singletonList(_indexSegments);
    }
    return _distinctInstances;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);

    Random rand = new Random();
    _intColX = rand.ints(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    _intColY = rand.ints(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    _doubleColX = rand.doubles(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    _doubleColY = rand.doubles(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    _longCol = rand.longs(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();

    int groupSize = NUM_RECORDS / NUM_GROUPS;
    double sumX = 0;
    double sumY = 0;
    double sumGroupBy = 0;
    double sumXY = 0;
    double sumXGroupBy = 0;
    int groupByVal = 0;

    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      int intX = _intColX[i];
      int intY = _intColY[i];
      double doubleX = _doubleColX[i];
      double doubleY = _doubleColY[i];
      long longVal = _longCol[i];
      float floatVal = -MAX_VALUE + rand.nextFloat() * 2 * MAX_VALUE;

      // set up inner segment group by results
      groupByVal = (int) Math.floor(i / groupSize);
      if (i % groupSize == 0 && groupByVal > 0) {
        _expectedGroupByResultVer1[groupByVal - 1] = new CovarianceTuple(sumX, sumGroupBy, sumXGroupBy, groupSize);
        _expectedGroupByResultVer2[groupByVal - 1] = new CovarianceTuple(sumX, sumY, sumXY, groupSize);
        sumX = 0;
        sumY = 0;
        sumGroupBy = 0;
        sumXY = 0;
        sumXGroupBy = 0;
      }

      sumX += doubleX;
      sumY += doubleY;
      sumGroupBy += groupByVal;
      sumXY += doubleX * doubleY;
      sumXGroupBy += doubleX * groupByVal;

      _floatCol[i] = floatVal;
      _groupByCol[i] = groupByVal;

      // calculate inner segment results
      _sumIntX += intX;
      _sumIntY += intY;
      _sumDoubleX += doubleX;
      _sumDoubleY += doubleY;
      _sumLong += longVal;
      _sumFloat += floatVal;
      _sumIntXY += intX * intY;
      _sumDoubleXY += doubleX * doubleY;
      _sumIntDouble += intX * doubleX;
      _sumIntLong += intX * longVal;
      _sumIntFloat += intX * _floatCol[i];
      _sumDoubleLong += doubleX * longVal;
      _sumDoubleFloat += doubleX * _floatCol[i];
      _sumLongFloat += longVal * _floatCol[i];

      record.putValue(INT_COLUMN_X, intX);
      record.putValue(INT_COLUMN_Y, intY);
      record.putValue(DOUBLE_COLUMN_X, doubleX);
      record.putValue(DOUBLE_COLUMN_Y, doubleY);
      record.putValue(LONG_COLUMN, longVal);
      record.putValue(FLOAT_COLUMN, floatVal);
      record.putValue(GROUP_BY_COLUMN, groupByVal);
      records.add(record);
    }
    _expectedGroupByResultVer1[groupByVal] = new CovarianceTuple(sumX, sumGroupBy, sumXGroupBy, groupSize);
    _expectedGroupByResultVer2[groupByVal] = new CovarianceTuple(sumX, sumY, sumXY, groupSize);

    // calculate inter segment result
    Covariance cov = new Covariance();
    double[] newIntColX = Arrays.stream(_intColX).asDoubleStream().toArray();
    double[] newIntColY = Arrays.stream(_intColY).asDoubleStream().toArray();
    double[] newLongCol = Arrays.stream(_longCol).asDoubleStream().toArray();
    _expectedCovIntXY = cov.covariance(newIntColX, newIntColY, false);
    _expectedCovDoubleXY = cov.covariance(_doubleColX, _doubleColY, false);
    _expectedCovIntDouble = cov.covariance(newIntColX, _doubleColX, false);
    _expectedCovIntLong = cov.covariance(newIntColX, newLongCol, false);
    _expectedCovIntFloat = cov.covariance(newIntColX, _floatCol, false);
    _expectedCovDoubleLong = cov.covariance(_doubleColX, newLongCol, false);
    _expectedCovDoubleFloat = cov.covariance(_doubleColX, _floatCol, false);
    _expectedCovLongFloat = cov.covariance(newLongCol, _floatCol, false);

    double[] filteredX = Arrays.copyOfRange(_doubleColX, 0, NUM_RECORDS / 2);
    double[] filteredY = Arrays.copyOfRange(_doubleColY, 0, NUM_RECORDS / 2);
    _expectedCovWithFilter = cov.covariance(filteredX, filteredY, false);

    // calculate inter segment group by results
    for (int i = 0; i < NUM_GROUPS; i++) {
      double[] colX = Arrays.copyOfRange(_doubleColX, i * groupSize, (i + 1) * groupSize);
      double[] colGroupBy = Arrays.copyOfRange(_groupByCol, i * groupSize, (i + 1) * groupSize);
      double[] colY = Arrays.copyOfRange(_doubleColY, i * groupSize, (i + 1) * groupSize);
      _expectedFinalResultVer1[i] = cov.covariance(colX, colGroupBy, false);
      _expectedFinalResultVer2[i] = cov.covariance(colX, colY, false);
    }

    // generate testSegment
    ImmutableSegment immutableSegment = setUpSingleSegment(records, SEGMENT_NAME);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);

    // divide testSegment into 4 distinct segments for distinct inter segment tests
    // by doing so, we can avoid calculating global covariance again
    _distinctInstances = new ArrayList<>();
    int segmentSize = NUM_RECORDS / 4;
    ImmutableSegment immutableSegment1 = setUpSingleSegment(records.subList(0, segmentSize), SEGMENT_NAME_1);
    ImmutableSegment immutableSegment2 =
        setUpSingleSegment(records.subList(segmentSize, segmentSize * 2), SEGMENT_NAME_2);
    ImmutableSegment immutableSegment3 =
        setUpSingleSegment(records.subList(segmentSize * 2, segmentSize * 3), SEGMENT_NAME_3);
    ImmutableSegment immutableSegment4 =
        setUpSingleSegment(records.subList(segmentSize * 3, NUM_RECORDS), SEGMENT_NAME_4);
    // generate 2 instances each with 2 distinct segments
    _distinctInstances.add(Arrays.asList(immutableSegment1, immutableSegment2));
    _distinctInstances.add(Arrays.asList(immutableSegment3, immutableSegment4));
  }

  private ImmutableSegment setUpSingleSegment(List<GenericRow> recordSet, String segmentName)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(recordSet));
    driver.build();

    return ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName), ReadMode.mmap);
  }

  @Test
  public void testCovarianceAggregationOnly() {
    // Inner Segment
    String query =
        "SELECT COVAR_POP(intColumnX, intColumnY), COVAR_POP(doubleColumnX, doubleColumnY), COVAR_POP(intColumnX, "
            + "doubleColumnX), " + "COVAR_POP(intColumnX, longColumn), COVAR_POP(intColumnX, floatColumn), "
            + "COVAR_POP(doubleColumnX, longColumn), COVAR_POP(doubleColumnX, floatColumn), COVAR_POP(longColumn, "
            + "floatColumn)  FROM testTable";
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 6, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    checkWithPrecisionForCovariance((CovarianceTuple) aggregationResult.get(0), _sumIntX, _sumIntY, _sumIntXY,
        NUM_RECORDS);
    checkWithPrecisionForCovariance((CovarianceTuple) aggregationResult.get(1), _sumDoubleX, _sumDoubleY, _sumDoubleXY,
        NUM_RECORDS);
    checkWithPrecisionForCovariance((CovarianceTuple) aggregationResult.get(2), _sumIntX, _sumDoubleX, _sumIntDouble,
        NUM_RECORDS);
    checkWithPrecisionForCovariance((CovarianceTuple) aggregationResult.get(3), _sumIntX, _sumLong, _sumIntLong,
        NUM_RECORDS);
    checkWithPrecisionForCovariance((CovarianceTuple) aggregationResult.get(4), _sumIntX, _sumFloat, _sumIntFloat,
        NUM_RECORDS);
    checkWithPrecisionForCovariance((CovarianceTuple) aggregationResult.get(5), _sumDoubleX, _sumLong, _sumDoubleLong,
        NUM_RECORDS);
    checkWithPrecisionForCovariance((CovarianceTuple) aggregationResult.get(6), _sumDoubleX, _sumFloat, _sumDoubleFloat,
        NUM_RECORDS);
    checkWithPrecisionForCovariance((CovarianceTuple) aggregationResult.get(7), _sumLong, _sumFloat, _sumLongFloat,
        NUM_RECORDS);

    // Inter segments with 4 identical segments (2 instances each having 2 identical segments)
    _useIdenticalSegment = true;
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    _useIdenticalSegment = false;
    assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 6 * NUM_RECORDS);
    assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    checkResultTableWithPrecisionForCovariance(brokerResponse);

    // Inter segments with 4 distinct segments (2 instances each having 2 distinct segments)
    brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getNumDocsScanned(), NUM_RECORDS);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 6 * NUM_RECORDS);
    assertEquals(brokerResponse.getTotalDocs(), NUM_RECORDS);
    checkResultTableWithPrecisionForCovariance(brokerResponse);

    // Inter segments with 4 identical segments with filter
    _useIdenticalSegment = true;
    query = "SELECT COVAR_POP(doubleColumnX, doubleColumnY) FROM testTable" + getFilter();
    brokerResponse = getBrokerResponse(query);
    _useIdenticalSegment = false;
    assertEquals(brokerResponse.getNumDocsScanned(), 2 * NUM_RECORDS);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * NUM_RECORDS);
    assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    Object[] results = brokerResponse.getResultTable().getRows().get(0);
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[0], _expectedCovWithFilter, RELATIVE_EPSILON));
  }

  @Test
  public void testCovarianceAggregationGroupBy() {
    // Inner Segment
    // case 1: (col1, groupByCol) group by groupByCol => all covariances are 0's
    String query =
        "SELECT COVAR_POP(doubleColumnX, groupByColumn) FROM testTable GROUP BY groupByColumn ORDER BY groupByColumn";
    GroupByOperator groupByOperator = getOperator(query);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 2, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    for (int i = 0; i < NUM_GROUPS; i++) {
      CovarianceTuple actualCovTuple = (CovarianceTuple) aggregationGroupByResult.getResultForGroupId(0, i);
      CovarianceTuple expectedCovTuple = _expectedGroupByResultVer1[i];
      checkWithPrecisionForCovariance(actualCovTuple, expectedCovTuple);
    }

    // Inter Segment with 4 identical segments
    _useIdenticalSegment = true;
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    checkGroupByResultsForCovariance(brokerResponse, _expectedFinalResultVer1);
    _useIdenticalSegment = false;
    // Inter Segment with 4 distinct segments
    brokerResponse = getBrokerResponse(query);
    checkGroupByResultsForCovariance(brokerResponse, _expectedFinalResultVer1);

    // Inner Segment
    // case 2: COVAR_POP(col1, col2) group by groupByCol => nondeterministic cov
    query =
        "SELECT COVAR_POP(doubleColumnX, doubleColumnY) FROM testTable GROUP BY groupByColumn ORDER BY groupByColumn";
    groupByOperator = getOperator(query);
    resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 3, NUM_RECORDS);
    aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);

    for (int i = 0; i < NUM_GROUPS; i++) {
      CovarianceTuple actualCovTuple = (CovarianceTuple) aggregationGroupByResult.getResultForGroupId(0, i);
      CovarianceTuple expectedCovTuple = _expectedGroupByResultVer2[i];
      checkWithPrecisionForCovariance(actualCovTuple, expectedCovTuple);
    }

    // Inter Segment with 4 identical segments
    _useIdenticalSegment = true;
    brokerResponse = getBrokerResponse(query);
    checkGroupByResultsForCovariance(brokerResponse, _expectedFinalResultVer2);
    _useIdenticalSegment = false;
    // Inter Segment with 4 distinct segments
    brokerResponse = getBrokerResponse(query);
    checkGroupByResultsForCovariance(brokerResponse, _expectedFinalResultVer2);
  }

  @Test
  public void testVarianceTuple() {
    // Start from empty variance and try to merge non-empty variance
    VarianceTuple varianceTuple = new VarianceTuple(0, 0.0, 0.0);
    Variance variance = new Variance(false);
    assertEquals(computeVariancePop(varianceTuple), variance.getResult());
    assertEquals(variance.getResult(), Double.NaN);

    VarianceTuple firstValueVarianceTuple = new VarianceTuple(1, 1.0, 0.0);
    varianceTuple.apply(firstValueVarianceTuple);
    variance.increment(1.0);
    assertTrue(Precision.equalsWithRelativeTolerance(computeVariancePop(varianceTuple), variance.getResult(),
        RELATIVE_EPSILON));

    VarianceTuple secondValueVarianceTuple = new VarianceTuple(1, 3.0, 0.0);
    varianceTuple.apply(secondValueVarianceTuple);
    variance.increment(3.0);
    assertTrue(Precision.equalsWithRelativeTolerance(computeVariancePop(varianceTuple), variance.getResult(),
        RELATIVE_EPSILON));


    // For this time, start from non-empty variance and try to merge empty variance
    varianceTuple = new VarianceTuple(0, 0.0, 0.0);
    varianceTuple.apply(new VarianceTuple(1, 1.0, 0.0));
    varianceTuple.apply(new VarianceTuple(1, 2.0, 0.0));
    variance = new Variance(false);
    variance.increment(1.0);
    variance.increment(2.0);

    varianceTuple.apply(new VarianceTuple(0, 0.0, 0.0));
    assertTrue(Precision.equalsWithRelativeTolerance(computeVariancePop(varianceTuple), variance.getResult(),
        RELATIVE_EPSILON));
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
      expectedVariances[0].increment(_intColX[i]);
      expectedVariances[1].increment(_longCol[i]);
      expectedVariances[2].increment(_floatCol[i]);
      expectedVariances[3].increment(_doubleColX[i]);
      expectedVariances[4].increment(_intColX[i]);
      expectedVariances[5].increment(_longCol[i]);
      expectedVariances[6].increment(_floatCol[i]);
      expectedVariances[7].increment(_doubleColX[i]);
    }
    double expectedIntSum = Arrays.stream(_intColX).asDoubleStream().sum();
    double expectedLongSum = Arrays.stream(_longCol).asDoubleStream().sum();
    double expectedFloatSum = 0.0;
    for (int i = 0; i < _floatCol.length; i++) {
      expectedFloatSum += _floatCol[i];
    }
    double expectedDoubleSum = Arrays.stream(_doubleColX).sum();

    // Compute the query
    String query = "SELECT VAR_POP(intColumnX), VAR_POP(longColumn), VAR_POP(floatColumn), VAR_POP(doubleColumnX),"
        + "VAR_SAMP(intColumnX), VAR_SAMP(longColumn), VAR_SAMP(floatColumn), VAR_SAMP(doubleColumnX) FROM testTable";
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

    // Validate the response
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
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
    query = "SELECT VAR_POP(intColumnX) from testTable" + getFilter();
    brokerResponse = getBrokerResponse(query);
    brokerResponse.getResultTable();
    results = brokerResponse.getResultTable().getRows().get(0);
    Variance filterExpectedVariance = new Variance(false);
    for (int i = 0; i < NUM_RECORDS / 2; i++) {
      filterExpectedVariance.increment(_intColX[i]);
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
      expectedGroupByResult[pos].increment(_intColX[j]);
      expectedSum[pos] += _intColX[j];
    }

    String query = "SELECT VAR_POP(intColumnX) FROM testTable GROUP BY groupByColumn ORDER BY groupByColumn";
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
      expectedStdDevs[0].increment(_intColX[i]);
      expectedStdDevs[1].increment(_longCol[i]);
      expectedStdDevs[2].increment(_floatCol[i]);
      expectedStdDevs[3].increment(_doubleColX[i]);
      expectedStdDevs[4].increment(_intColX[i]);
      expectedStdDevs[5].increment(_longCol[i]);
      expectedStdDevs[6].increment(_floatCol[i]);
      expectedStdDevs[7].increment(_doubleColX[i]);
    }

    double expectedIntSum = Arrays.stream(_intColX).asDoubleStream().sum();
    double expectedLongSum = Arrays.stream(_longCol).asDoubleStream().sum();
    double expectedFloatSum = 0.0;
    for (int i = 0; i < _floatCol.length; i++) {
      expectedFloatSum += _floatCol[i];
    }
    double expectedDoubleSum = Arrays.stream(_doubleColX).sum();

    // Compute the query
    String query =
        "SELECT STDDEV_POP(intColumnX), STDDEV_POP(longColumn), STDDEV_POP(floatColumn), STDDEV_POP(doubleColumnX),"
            + "STDDEV_SAMP(intColumnX), STDDEV_SAMP(longColumn), STDDEV_SAMP(floatColumn), STDDEV_SAMP(doubleColumnX) "
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
    query = "SELECT STDDEV_POP(intColumnX) from testTable" + getFilter();
    brokerResponse = getBrokerResponse(query);
    brokerResponse.getResultTable();
    results = brokerResponse.getResultTable().getRows().get(0);
    StandardDeviation filterExpectedStdDev = new StandardDeviation(false);
    for (int i = 0; i < NUM_RECORDS / 2; i++) {
      filterExpectedStdDev.increment(_intColX[i]);
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
      expectedGroupByResult[pos].increment(_intColX[j]);
      expectedSum[pos] += _intColX[j];
    }

    String query = "SELECT STDDEV_POP(intColumnX) FROM testTable GROUP BY groupByColumn ORDER BY groupByColumn";
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

  @Test
  public void testSkewAggregationOnly() {
    // Compute the expected values
    Skewness[] expectedSkew = new Skewness[4];
    for (int i = 0; i < 4; i++) {
      expectedSkew[i] = new Skewness();
    }

    for (int i = 0; i < NUM_RECORDS; i++) {
      expectedSkew[0].increment(_intColX[i]);
      expectedSkew[1].increment(_longCol[i]);
      expectedSkew[2].increment(_floatCol[i]);
      expectedSkew[3].increment(_doubleColX[i]);
    }

    // Compute the query
    String query =
        "SELECT SKEWNESS(intColumnX), SKEWNESS(longColumn), SKEWNESS(floatColumn), SKEWNESS(doubleColumnX) "
            + "FROM testTable";
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 4, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();

    // Validate the aggregation results
    checkWithPrecisionForSkew((PinotFourthMoment) aggregationResult.get(0), NUM_RECORDS, expectedSkew[0].getResult());
    checkWithPrecisionForSkew((PinotFourthMoment) aggregationResult.get(1), NUM_RECORDS, expectedSkew[1].getResult());
    checkWithPrecisionForSkew((PinotFourthMoment) aggregationResult.get(2), NUM_RECORDS, expectedSkew[2].getResult());
    checkWithPrecisionForSkew((PinotFourthMoment) aggregationResult.get(3), NUM_RECORDS, expectedSkew[3].getResult());

    // Validate the response
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    brokerResponse.getResultTable();
    Object[] results = brokerResponse.getResultTable().getRows().get(0);
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[0], expectedSkew[0].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[1], expectedSkew[1].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[2], expectedSkew[2].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[3], expectedSkew[3].getResult(), RELATIVE_EPSILON));

    // Validate the response for a query with a filter
    query = "SELECT SKEWNESS(intColumnX) from testTable" + getFilter();
    brokerResponse = getBrokerResponse(query);
    brokerResponse.getResultTable();
    results = brokerResponse.getResultTable().getRows().get(0);
    Skewness filterExpectedSkew = new Skewness();
    for (int i = 0; i < NUM_RECORDS / 2; i++) {
      filterExpectedSkew.increment(_intColX[i]);
    }
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[0], filterExpectedSkew.getResult(), RELATIVE_EPSILON));
  }

  @Test
  public void testSkewAggregationGroupBy() {
    // Compute expected group results
    Skewness[] expectedGroupByResult = new Skewness[NUM_GROUPS];

    for (int i = 0; i < NUM_GROUPS; i++) {
      expectedGroupByResult[i] = new Skewness();
    }
    for (int j = 0; j < NUM_RECORDS; j++) {
      int pos = j / (NUM_RECORDS / NUM_GROUPS);
      expectedGroupByResult[pos].increment(_intColX[j]);
    }

    String query = "SELECT SKEWNESS(intColumnX) FROM testTable GROUP BY groupByColumn ORDER BY groupByColumn";
    GroupByOperator groupByOperator = getOperator(query);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 2, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    for (int i = 0; i < NUM_GROUPS; i++) {
      PinotFourthMoment actual = (PinotFourthMoment) aggregationGroupByResult.getResultForGroupId(0, i);
      checkWithPrecisionForSkew(actual, NUM_RECORDS / NUM_GROUPS, expectedGroupByResult[i].getResult());
    }
  }

  @Test
  public void testKurtosisAggregationOnly() {
    // Compute the expected values
    Kurtosis[] expectedKurt = new Kurtosis[4];
    for (int i = 0; i < 4; i++) {
      expectedKurt[i] = new Kurtosis();
    }

    for (int i = 0; i < NUM_RECORDS; i++) {
      expectedKurt[0].increment(_intColX[i]);
      expectedKurt[1].increment(_longCol[i]);
      expectedKurt[2].increment(_floatCol[i]);
      expectedKurt[3].increment(_doubleColX[i]);
    }

    // Compute the query
    String query =
        "SELECT KURTOSIS(intColumnX), KURTOSIS(longColumn), KURTOSIS(floatColumn), "
            + "KURTOSIS(doubleColumnX) FROM testTable";
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 4, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();

    // Validate the aggregation results
    checkWithPrecisionForKurt((PinotFourthMoment) aggregationResult.get(0), NUM_RECORDS, expectedKurt[0].getResult());
    checkWithPrecisionForKurt((PinotFourthMoment) aggregationResult.get(1), NUM_RECORDS, expectedKurt[1].getResult());
    checkWithPrecisionForKurt((PinotFourthMoment) aggregationResult.get(2), NUM_RECORDS, expectedKurt[2].getResult());
    checkWithPrecisionForKurt((PinotFourthMoment) aggregationResult.get(3), NUM_RECORDS, expectedKurt[3].getResult());

    // Validate the response
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    brokerResponse.getResultTable();
    Object[] results = brokerResponse.getResultTable().getRows().get(0);
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[0], expectedKurt[0].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[1], expectedKurt[1].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[2], expectedKurt[2].getResult(), RELATIVE_EPSILON));
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[3], expectedKurt[3].getResult(), RELATIVE_EPSILON));

    // Validate the response for a query with a filter
    query = "SELECT KURTOSIS(intColumnX) from testTable" + getFilter();
    brokerResponse = getBrokerResponse(query);
    brokerResponse.getResultTable();
    results = brokerResponse.getResultTable().getRows().get(0);
    Kurtosis filterExpectedKurt = new Kurtosis();
    for (int i = 0; i < NUM_RECORDS / 2; i++) {
      filterExpectedKurt.increment(_intColX[i]);
    }
    assertTrue(
        Precision.equalsWithRelativeTolerance((double) results[0], filterExpectedKurt.getResult(), RELATIVE_EPSILON));
  }

  @Test
  public void testKurtosisAggregationGroupBy() {
    // Compute expected group results
    Kurtosis[] expectedGroupByResult = new Kurtosis[NUM_GROUPS];

    for (int i = 0; i < NUM_GROUPS; i++) {
      expectedGroupByResult[i] = new Kurtosis();
    }
    for (int j = 0; j < NUM_RECORDS; j++) {
      int pos = j / (NUM_RECORDS / NUM_GROUPS);
      expectedGroupByResult[pos].increment(_intColX[j]);
    }

    String query = "SELECT KURTOSIS(intColumnX) FROM testTable GROUP BY groupByColumn ORDER BY groupByColumn";
    GroupByOperator groupByOperator = getOperator(query);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 2, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    for (int i = 0; i < NUM_GROUPS; i++) {
      PinotFourthMoment actual = (PinotFourthMoment) aggregationGroupByResult.getResultForGroupId(0, i);
      checkWithPrecisionForKurt(actual, NUM_RECORDS / NUM_GROUPS, expectedGroupByResult[i].getResult());
    }
  }

  private void checkWithPrecisionForCovariance(CovarianceTuple tuple, double sumX, double sumY, double sumXY,
      int count) {
    assertEquals(tuple.getCount(), count);
    assertTrue(Precision.equalsWithRelativeTolerance(tuple.getSumX(), sumX, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance(tuple.getSumY(), sumY, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance(tuple.getSumXY(), sumXY, RELATIVE_EPSILON));
  }

  private void checkWithPrecisionForCovariance(CovarianceTuple actual, CovarianceTuple expected) {
    checkWithPrecisionForCovariance(actual, expected.getSumX(), expected.getSumY(), expected.getSumXY(),
        (int) expected.getCount());
  }

  private void checkResultTableWithPrecisionForCovariance(BrokerResponseNative brokerResponse) {
    Object[] results = brokerResponse.getResultTable().getRows().get(0);
    assertEquals(results.length, 8);
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[0], _expectedCovIntXY, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[1], _expectedCovDoubleXY, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[2], _expectedCovIntDouble, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[3], _expectedCovIntLong, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[4], _expectedCovIntFloat, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[5], _expectedCovDoubleLong, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[6], _expectedCovDoubleFloat, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[7], _expectedCovLongFloat, RELATIVE_EPSILON));
  }

  private void checkGroupByResultsForCovariance(BrokerResponseNative brokerResponse, double[] expectedResults) {
    ResultTableRows resultTableRows = brokerResponse.getResultTable();
    List<Object[]> rows = resultTableRows.getRows();
    for (int i = 0; i < NUM_GROUPS; i++) {
      assertTrue(Precision.equals((double) rows.get(i)[0], expectedResults[i], DELTA));
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

  private void checkWithPrecisionForSkew(PinotFourthMoment m4, int expectedCount, double expectedSkew) {
    assertEquals(m4.getN(), expectedCount);
    assertTrue(Precision.equalsWithRelativeTolerance(m4.skew(), expectedSkew, RELATIVE_EPSILON));
  }

  private void checkWithPrecisionForKurt(PinotFourthMoment m4, int expectedCount, double expectedSkew) {
    assertEquals(m4.getN(), expectedCount);
    assertTrue(Precision.equalsWithRelativeTolerance(m4.kurtosis(), expectedSkew, RELATIVE_EPSILON));
  }

  private double computeVariancePop(VarianceTuple varianceTuple) {
    return varianceTuple.getM2() / varianceTuple.getCount();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    for (List<IndexSegment> indexList : _distinctInstances) {
      for (IndexSegment seg : indexList) {
        seg.destroy();
      }
    }
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
