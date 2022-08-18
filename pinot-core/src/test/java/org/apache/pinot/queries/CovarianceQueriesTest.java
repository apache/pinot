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
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Precision;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOrderByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.segment.local.customobject.CovarianceTuple;
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
 * Queries test for covariance queries.
 */
public class CovarianceQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "CovarianceQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 2000;
  private static final int MAX_VALUE = 500;
  private static final double RELATIVE_EPSILON = 0.00001;
  private static final int NUM_GROUPS = 10;

  private static final String INT_COLUMN_X = "intColumnX";
  private static final String INT_COLUMN_Y = "intColumnY";
  private static final String DOUBLE_COLUMN_X = "doubleColumnX";
  private static final String DOUBLE_COLUMN_Y = "doubleColumnY";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String GROUP_BY_COLUMN = "groupByColumn";

  // add a group by column with 10 groups across 2000 rows
  // case 1: group by + cov called on this column => all covariances are 0's
  // case 2: group by called on this column => compare results against java's cov func

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

  private CovarianceTuple[] _expectedGroupByResultVer1;
  private CovarianceTuple[] _expectedGroupByResultVer2;
  private double[] _expectedFinalResultVer1;
  private double[] _expectedFinalResultVer2;

  @Override
  protected String getFilter() {
    // TODO
    return null;
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
    Random rand = new Random();
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    int[] intColX = rand.ints(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    int[] intColY = rand.ints(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    double[] doubleColX = rand.doubles(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    double[] doubleColY = rand.doubles(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    long[] longCol = rand.longs(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    double[] floatCol = new double[NUM_RECORDS];
    double[] groupByCol = new double[NUM_RECORDS];

    // set up group by results
    _expectedGroupByResultVer1 = new CovarianceTuple[NUM_GROUPS];
    _expectedGroupByResultVer2 = new CovarianceTuple[NUM_GROUPS];
    _expectedFinalResultVer1 = new double[NUM_GROUPS];
    _expectedFinalResultVer2 = new double[NUM_GROUPS];
    int groupSize = NUM_RECORDS / NUM_GROUPS;
    double sumX = 0;
    double sumY = 0;
    double sumGroupBy = 0;
    double sumXY = 0;
    double sumXGroupBy = 0;
    int groupByVal = 0;

    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      int intX = intColX[i];
      int intY = intColY[i];
      double doubleX = doubleColX[i];
      double doubleY = doubleColY[i];
      long longVal = longCol[i];
      float floatVal = -MAX_VALUE + rand.nextFloat() * 2 * MAX_VALUE;

      // set up inner seg group by results
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

      floatCol[i] = floatVal;
      groupByCol[i] = groupByVal;
      _sumIntXY += intX * intY;
      _sumDoubleXY += doubleX * doubleY;
      _sumIntDouble += intX * doubleX;
      _sumIntLong += intX * longVal;
      _sumIntFloat += intX * floatCol[i];
      _sumDoubleLong += doubleX * longVal;
      _sumDoubleFloat += doubleX * floatCol[i];
      _sumLongFloat += longVal * floatCol[i];

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

    _sumIntX = Arrays.stream(intColX).sum();
    _sumIntY = Arrays.stream(intColY).sum();
    _sumDoubleX = Arrays.stream(doubleColX).sum();
    _sumDoubleY = Arrays.stream(doubleColY).sum();
    _sumLong = Arrays.stream(longCol).sum();
    _sumFloat = Arrays.stream(floatCol).sum();

    Covariance cov = new Covariance();
    double[] newIntColX = Arrays.stream(intColX).asDoubleStream().toArray();
    double[] newIntColY = Arrays.stream(intColY).asDoubleStream().toArray();
    double[] newLongCol = Arrays.stream(longCol).asDoubleStream().toArray();
    _expectedCovIntXY = cov.covariance(newIntColX, newIntColY, false);
    _expectedCovDoubleXY = cov.covariance(doubleColX, doubleColY, false);
    _expectedCovIntDouble = cov.covariance(newIntColX, doubleColX, false);
    _expectedCovIntLong = cov.covariance(newIntColX, newLongCol, false);
    _expectedCovIntFloat = cov.covariance(newIntColX, floatCol, false);
    _expectedCovDoubleLong = cov.covariance(doubleColX, newLongCol, false);
    _expectedCovDoubleFloat = cov.covariance(doubleColX, floatCol, false);
    _expectedCovLongFloat = cov.covariance(newLongCol, floatCol, false);

    // set up group by final results
    for (int i = 0; i < NUM_GROUPS; i++) {
      double[] colX = Arrays.copyOfRange(doubleColX, i * groupSize, (i + 1) * groupSize);
      double[] colGroupBy = Arrays.copyOfRange(groupByCol, i * groupSize, (i + 1) * groupSize);
      double[] colY = Arrays.copyOfRange(doubleColY, i * groupSize, (i + 1) * groupSize);
      _expectedFinalResultVer1[i] = cov.covariance(colX, colGroupBy, false);
      _expectedFinalResultVer2[i] = cov.covariance(colX, colY, false);
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
  public void testAggregationOnly() {
    // Inner Segment
    String query = "SELECT COV_POP(intColumnX, intColumnY), COV_POP(doubleColumnX, doubleColumnY), COV_POP(intColumnX, "
        + "doubleColumnX), " + "COV_POP(intColumnX, longColumn), COV_POP(intColumnX, floatColumn), "
        + "COV_POP(doubleColumnX, longColumn), COV_POP(doubleColumnX, floatColumn), COV_POP(longColumn, "
        + "floatColumn)  FROM testTable";
    Object operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 6, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    assertNotNull(aggregationResult);
    checkWithPrecision((CovarianceTuple) aggregationResult.get(0), _sumIntX, _sumIntY, _sumIntXY, NUM_RECORDS);
    checkWithPrecision((CovarianceTuple) aggregationResult.get(1), _sumDoubleX, _sumDoubleY, _sumDoubleXY, NUM_RECORDS);
    checkWithPrecision((CovarianceTuple) aggregationResult.get(2), _sumIntX, _sumDoubleX, _sumIntDouble, NUM_RECORDS);
    checkWithPrecision((CovarianceTuple) aggregationResult.get(3), _sumIntX, _sumLong, _sumIntLong, NUM_RECORDS);
    checkWithPrecision((CovarianceTuple) aggregationResult.get(4), _sumIntX, _sumFloat, _sumIntFloat, NUM_RECORDS);
    checkWithPrecision((CovarianceTuple) aggregationResult.get(5), _sumDoubleX, _sumLong, _sumDoubleLong, NUM_RECORDS);
    checkWithPrecision((CovarianceTuple) aggregationResult.get(6), _sumDoubleX, _sumFloat, _sumDoubleFloat,
        NUM_RECORDS);
    checkWithPrecision((CovarianceTuple) aggregationResult.get(7), _sumLong, _sumFloat, _sumLongFloat, NUM_RECORDS);

    // Inter segments
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
    assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
    assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 6 * NUM_RECORDS);
    assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
    Object[] results = brokerResponse.getResultTable().getRows().get(0);
    assertEquals(results.length, 8);
    // Cov results should be the same as if there's only one segment
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[0], _expectedCovIntXY, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[1], _expectedCovDoubleXY, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[2], _expectedCovIntDouble, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[3], _expectedCovIntLong, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[4], _expectedCovIntFloat, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[5], _expectedCovDoubleLong, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[6], _expectedCovDoubleFloat, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance((double) results[7], _expectedCovLongFloat, RELATIVE_EPSILON));
  }

  @Test
  public void testAggregationGroupBy() {
    // Inner Segment
    String query =
        "SELECT COV_POP(doubleColumnX, groupByColumn) FROM testTable GROUP BY groupByColumn ORDER BY groupByColumn";
    Object operator = getOperator(query);
    assertTrue(operator instanceof AggregationGroupByOrderByOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationGroupByOrderByOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 2, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);

    for (int i = 0; i < NUM_GROUPS; i++) {
      CovarianceTuple actualCovTuple = (CovarianceTuple) aggregationGroupByResult.getResultForGroupId(0, i);
      CovarianceTuple expectedCovTuple = _expectedGroupByResultVer1[i];
      checkWithPrecision(actualCovTuple, expectedCovTuple);
    }

    // Inter Segment
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    ResultTable resultTable = brokerResponse.getResultTable();
    List<Object[]> rows = resultTable.getRows();
    for (int i = 0; i < NUM_GROUPS; i++) {
      Precision.equalsWithRelativeTolerance((double) rows.get(i)[0], _expectedFinalResultVer1[i], RELATIVE_EPSILON);
      Precision.equals((double) rows.get(i)[0], _expectedFinalResultVer2[i], 0.0001);
    }

    // Inner Segment
    query = "SELECT COV_POP(doubleColumnX, doubleColumnY) FROM testTable GROUP BY groupByColumn ORDER BY groupByColumn";
    operator = getOperator(query);
    assertTrue(operator instanceof AggregationGroupByOrderByOperator);
    resultsBlock = ((AggregationGroupByOrderByOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 3, NUM_RECORDS);
    aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);

    for (int i = 0; i < NUM_GROUPS; i++) {
      CovarianceTuple actualCovTuple = (CovarianceTuple) aggregationGroupByResult.getResultForGroupId(0, i);
      CovarianceTuple expectedCovTuple = _expectedGroupByResultVer2[i];
      checkWithPrecision(actualCovTuple, expectedCovTuple);
    }

    // Inter Segment
    brokerResponse = getBrokerResponse(query);
    resultTable = brokerResponse.getResultTable();
    rows = resultTable.getRows();
    for (int i = 0; i < NUM_GROUPS; i++) {
      Precision.equalsWithRelativeTolerance((double) rows.get(i)[0], _expectedFinalResultVer2[i], RELATIVE_EPSILON);
      Precision.equals((double) rows.get(i)[0], _expectedFinalResultVer2[i], 0.0001);
    }
  }

  private void checkWithPrecision(CovarianceTuple tuple, double sumX, double sumY, double sumXY, int count) {
    assertEquals(tuple.getCount(), count);
    assertTrue(Precision.equalsWithRelativeTolerance(tuple.getSumX(), sumX, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance(tuple.getSumY(), sumY, RELATIVE_EPSILON));
    assertTrue(Precision.equalsWithRelativeTolerance(tuple.getSumXY(), sumXY, RELATIVE_EPSILON));
  }

  private void checkWithPrecision(CovarianceTuple actual, CovarianceTuple expected) {
    checkWithPrecision(actual, expected.getSumX(), expected.getSumY(), expected.getSumXY(), (int) expected.getCount());
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
