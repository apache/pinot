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
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for histogram queries.
 */
public class CovarianceQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(),"CovarianceQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 2000;
  private static final int MAX_VALUE = 100;

  private static final String INT_COLUMN_X = "intColumnX";
  private static final String INT_COLUMN_Y = "intColumnY";
  private static final String DOUBLE_COLUMN_X = "doubleColumnX";
  private static final String DOUBLE_COLUMN_Y = "doubleColumnY";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";

  private static final Schema
      SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN_X, FieldSpec.DataType.INT).addSingleValueDimension(INT_COLUMN_Y, FieldSpec.DataType.INT).addSingleValueDimension(DOUBLE_COLUMN_X, FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension(DOUBLE_COLUMN_Y, FieldSpec.DataType.DOUBLE).addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, FieldSpec.DataType.FLOAT)
      .build();
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

  private long _sumLong = 0l;
  private float _sumFloat = 0f;

  private double _sumIntDouble = 0;
  private long _sumIntLong = 0l;
  private float _sumIntFloat = 0f;
  private double _sumDoubleLong = 0;
  private double _sumDoubleFloat = 0;
  private float _sumLongFloat = 0f;

  private double _expectedCovIntXY;
  private double _expectedCovDoubleXY;
  private double _expectedCovIntDouble;
  private double _expectedCovIntLong;
  private double _expectedCovIntFloat;
  private double _expectedCovDoubleLong;
  private double _expectedCovDoubleFloat;
  private double _expectedCovLongFloat;


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
  public void setUp() throws Exception{
    FileUtils.deleteDirectory(INDEX_DIR);
    Random rand = new Random();
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    int[] intColX = rand.ints(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    int[] intColY = rand.ints(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    double[] doubleColX = rand.doubles(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    double[] doubleColY = rand.doubles(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();
    long[] longCol = rand.longs(NUM_RECORDS, -MAX_VALUE, MAX_VALUE).toArray();

    for (int i  = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      int intX = intColX[i];
      int intY = intColY[i];
      double doubleX = doubleColX[i];
      double doubleY = doubleColY[i];
      long longVal = longCol[i];
      float floatVal = -MAX_VALUE + rand.nextFloat() * 2 * MAX_VALUE;

      _sumIntXY += intX * intY;
      _sumDoubleXY += doubleX * doubleY;
      _sumIntDouble += intX * doubleX;
      _sumIntLong += intX * longVal;
      _sumIntFloat += intX * floatVal;
      _sumDoubleLong += doubleX * longVal;
      _sumDoubleFloat += doubleX * floatVal;
      _sumLongFloat += longVal * floatVal;
      _sumFloat += floatVal;

      record.putValue(INT_COLUMN_X, intX);
      record.putValue(INT_COLUMN_Y, intY);
      record.putValue(DOUBLE_COLUMN_X, doubleX);
      record.putValue(DOUBLE_COLUMN_Y, doubleY);
      record.putValue(LONG_COLUMN, longVal);
      record.putValue(FLOAT_COLUMN, floatVal);
      records.add(record);
    }

    _sumIntX = Arrays.stream(intColX).sum();
    _sumIntY = Arrays.stream(intColY).sum();
    _sumDoubleX = Arrays.stream(doubleColX).sum();
    _sumDoubleY = Arrays.stream(doubleColY).sum();
    _sumLong = Arrays.stream(longCol).sum();



//    Covariance cov = new Covariance();
//    _expectedCovIntXY = cov.covariance(intColX, intColY, false);
//    _expectedCovDoubleXY = cov.covariance(doubleColX, doubleColY, false);
//    _expectedCovIntDouble = cov.covariance(intColX, doubleColX, false);
//    _expectedCovIntLong = cov.covariance(intColX, longCol, false);
//    _expectedCovIntFloat = cov.covariance(intColX, floatCol, false);
//    _expectedCovDoubleLong = cov.covariance(doubleColX, longCol, false);
//    _expectedCovDoubleFloat = cov.covariance(doubleColX, floatCol, false);
//    _expectedCovLongFloat = cov.covariance(longCol, floatCol, false);

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
  public void testAggregationOnly(){
    // Inner Segment
    String query =
        "SELECT COV_POP(intColumnX, intColumnY), COV_POP(doubleColumnX, doubleColumnY), COV_POP(intColumnX, "
            + "doubleColumnX), "
            + "COV_POP(intColumnX, longColumn), COV_POP(intColumnX, floatColumn), "
            + "COV_POP(doubleColumnX, longColumn), COV_POP(doubleColumnX, floatColumn), COV_POP(longColumn, "
            + "floatColumn)  FROM testTable";
    Object operator = getOperator(query);
    assertTrue(operator instanceof AggregationOperator);
    IntermediateResultsBlock resultsBlock = ((AggregationOperator) operator).nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(((Operator) operator).getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS * 6, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getAggregationResult();
    assertNotNull(aggregationResult);
    checkWithPrecision(4, (CovarianceTuple) aggregationResult.get(0), _sumIntX, _sumIntY, _sumIntXY);
    checkWithPrecision(4, (CovarianceTuple) aggregationResult.get(1), _sumDoubleX, _sumDoubleY, _sumDoubleXY);
    checkWithPrecision(4, (CovarianceTuple) aggregationResult.get(2), _sumIntX, _sumDoubleX, _sumIntDouble);
    checkWithPrecision(4, (CovarianceTuple) aggregationResult.get(3), _sumIntX, _sumLong, _sumIntLong);
    checkWithPrecision(2, (CovarianceTuple) aggregationResult.get(4), _sumIntX, _sumFloat, _sumIntFloat);
    checkWithPrecision(4, (CovarianceTuple) aggregationResult.get(5), _sumDoubleX, _sumLong, _sumDoubleLong);
    checkWithPrecision(2, (CovarianceTuple) aggregationResult.get(6), _sumDoubleX, _sumFloat, _sumDoubleFloat);
    checkWithPrecision(2, (CovarianceTuple) aggregationResult.get(7), _sumLong, _sumFloat, _sumLongFloat);



//    assertEquals(((CovarianceTuple) aggregationResult.get(0)).getSumX(), (double) _sumIntX);
//    assertEquals(((CovarianceTuple) aggregationResult.get(0)).getSumY(), (double) _sumIntY);
//    assertEquals(((CovarianceTuple) aggregationResult.get(0)).getSumXY(), (double) _sumIntXY);
//
//    assertEquals(((CovarianceTuple)aggregationResult.get(1)).getSumX(), _sumDoubleX);
//    assertEquals(((CovarianceTuple)aggregationResult.get(1)).getSumY(), _sumDoubleY);
//    assertEquals(((CovarianceTuple)aggregationResult.get(1)).getSumXY(), _sumDoubleXY);
//
//    assertEquals(((CovarianceTuple)aggregationResult.get(2)).getSumX(), (double) _sumIntX);
//    assertEquals(((CovarianceTuple)aggregationResult.get(2)).getSumY(), _sumDoubleX);
//    assertEquals(((CovarianceTuple)aggregationResult.get(2)).getSumXY(), _sumIntDouble);

//    assertEquals(((CovarianceTuple)aggregationResult.get(3)).getSumX(), (double) _sumIntX);
//    assertEquals(((CovarianceTuple)aggregationResult.get(3)).getSumY(), _sumLong);
//    assertEquals(((CovarianceTuple)aggregationResult.get(3)).getSumXY(), _sumIntLong);

//    assertEquals(((CovarianceTuple)aggregationResult.get(4)).getSumX(), (double) _sumIntX);
//    assertEquals(((CovarianceTuple)aggregationResult.get(4)).getSumY(), _sumFloat);
//    assertEquals(((CovarianceTuple)aggregationResult.get(4)).getSumXY(), _sumIntFloat);
//
//    assertEquals(((CovarianceTuple)aggregationResult.get(5)).getSumX(), _sumDoubleX);
//    assertEquals(((CovarianceTuple)aggregationResult.get(5)).getSumY(), _sumLong);
//    assertEquals(((CovarianceTuple)aggregationResult.get(5)).getSumXY(), _sumDoubleLong);

//    assertEquals(((CovarianceTuple)aggregationResult.get(6)).getSumX(), _sumDoubleX);
//    assertEquals(((CovarianceTuple)aggregationResult.get(6)).getSumY(), _sumFloat);
//    assertEquals(((CovarianceTuple)aggregationResult.get(6)).getSumXY(), _sumDoubleFloat);
//
//    assertEquals(((CovarianceTuple)aggregationResult.get(7)).getSumX(), _sumLong);
//    assertEquals(((CovarianceTuple)aggregationResult.get(7)).getSumY(), _sumFloat);
//    assertEquals(((CovarianceTuple)aggregationResult.get(7)).getSumXY(), _sumLongFloat);

//    assertEquals(aggregationResult.get(1), _expectedCovDoubleXY);
//    assertEquals(aggregationResult.get(2), _expectedCovIntDouble);
//    assertEquals(aggregationResult.get(3), _expectedCovIntLong);
//    assertEquals(aggregationResult.get(4), _expectedCovIntFloat);
//    assertEquals(aggregationResult.get(5), _expectedCovDoubleLong);
//    assertEquals(aggregationResult.get(6), _expectedCovDoubleFloat);
//    assertEquals(aggregationResult.get(7), _expectedCovLongFloat);
  }

  private void checkWithPrecision(int scale, CovarianceTuple tuple, double sumX, double sumY, double sumXY) {
    assertEquals(BigDecimal.valueOf(tuple.getSumX()).setScale(scale, RoundingMode.HALF_UP), BigDecimal.valueOf(sumX).setScale(scale, RoundingMode.HALF_UP));
    assertEquals(BigDecimal.valueOf(tuple.getSumY()).setScale(scale, RoundingMode.HALF_UP), BigDecimal.valueOf(sumY).setScale(scale, RoundingMode.HALF_UP));
    assertEquals(BigDecimal.valueOf(tuple.getSumXY()).setScale(scale, RoundingMode.HALF_UP), BigDecimal.valueOf(sumXY).setScale(scale, RoundingMode.HALF_UP));
  }
}
