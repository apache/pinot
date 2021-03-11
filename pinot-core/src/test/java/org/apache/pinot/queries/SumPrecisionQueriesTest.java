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
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BigDecimalUtils;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


@SuppressWarnings("rawtypes")
public class SumPrecisionQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SumPrecisionQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();
  private static final BigDecimal FOUR = BigDecimal.valueOf(4);

  private static final int NUM_RECORDS = 2000;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String BYTES_COLUMN = "bytesColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE).addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private BigDecimal _intSum;
  private BigDecimal _longSum;
  private BigDecimal _floatSum;
  private BigDecimal _doubleSum;
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
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
    FileUtils.deleteQuietly(INDEX_DIR);

    _intSum = BigDecimal.ZERO;
    _longSum = BigDecimal.ZERO;
    _floatSum = BigDecimal.ZERO;
    _doubleSum = BigDecimal.ZERO;
    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      int intValue = RANDOM.nextInt();
      _intSum = _intSum.add(BigDecimal.valueOf(intValue));
      long longValue = RANDOM.nextLong();
      _longSum = _longSum.add(BigDecimal.valueOf(longValue));
      float floatValue = RANDOM.nextFloat();
      _floatSum = _floatSum.add(new BigDecimal(Float.toString(floatValue)));
      double doubleValue = RANDOM.nextDouble();
      String stringValue = Double.toString(doubleValue);
      BigDecimal bigDecimalValue = new BigDecimal(stringValue);
      _doubleSum = _doubleSum.add(bigDecimalValue);
      byte[] bytesValue = BigDecimalUtils.serialize(bigDecimalValue);

      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, intValue);
      record.putValue(LONG_COLUMN, longValue);
      record.putValue(FLOAT_COLUMN, floatValue);
      record.putValue(DOUBLE_COLUMN, doubleValue);
      record.putValue(STRING_COLUMN, stringValue);
      record.putValue(BYTES_COLUMN, bytesValue);
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
  public void testAggregationOnly() {
    String query =
        "SELECT SUM_PRECISION(intColumn), SUM_PRECISION(longColumn), SUM_PRECISION(floatColumn), SUM_PRECISION(doubleColumn), SUM_PRECISION(stringColumn), SUM_PRECISION(bytesColumn) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForSqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    List<Object> aggregationResult = ((AggregationOperator) operator).nextBlock().getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 6);
    assertEquals(aggregationResult.get(0), _intSum);
    assertEquals(aggregationResult.get(1), _longSum);
    assertEquals(aggregationResult.get(2), _floatSum);
    assertEquals(aggregationResult.get(3), _doubleSum);
    assertEquals(aggregationResult.get(4), _doubleSum);
    assertEquals(aggregationResult.get(5), _doubleSum);

    // Inter segment
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
    List<Object[]> rows = brokerResponse.getResultTable().getRows();
    assertEquals(rows.size(), 1);
    BigDecimal intSum = _intSum.multiply(FOUR);
    BigDecimal longSum = _longSum.multiply(FOUR);
    BigDecimal floatSum = _floatSum.multiply(FOUR);
    BigDecimal doubleSum = _doubleSum.multiply(FOUR);
    assertEquals(rows.get(0), new Object[]{intSum, longSum, floatSum, doubleSum, doubleSum, doubleSum});
  }

  @Test
  public void testAggregationWithPrecision() {
    String query =
        "SELECT SUM_PRECISION(intColumn, 6), SUM_PRECISION(longColumn, 6), SUM_PRECISION(floatColumn, 6), SUM_PRECISION(doubleColumn, 6), SUM_PRECISION(stringColumn, 6), SUM_PRECISION(bytesColumn, 6) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForSqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    List<Object> aggregationResult = ((AggregationOperator) operator).nextBlock().getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 6);
    assertEquals(aggregationResult.get(0), _intSum);
    assertEquals(aggregationResult.get(1), _longSum);
    assertEquals(aggregationResult.get(2), _floatSum);
    assertEquals(aggregationResult.get(3), _doubleSum);
    assertEquals(aggregationResult.get(4), _doubleSum);
    assertEquals(aggregationResult.get(5), _doubleSum);

    // Inter segment
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
    List<Object[]> rows = brokerResponse.getResultTable().getRows();
    assertEquals(rows.size(), 1);
    MathContext mathContext = new MathContext(6, RoundingMode.HALF_EVEN);
    BigDecimal intSum = _intSum.multiply(FOUR).round(mathContext);
    BigDecimal longSum = _longSum.multiply(FOUR).round(mathContext);
    BigDecimal floatSum = _floatSum.multiply(FOUR).round(mathContext);
    BigDecimal doubleSum = _doubleSum.multiply(FOUR).round(mathContext);
    assertEquals(rows.get(0), new Object[]{intSum, longSum, floatSum, doubleSum, doubleSum, doubleSum});
  }

  @Test
  public void testAggregationWithPrecisionAndScale() {
    String query =
        "SELECT SUM_PRECISION(intColumn, 10, 3), SUM_PRECISION(longColumn, 10, 3), SUM_PRECISION(floatColumn, 10, 3), SUM_PRECISION(doubleColumn, 10, 3), SUM_PRECISION(stringColumn, 10, 3), SUM_PRECISION(bytesColumn, 10, 3) FROM testTable";

    // Inner segment
    Operator operator = getOperatorForSqlQuery(query);
    assertTrue(operator instanceof AggregationOperator);
    List<Object> aggregationResult = ((AggregationOperator) operator).nextBlock().getAggregationResult();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 6);
    assertEquals(aggregationResult.get(0), _intSum);
    assertEquals(aggregationResult.get(1), _longSum);
    assertEquals(aggregationResult.get(2), _floatSum);
    assertEquals(aggregationResult.get(3), _doubleSum);
    assertEquals(aggregationResult.get(4), _doubleSum);
    assertEquals(aggregationResult.get(5), _doubleSum);

    // Inter segment
    BrokerResponseNative brokerResponse = getBrokerResponseForSqlQuery(query);
    List<Object[]> rows = brokerResponse.getResultTable().getRows();
    assertEquals(rows.size(), 1);
    MathContext mathContext = new MathContext(10, RoundingMode.HALF_EVEN);
    BigDecimal intSum = _intSum.multiply(FOUR).round(mathContext).setScale(3, RoundingMode.HALF_EVEN);
    BigDecimal longSum = _longSum.multiply(FOUR).round(mathContext).setScale(3, RoundingMode.HALF_EVEN);
    BigDecimal floatSum = _floatSum.multiply(FOUR).round(mathContext).setScale(3, RoundingMode.HALF_EVEN);
    BigDecimal doubleSum = _doubleSum.multiply(FOUR).round(mathContext).setScale(3, RoundingMode.HALF_EVEN);
    assertEquals(rows.get(0), new Object[]{intSum, longSum, floatSum, doubleSum, doubleSum, doubleSum});
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
