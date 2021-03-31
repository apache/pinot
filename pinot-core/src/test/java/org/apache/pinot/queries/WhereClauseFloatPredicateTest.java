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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test cases verifying evaluation of predicate with expressions that contain numerical values of different types.
 */
public class WhereClauseFloatPredicateTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NumericalFloatPredicateTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_RECORDS = 10;

  private static final String INT_COLUMN = "intColumn";
  private static final String ANOTHER_FLOAT_COLUMN = "anotherFloatColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(ANOTHER_FLOAT_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
      .addSingleValueDimension(FLOAT_COLUMN, FieldSpec.DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE)
      .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

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

  GenericRow createRecord(int intValue, float anotherFloatValue, long longValue, float floatValue, double doubleValue, String stringValue) {
    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, intValue);
    record.putValue(ANOTHER_FLOAT_COLUMN, anotherFloatValue);
    record.putValue(LONG_COLUMN, longValue);
    record.putValue(FLOAT_COLUMN, floatValue);
    record.putValue(DOUBLE_COLUMN, doubleValue);
    record.putValue(STRING_COLUMN, stringValue);

    return record;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    records.add(createRecord(12, 5.1f,1609046259848l, 12.1f, 12.01d, "scrooge"));
    records.add(createRecord(1, 0.2f,1609046249848l, 12.101f, 1.11d, "mickey"));
    records.add(createRecord(25,25.1f, 1609046359848l, 25.1f, 25.01d, "minnie"));
    records.add(createRecord(40, 20.0f, 1609046659848l, 40.1f, 40.11d, "donald"));
    records.add(createRecord(15, 20.1f, 1609046219848l, 15.1f, 10.01d, "goofy"));
    records.add(createRecord(45,15.9f, 1609046279848l, 45.1f, 45.11d, "daffy1"));
    records.add(createRecord(45,90.0001f, 1609046279848l, -45.1f, 45.11d, "daffy2"));
    records.add(createRecord(45,38.05f, 1609046279848l, -15.1f, 45.11d, "daffy3"));
    records.add(createRecord(45,105.0f, 1609046279848l, -2.1f, 45.11d, "daffy4"));
    records.add(createRecord(45,105.0f, 1609046279848l, -12.1f, 45.11d, "daffy4"));
    records.add(createRecord(Integer.MAX_VALUE, 0, Long.MAX_VALUE, Float.MAX_VALUE, 47.01d, "pluto"));
    records.add(createRecord(Integer.MIN_VALUE, 0, Long.MIN_VALUE, -Float.MAX_VALUE, 49.11d, "daisy"));

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

  /** Check if we can compare an FLOAT column with an int value. */
  @Test
  public void testFloatColumnEqualToDecimalValue() {
    Operator operator = getOperatorForSqlQuery("SELECT count(*) FROM testTable WHERE floatColumn = 25.1");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 1l);
  }

  /** Check if we can compare an FLOAT column with an int value. */
  @Test
  public void testFloatColumnEqualToAnotherDecimalValue() {
    Operator operator = getOperatorForSqlQuery("SELECT count(*) FROM testTable WHERE floatColumn = 25.2");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 0l);
  }

  /** Check if we can compare an FLOAT column with a decimal value. */
  @Test
  public void testFloatColumnNotEqualToDecimalValue() {
    Operator operator = getOperatorForSqlQuery("SELECT count(*) FROM testTable WHERE floatColumn != 40.00000000000");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 12l);
  }

  /** Check if we can compare an FLOAT column with a decimal value. */
  @Test
  public void testFloatColumnGreaterThanPositiveDecimalValue() {
    Operator operator = getOperatorForSqlQuery("SELECT count(*) FROM testTable WHERE floatColumn > 12.1");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 6l);
  }

  /** Check if we can compare an FLOAT column with a decimal value. */
  @Test
  public void testFloatColumnGreaterThanOrEqualToPositiveDecimalValue() {
    Operator operator = getOperatorForSqlQuery("SELECT count(*) FROM testTable WHERE floatColumn >= 12.1");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 7l);
  }

  /** Check if we can compare an FLOAT column with a decimal value. */
  @Test
  public void testFloatColumnLessThanPositiveDecimalValue() {
    Operator operator = getOperatorForSqlQuery("SELECT count(*) FROM testTable WHERE floatColumn < 12.1");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 5l);
  }

  /** Check if we can compare an FLOAT column with a decimal value. */
  @Test
  public void testFloatColumnLessThanOrEqualToPositiveDecimalValue() {
    Operator operator = getOperatorForSqlQuery("SELECT count(*) FROM testTable WHERE floatColumn <= 12.1");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 6l);
  }

  /** Check if we can compare an FLOAT column with a decimal value. */
  @Test
  public void testFloatColumnGreaterThanNegativeDecimalValue() {
    Operator operator = getOperatorForSqlQuery("SELECT count(*) FROM testTable WHERE floatColumn > -12.1");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 8l);
  }

  /** Check if we can compare an FLOAT column with a decimal value. */
  @Test
  public void testFloatColumnGreaterThanOrEqualToNegativeDecimalValue() {
    Operator operator = getOperatorForSqlQuery("SELECT count(*) FROM testTable WHERE floatColumn >= -12.1");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 9l);
  }

  /** Check if we can compare an FLOAT column with a decimal value. */
  @Test
  public void testFloatColumnLessThanNegativeDecimalValue() {
    Operator operator = getOperatorForSqlQuery("SELECT count(*) FROM testTable WHERE floatColumn < -12.1");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 3l);
  }

  /** Check if we can compare an FLOAT column with a decimal value. */
  @Test
  public void testFloatColumnLessThanOrEqualToNegativeDecimalValue() {
    Operator operator = getOperatorForSqlQuery("SELECT count(*) FROM testTable WHERE floatColumn <= -12.1");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 4l);
  }

  /** Check integer comparison with value exceeding integer maximum. */
  @Test
  public void testFloatColumnLessThanDecimalValueWithSixDecimalPlaces() {
    Operator operator = getOperatorForSqlQuery("select count(*) from scores where floatColumn < 12.100001");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 6l);
  }

  /** Check integer comparison with value exceeding integer maximum. */
  @Test
  public void testFloatColumnLessThanPositiveDecimalValueWithTenDecimalPlaces() {
    Operator operator = getOperatorForSqlQuery("select count(*) from scores where floatColumn < 12.1000000001");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 6l);
  }

  /** Check float comparison with value less than a number below float minimum. */
  //@Test TODO: Fix Calcite bug.
  public void testFloatColumnGreaterThanOverflowValue() {
    Operator operator = getOperatorForSqlQuery("select count(*) from scores where floatColumn > 3.5028235E38");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 0l);
  }

  /** Check float comparison with value less than a number below float maximum. */
  //@Test TODO: Fix Calcite bug.
  public void testFloatColumnLessThanOverflowValue() {
    Operator operator = getOperatorForSqlQuery("select count(*) from scores where floatColumn < 3.5028235E38");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 12l);
  }

  /** Check float comparison with value less than a number below float minimum. */
  //@Test TODO: Fix Calcite bug.
  public void testFloatColumnGreaterThanUnderflowValue() {
    Operator operator = getOperatorForSqlQuery("select count(*) from scores where floatColumn > -3.5028235E38");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 12l);
  }

  /** Check float comparison with value less than a number below float maximum. */
  //@Test TODO: Fix Calcite bug.
  public void testFloatColumnLessThanUnderflowValue() {
    Operator operator = getOperatorForSqlQuery("select count(*) from scores where floatColumn < -3.5028235E38");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 0l);
  }

  /** Check if we can use columns of different numerical types in calculations. */
  @Test
  public void testDoubleColumnAndFloatColumnCalculation() {
    Operator operator = getOperatorForSqlQuery("select count(*) from scores where doubleColumn - floatColumn < 0");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 5l);
  }

  /** Check if we can compare two columns of different numerical types. */
  @Test
  public void testCompareFloatColumnWithLongColumn() {
    Operator operator = getOperatorForSqlQuery("select count(*) from scores where floatColumn > doubleColumn");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 5l);
  }

  /** Check if we can use values of different numerical types in IN predicate. */
  @Test
  public void testFloatColumnWithInPredicate() {
    Operator operator = getOperatorForSqlQuery("select count(*) from scores where floatColumn IN (12.1, 25.1, 45.1, -45.1)");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 4l);
  }

  /** Check if we can use values of different numerical types in NOT IN predicate. */
  @Test
  public void testFloatColumnWithNotInPredicate() {
    Operator operator = getOperatorForSqlQuery("select count(*) from scores where floatColumn NOT IN (12.1, 12.101, 25.1, 45.1, -45.1)");
    IntermediateResultsBlock block = (IntermediateResultsBlock) operator.nextBlock();
    List<Object> result = block.getAggregationResult();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), 7l);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
