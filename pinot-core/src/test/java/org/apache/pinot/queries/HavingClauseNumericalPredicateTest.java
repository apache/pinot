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
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
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

import static org.testng.Assert.assertTrue;


/**
 * Test cases verifying evaluation of predicate with expressions that contain numerical values of different types.
 */
public class HavingClauseNumericalPredicateTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "HavingPredicateTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_RECORDS = 10;

  private  static final String STRING_COLUMN = "stringColumn";
  private static final String INT_COLUMN = "intColumn";
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
          .addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT).build();
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

  GenericRow createRecord(String stringValue, int intValue) {
    GenericRow record = new GenericRow();
    record.putValue(STRING_COLUMN, stringValue);
    record.putValue(INT_COLUMN, intValue);

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
    records.add(createRecord("daffy", 1));
    records.add(createRecord("mickey", 3));
    records.add(createRecord("minnie", 2));
    records.add(createRecord("goofy", 2));
    records.add(createRecord("daffy", 1));
    records.add(createRecord("daffy", 3));
    records.add(createRecord("minnie", 2));
    records.add(createRecord("pluto", 1));

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


  private List<Object[]> executeHavingQuery(String query) {
    BrokerResponseNative brokerResponseNative = getBrokerResponseForSqlQuery(query);
    return brokerResponseNative.getResultTable().getRows();
  }

  @Test
  public void testHavingClauseGreaterThan() {
      List<Object[]> rows = executeHavingQuery(
          "SELECT stringColumn, count(*) as counter FROM testTable GROUP BY stringColumn HAVING  counter > 4");
      Object[][] expectedRows = {{"daffy", 12L},{"minnie", 8L}};
      assertEquals(rows, expectedRows);
  }

  @Test
  public void testHavingClauseLessThan() {
      List<Object[]> rows = executeHavingQuery(
          "SELECT stringColumn, count(*) as counter FROM testTable GROUP BY stringColumn HAVING  counter < 4.1");
      Object[][] expectedRows = {{"mickey", 4L},{"goofy", 4L},{"pluto", 4L}};
      assertEquals(rows, expectedRows);
  }

  @Test
  public void testHavingClauseLessThanOrGreaterThan() {
    List<Object[]> rows = executeHavingQuery(
        "SELECT stringColumn, sum(intColumn) as summation FROM testTable GROUP BY stringColumn HAVING  summation < 8.1 OR summation > 15.9");
    for (Object[] row : rows) {
      System.out.println(Arrays.toString(row));
    }

    Object[][] expectedRows = {{"daffy", 20D},{"goofy", 8D},{"minnie", 16D},{"pluto", 4D}};
    assertEquals(rows, expectedRows);
  }

  @Test
  public void testHavingClauseEqual() {
    List<Object[]> rows = executeHavingQuery(
        "SELECT stringColumn, count(*) as counter FROM testTable GROUP BY stringColumn HAVING  counter = 3.9");
    Object[][] expectedRows = new Object[0][0];
    assertEquals(rows, expectedRows);
  }

  @Test
  public void testHavingClauseNotEqual() {
    List<Object[]> rows = executeHavingQuery(
        "SELECT stringColumn, count(*) as counter FROM testTable GROUP BY stringColumn HAVING  counter != 4.0000001");
    Object[][] expectedRows = {{"mickey", 4L},{"daffy", 12L},{"goofy", 4L},{"minnie", 8L},{"pluto", 4L}};
    assertEquals(rows, expectedRows);
  }

  @Test
  public void testHavingClauseIn() {
    List<Object[]> rows = executeHavingQuery(
        "SELECT stringColumn, count(*) as counter FROM testTable GROUP BY stringColumn HAVING  counter IN (4.1,4)");
    Object[][] expectedRows = {{"mickey", 4L},{"goofy", 4L}, {"pluto", 4L}};
    assertEquals(rows, expectedRows);
  }

  @Test
  public void testHavingClauseNotIn() {
    List<Object[]> rows = executeHavingQuery(
        "SELECT stringColumn, count(*) as counter FROM testTable GROUP BY stringColumn HAVING  counter NOT IN (4.1,7.9999)");
    Object[][] expectedRows = {{"mickey", 4L}, {"daffy", 12L},{"goofy", 4L},{"minnie", 8L},{"pluto", 4L}};
    assertEquals(rows, expectedRows);
  }

  private static void assertEquals(List<Object[]> actual, Object[][] expected) {
    Assert.assertEquals(actual.size(), expected.length);

    for (int i = 0; i < actual.size(); i++) {
      Object[] actualRow = actual.get(i);
      Object[] expectedRow = expected[i];
      Assert.assertEquals(actualRow, expectedRow);
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
