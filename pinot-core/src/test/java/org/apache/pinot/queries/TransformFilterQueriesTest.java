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
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
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
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class TransformFilterQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "TransformFilterQueriesTest");
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INT_COLUMN = "intColumn";
  private static final String STRING_COLUMN = "stringColumn";

  private static final int NUM_ROWS = 4;

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

  GenericRow createRecord(int intValue, String stringValue) {
    GenericRow record = new GenericRow();
    record.putValue(INT_COLUMN, intValue);
    record.putValue(STRING_COLUMN, stringValue);

    return record;
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    rows.add(createRecord(1, "apple"));
    rows.add(createRecord(2, "banana"));
    rows.add(createRecord(3, "carrot"));
    rows.add(createRecord(4, "fruit"));

    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
        .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING).build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME)
        .setNoDictionaryColumns(List.of(STRING_COLUMN, INT_COLUMN)).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(rows)) {
      driver.init(config, recordReader);
      driver.build();
    }

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Test
  public void testTransformDocIdsLessThanFiltered() {
    Operator<SelectionResultsBlock> operator = getOperator(
        "SELECT * FROM testTable WHERE NOT (intColumn = 3 AND lower(stringColumn) = 'banana')");
    SelectionResultsBlock block = operator.nextBlock();
    List<Object[]> rows = block.getRows();

    assertNotNull(rows);
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0)[0], 1);
    assertEquals(rows.get(1)[0], 2);
    assertEquals(rows.get(2)[0], 3);
    assertEquals(rows.get(3)[0], 4);
  }

  @Test
  public void testTransformDocIdsGreaterThanFiltered() {
    Operator<SelectionResultsBlock> operator = getOperator(
        "SELECT * FROM testTable WHERE NOT (intColumn = 1 AND lower(stringColumn) = 'banana')");
    SelectionResultsBlock block = operator.nextBlock();
    List<Object[]> rows = block.getRows();

    assertNotNull(rows);
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0)[0], 1);
    assertEquals(rows.get(1)[0], 2);
    assertEquals(rows.get(2)[0], 3);
    assertEquals(rows.get(3)[0], 4);
  }

  @Test
  public void testTransformDocIdsEqualToFiltered() {
    Operator<SelectionResultsBlock> operator = getOperator(
        "SELECT * FROM testTable WHERE NOT (intColumn = 2 AND lower(stringColumn) = 'banana')");
    SelectionResultsBlock block = operator.nextBlock();
    List<Object[]> rows = block.getRows();

    assertNotNull(rows);
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0)[0], 1);
    assertEquals(rows.get(1)[0], 3);
    assertEquals(rows.get(2)[0], 4);
  }
}
