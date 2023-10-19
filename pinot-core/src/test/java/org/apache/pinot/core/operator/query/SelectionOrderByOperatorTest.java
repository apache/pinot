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
package org.apache.pinot.core.operator.query;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;


public class SelectionOrderByOperatorTest {
  private static File _tempDir;
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String COL_1 = "col1";
  private static final String COL_2 = "col2";
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE)
      .setTableName(RAW_TABLE_NAME)
      .setSortedColumn(COL_1)
      .build();
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(COL_1, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL_2, FieldSpec.DataType.INT)
      .build();
  private IndexSegment _segmentWithNullValues;

  @BeforeClass
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("SelectionOrderByOperatorTest").toFile();
    _segmentWithNullValues = createOfflineSegmentWithNullValue();
  }

  @Test
  public void testTotalSortNullWithNullHandling() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT col1, col2 "
            + "FROM testTable "
            + "ORDER BY col1, col2 "
            + "LIMIT 1");
    queryContext.setNullHandlingEnabled(true);
    List<Object[]> rows = executeQuery(queryContext);
    assertNull(rows.get(0)[0], "Column 'col1' value should be 'null' when null handling is enabled");
    assertNull(rows.get(0)[1], "Column 'col2' value should be 'null' when null handling is enabled");
  }

  @Test
  public void testTotalSortNullWithoutNullHandling() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT col1, col2 "
            + "FROM testTable "
            + "ORDER BY col1, col2 "
            + "LIMIT 1");
    queryContext.setNullHandlingEnabled(false);
    List<Object[]> rows = executeQuery(queryContext);
    assertNotNull(rows.get(0)[0], "Column 'col1' value should not be 'null' when null handling is disabled");
    assertNotNull(rows.get(0)[1], "Column 'col2' value should not be 'null' when null handling is disabled");
  }

  @Test
  public void testPartialSortNullWithNullHandling() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT col1, col2 "
            + "FROM testTable "
            + "ORDER BY col1 "
            + "LIMIT 1");
    queryContext.setNullHandlingEnabled(true);
    List<Object[]> rows = executeQuery(queryContext);
    assertNull(rows.get(0)[0], "Column 'col1' value should be 'null' when null handling is enabled");
    assertNull(rows.get(0)[1], "Column 'col2' value should be 'null' when null handling is enabled");
  }

  @Test
  public void testPartialSortNullWithoutNullHandling() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT col1, col2 "
            + "FROM testTable "
            + "ORDER BY col1 "
            + "LIMIT 1");
    queryContext.setNullHandlingEnabled(false);
    List<Object[]> rows = executeQuery(queryContext);
    assertNotNull(rows.get(0)[0], "Column 'col1' value should not be 'null' when null handling is disabled");
    assertNotNull(rows.get(0)[1], "Column 'col2' value should not be 'null' when null handling is disabled");
  }

  private List<Object[]> executeQuery(QueryContext queryContext) {
    List<ExpressionContext> expressions =
        SelectionOperatorUtils.extractExpressions(queryContext, _segmentWithNullValues);
    BaseProjectOperator<?> projectOperator = new ProjectPlanNode(_segmentWithNullValues, queryContext, expressions,
        DocIdSetPlanNode.MAX_DOC_PER_CALL).run();

    SelectionOrderByOperator operator = new SelectionOrderByOperator(
        _segmentWithNullValues,
        queryContext,
        expressions,
        projectOperator
    );
    SelectionResultsBlock block = operator.getNextBlock();
    return block.getRows();
  }

  private IndexSegment createOfflineSegmentWithNullValue()
      throws Exception {

    List<GenericRow> records = new ArrayList<>();

    // Add one row with null value
    GenericRow record = new GenericRow();
    record.addNullValueField(COL_1);
    record.addNullValueField(COL_2);
    records.add(record);

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(_tempDir.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    return ImmutableSegmentLoader.load(new File(_tempDir, SEGMENT_NAME), ReadMode.mmap);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _segmentWithNullValues.destroy();
    FileUtils.deleteDirectory(_tempDir);
  }
}
