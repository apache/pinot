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
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.selection.SelectionOperatorUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.query.QueryScanCostContext;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Verifies that query operators correctly push scan cost metrics into {@link QueryScanCostContext}
 */
public class OperatorScanCostTrackingTest {
  private static final String RAW_TABLE_NAME = "scanCostTestTable";
  private static final String SEGMENT_NAME = "scanCostTestSegment";
  private static final int NUM_ROWS = 100;
  private static final String COL_INT = "intCol";
  private static final String COL_STRING = "stringCol";
  private static final String COL_DOUBLE = "doubleCol";

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(COL_INT, FieldSpec.DataType.INT)
      .addSingleValueDimension(COL_STRING, FieldSpec.DataType.STRING)
      .addMetric(COL_DOUBLE, FieldSpec.DataType.DOUBLE)
      .build();

  private File _tempDir;
  private IndexSegment _segment;

  @BeforeClass
  public void setUp()
      throws Exception {
    _tempDir = Files.createTempDirectory("OperatorScanCostTrackingTest").toFile();

    List<GenericRow> records = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      row.putValue(COL_INT, i);
      row.putValue(COL_STRING, "val_" + (i % 10));
      row.putValue(COL_DOUBLE, i * 1.5);
      records.add(row);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setTableName(RAW_TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setOutDir(_tempDir.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(records));
    driver.build();

    _segment = ImmutableSegmentLoader.load(new File(_tempDir, SEGMENT_NAME), ReadMode.mmap);
  }

  @Test
  public void testSelectionOnlyOperatorTracksScanCost() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT intCol, stringCol, doubleCol FROM scanCostTestTable LIMIT 50");

    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      QueryScanCostContext scanCost = new QueryScanCostContext();
      QueryThreadContext.get().getExecutionContext().setQueryScanCostContext(scanCost);

      List<ExpressionContext> expressions =
          SelectionOperatorUtils.extractExpressions(queryContext, _segment);
      BaseProjectOperator<?> projectOperator =
          new ProjectPlanNode(new SegmentContext(_segment), queryContext, expressions,
              DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
      int numColumnsProjected = projectOperator.getNumColumnsProjected();

      SelectionOnlyOperator operator =
          new SelectionOnlyOperator(_segment, queryContext, expressions, projectOperator);
      operator.nextBlock();

      long docsScanned = scanCost.getNumDocsScanned();
      assertTrue(docsScanned > 0, "Should have scanned some docs");
      assertEquals(scanCost.getNumEntriesScannedPostFilter(), docsScanned * numColumnsProjected);
    }
  }

  @Test
  public void testSelectionOrderByOperatorTracksScanCost() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT intCol, stringCol FROM scanCostTestTable ORDER BY intCol LIMIT 10");

    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      QueryScanCostContext scanCost = new QueryScanCostContext();
      QueryThreadContext.get().getExecutionContext().setQueryScanCostContext(scanCost);

      List<ExpressionContext> expressions =
          SelectionOperatorUtils.extractExpressions(queryContext, _segment);
      BaseProjectOperator<?> projectOperator =
          new ProjectPlanNode(new SegmentContext(_segment), queryContext, expressions,
              DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
      int numColumnsProjected = projectOperator.getNumColumnsProjected();

      SelectionOrderByOperator operator =
          new SelectionOrderByOperator(_segment, queryContext, expressions, projectOperator);
      operator.nextBlock();

      long docsScanned = scanCost.getNumDocsScanned();
      assertTrue(docsScanned > 0, "Should have scanned some docs");
      assertEquals(scanCost.getNumEntriesScannedPostFilter(), docsScanned * numColumnsProjected);
    }
  }

  @Test
  public void testDistinctOperatorTracksScanCost() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT DISTINCT stringCol FROM scanCostTestTable");

    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      QueryScanCostContext scanCost = new QueryScanCostContext();
      QueryThreadContext.get().getExecutionContext().setQueryScanCostContext(scanCost);

      List<ExpressionContext> expressions = queryContext.getSelectExpressions();
      BaseProjectOperator<?> projectOperator =
          new ProjectPlanNode(new SegmentContext(_segment), queryContext, expressions,
              DocIdSetPlanNode.MAX_DOC_PER_CALL).run();
      int numColumnsProjected = projectOperator.getNumColumnsProjected();

      DistinctOperator operator = new DistinctOperator(_segment, queryContext, projectOperator);
      operator.nextBlock();

      long docsScanned = scanCost.getNumDocsScanned();
      assertTrue(docsScanned > 0, "Should have scanned some docs");
      assertEquals(scanCost.getNumEntriesScannedPostFilter(), docsScanned * numColumnsProjected);
    }
  }

  @Test
  public void testScanCostIsZeroWhenContextNotSet() {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT intCol, stringCol FROM scanCostTestTable LIMIT 10");

    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      // Intentionally NOT setting QueryScanCostContext — getScanCostContext() returns null
      List<ExpressionContext> expressions =
          SelectionOperatorUtils.extractExpressions(queryContext, _segment);
      BaseProjectOperator<?> projectOperator =
          new ProjectPlanNode(new SegmentContext(_segment), queryContext, expressions,
              DocIdSetPlanNode.MAX_DOC_PER_CALL).run();

      SelectionOnlyOperator operator =
          new SelectionOnlyOperator(_segment, queryContext, expressions, projectOperator);
      operator.nextBlock();
      // No exception — scan cost tracking is gracefully skipped when context is null
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _segment.destroy();
    FileUtils.deleteDirectory(_tempDir);
  }
}
