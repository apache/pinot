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
package org.apache.pinot.core.operator.streaming;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.segment.spi.datasource.NullMode;
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

import static org.testng.Assert.assertNull;


public class StreamingSelectionOnlyOperatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "StreamingSelectionOperatorTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_COLUMN = "intColumn";
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT).build();
  private IndexSegment _segmentWithNullValues;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);
    _segmentWithNullValues = createOfflineSegmentWithNullValue();
  }

  @Test
  public void testNullHandling() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable WHERE intColumn IS NULL");
    queryContext.setNullMode(NullMode.ALL_NULLABLE);
    List<ExpressionContext> expressions =
        SelectionOperatorUtils.extractExpressions(queryContext, _segmentWithNullValues);
    BaseProjectOperator<?> projectOperator = new ProjectPlanNode(_segmentWithNullValues, queryContext, expressions,
        DocIdSetPlanNode.MAX_DOC_PER_CALL).run();

    StreamingSelectionOnlyOperator operator = new StreamingSelectionOnlyOperator(
        _segmentWithNullValues,
        queryContext,
        expressions,
        projectOperator
    );
    SelectionResultsBlock block = operator.getNextBlock();
    List<Object[]> rows = block.getRows();
    assertNull(rows.get(0)[0], "Column value should be 'null' when null handling is enabled");
  }

  private IndexSegment createOfflineSegmentWithNullValue()
      throws Exception {

    List<GenericRow> records = new ArrayList<>();

    // Add one row with null value
    GenericRow record = new GenericRow();
    record.addNullValueField(INT_COLUMN);
    records.add(record);

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    return ImmutableSegmentLoader.load(new File(TEMP_DIR, SEGMENT_NAME), ReadMode.mmap);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _segmentWithNullValues.destroy();
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
