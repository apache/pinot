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
package org.apache.pinot.core.query.aggregation.groupby;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentContext;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/// Regression test for [NoDictionarySingleColumnGroupKeyGenerator] counting the null group.
///
/// For primitive stored types (INT/LONG/FLOAT/DOUBLE) with null handling enabled, the null group lives
/// *outside* the primitive key map, but it still takes the next dense group id. `getNumKeys()` and
/// `getCurrentGroupKeyUpperBound()` used to return only the map size, so once a null group was assigned the
/// reported upper bound equaled an already-issued group id. [DefaultGroupByExecutor#process] sizes the result
/// holders with `ensureCapacity(getCurrentGroupKeyUpperBound())`, so on a segment whose distinct-value count
/// exceeds the initial holder capacity the aggregation wrote one slot past the array —
/// an `ArrayIndexOutOfBoundsException` on the default on-heap path.
///
/// The fixture makes the failure deterministic: the null appears in the very first row (null group id 0), the
/// distinct non-null values exceed the (artificially low) `maxInitialResultHolderCapacity`, and everything fits
/// in one block, so the pre-fix under-count always leaves the highest group id out of the holder.
public class NoDictionaryNullGroupCountRegressionTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), "NoDictionaryNullGroupCountRegressionTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_COLUMN = "nInt";
  private static final String LONG_COLUMN = "nLong";
  private static final String FLOAT_COLUMN = "nFloat";
  private static final String DOUBLE_COLUMN = "nDouble";
  private static final String[] COLUMNS = {INT_COLUMN, LONG_COLUMN, FLOAT_COLUMN, DOUBLE_COLUMN};

  private static final int NUM_RECORDS = 200;
  // 24 distinct non-null values (pool indexes 1..24) plus the null group = 25 groups
  private static final int VALUE_POOL_SIZE = 25;
  private static final int NUM_GROUPS = VALUE_POOL_SIZE;
  // Far below the number of groups, so the holders must grow to exactly the reported upper bound
  private static final int MAX_INITIAL_RESULT_HOLDER_CAPACITY = 8;

  private IndexSegment _indexSegment;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(INT_COLUMN, DataType.INT)
        .addSingleValueDimension(LONG_COLUMN, DataType.LONG)
        .addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
        .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(List.of(COLUMNS)).build();

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      // Pool index 0 is the null value, and row 0 uses it, so the null group takes dense group id 0 and every
      // later distinct value pushes the maximum issued group id one past the (pre-fix) reported upper bound
      int poolIndex = i % VALUE_POOL_SIZE;
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, poolIndex == 0 ? null : poolIndex * 3 - 15);
      record.putValue(LONG_COLUMN, poolIndex == 0 ? null : poolIndex * 1_000_003L);
      record.putValue(FLOAT_COLUMN, poolIndex == 0 ? null : (poolIndex - 5) * 0.25f);
      record.putValue(DOUBLE_COLUMN, poolIndex == 0 ? null : (poolIndex - 5) * 0.5d);
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setDefaultNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();
    _indexSegment = ImmutableSegmentLoader.load(new File(TEMP_DIR, SEGMENT_NAME), ReadMode.mmap);
  }

  @DataProvider(name = "primitiveColumns")
  public Object[][] primitiveColumns() {
    Object[][] result = new Object[COLUMNS.length][];
    for (int i = 0; i < COLUMNS.length; i++) {
      result[i] = new Object[]{COLUMNS[i]};
    }
    return result;
  }

  @Test(dataProvider = "primitiveColumns")
  public void testNullGroupCountedInUpperBound(String column) {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable GROUP BY " + column);
    queryContext.setNullHandlingEnabled(true);
    queryContext.setMaxInitialResultHolderCapacity(MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    ExpressionContext[] groupByExpressions = {ExpressionContext.forIdentifier(column)};

    ProjectPlanNode projectPlanNode = new ProjectPlanNode(new SegmentContext(_indexSegment), queryContext,
        List.of(groupByExpressions), DocIdSetPlanNode.MAX_DOC_PER_CALL);
    BaseProjectOperator<?> projectOperator = projectPlanNode.run();
    DefaultGroupByExecutor groupByExecutor =
        new DefaultGroupByExecutor(queryContext, groupByExpressions, projectOperator);

    // Pre-fix this throws ArrayIndexOutOfBoundsException: the null group is assigned first (group id 0), the
    // 24 distinct values take ids 1..24, but the reported upper bound was 24 (the map size), so the result
    // holder never grew to cover group id 24
    ValueBlock valueBlock;
    while ((valueBlock = projectOperator.nextBlock()) != null) {
      groupByExecutor.process(valueBlock);
    }

    GroupKeyGenerator groupKeyGenerator = groupByExecutor.getGroupKeyGenerator();
    assertEquals(groupKeyGenerator.getNumKeys(), NUM_GROUPS, "getNumKeys() must count the out-of-map null group");
    assertEquals(groupKeyGenerator.getCurrentGroupKeyUpperBound(), NUM_GROUPS,
        "getCurrentGroupKeyUpperBound() must count the out-of-map null group");

    // The iterators emit the null group too, and every issued id stays below the reported upper bound
    int numKeys = 0;
    int numNullKeys = 0;
    int maxGroupId = -1;
    Iterator<GroupKeyGenerator.GroupKey> groupKeys = groupKeyGenerator.getGroupKeys();
    while (groupKeys.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = groupKeys.next();
      numKeys++;
      if (groupKey._keys[0] == null) {
        numNullKeys++;
      }
      maxGroupId = Math.max(maxGroupId, groupKey._groupId);
    }
    assertEquals(numKeys, NUM_GROUPS);
    assertEquals(numNullKeys, 1, "Exactly one null group expected");
    assertTrue(maxGroupId < groupKeyGenerator.getCurrentGroupKeyUpperBound(),
        "Issued group id " + maxGroupId + " must stay below the upper bound");
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
