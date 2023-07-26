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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.plan.ProjectPlanNode;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
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
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Unit test for {@link NoDictionaryMultiColumnGroupKeyGenerator}
 */
public class NoDictionaryGroupKeyGeneratorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "NoDictionaryGroupKeyGeneratorTest");
  private static final Random RANDOM = new Random();

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String BYTES_COLUMN = "bytesColumn";
  private static final String BYTES_DICT_COLUMN = "bytesDictColumn";
  private static final List<String> COLUMNS =
      Arrays.asList(INT_COLUMN, LONG_COLUMN, FLOAT_COLUMN, DOUBLE_COLUMN, STRING_COLUMN, BYTES_COLUMN,
          BYTES_DICT_COLUMN);
  private static final int NUM_COLUMNS = COLUMNS.size();
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(COLUMNS.subList(0, NUM_COLUMNS - 1)).build();
  private static final Schema SCHEMA =
      new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, FieldSpec.DataType.INT)
          .addSingleValueDimension(LONG_COLUMN, FieldSpec.DataType.LONG)
          .addSingleValueDimension(FLOAT_COLUMN, FieldSpec.DataType.FLOAT)
          .addSingleValueDimension(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE)
          .addSingleValueDimension(STRING_COLUMN, FieldSpec.DataType.STRING)
          .addSingleValueDimension(BYTES_COLUMN, FieldSpec.DataType.BYTES)
          .addSingleValueDimension(BYTES_DICT_COLUMN, FieldSpec.DataType.BYTES).build();

  private static final int NUM_RECORDS = 1000;
  private static final int NUM_UNIQUE_RECORDS = 100;

  private final String[][] _stringValues = new String[NUM_UNIQUE_RECORDS][NUM_COLUMNS];
  private IndexSegment _indexSegment;
  private BaseProjectOperator<?> _projectOperator;
  private ValueBlock _valueBlock;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteDirectory(TEMP_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_UNIQUE_RECORDS; i++) {
      GenericRow record = new GenericRow();
      String[] values = _stringValues[i];
      int intValue = RANDOM.nextInt();
      record.putValue(INT_COLUMN, intValue);
      values[0] = Integer.toString(intValue);
      long longValue = RANDOM.nextLong();
      record.putValue(LONG_COLUMN, longValue);
      values[1] = Long.toString(longValue);
      float floatValue = RANDOM.nextFloat();
      record.putValue(FLOAT_COLUMN, floatValue);
      values[2] = Float.toString(floatValue);
      double doubleValue = RANDOM.nextDouble();
      record.putValue(DOUBLE_COLUMN, doubleValue);
      values[3] = Double.toString(doubleValue);
      String stringValue = RandomStringUtils.randomAlphabetic(10);
      record.putValue(STRING_COLUMN, stringValue);
      values[4] = stringValue;
      // NOTE: Create fixed-length bytes so that dictionary can be generated.
      byte[] bytesValue = new byte[10];
      RANDOM.nextBytes(bytesValue);
      record.putValue(BYTES_COLUMN, bytesValue);
      record.putValue(BYTES_DICT_COLUMN, bytesValue);
      values[5] = BytesUtils.toHexString(bytesValue);
      values[6] = values[5];
      for (int j = 0; j < NUM_RECORDS / NUM_UNIQUE_RECORDS; j++) {
        records.add(record);
      }
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(TEMP_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    _indexSegment = ImmutableSegmentLoader.load(new File(TEMP_DIR, SEGMENT_NAME), ReadMode.mmap);

    // Create transform operator and block
    // NOTE: put all columns into group-by so that transform operator has expressions for all columns
    String query = "SELECT COUNT(*) FROM testTable GROUP BY " + StringUtils.join(COLUMNS, ", ");
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);
    List<ExpressionContext> expressions = new ArrayList<>();
    for (String column : COLUMNS) {
      expressions.add(ExpressionContext.forIdentifier(column));
    }
    ProjectPlanNode projectPlanNode =
        new ProjectPlanNode(_indexSegment, queryContext, expressions, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    _projectOperator = projectPlanNode.run();
    _valueBlock = _projectOperator.nextBlock();
  }

  /**
   * Unit test for {@link NoDictionarySingleColumnGroupKeyGenerator}
   */
  @Test
  public void testSingleColumnGroupKeyGenerator() {
    for (int i = 0; i < NUM_COLUMNS - 1; i++) {
      testGroupKeyGenerator(new int[]{i});
    }
  }

  /**
   * Unit test for {@link NoDictionaryMultiColumnGroupKeyGenerator}
   */
  @Test
  public void testMultiColumnGroupKeyGenerator() {
    testGroupKeyGenerator(new int[]{0, 1});
    testGroupKeyGenerator(new int[]{2, 3});
    testGroupKeyGenerator(new int[]{4, 5});
    testGroupKeyGenerator(new int[]{1, 2, 3});
    testGroupKeyGenerator(new int[]{4, 5, 0});
    testGroupKeyGenerator(new int[]{5, 4, 3, 2, 1, 0});
  }

  /**
   * Tests multi-column group key generator when at least one column as dictionary, and others don't.
   */
  @Test
  public void testMultiColumnHybridGroupKeyGenerator() {
    for (int i = 0; i < NUM_COLUMNS - 1; i++) {
      testGroupKeyGenerator(new int[]{i, NUM_COLUMNS - 1});
    }
  }

  private void testGroupKeyGenerator(int[] groupByColumnIndexes) {
    int numGroupByColumns = groupByColumnIndexes.length;
    GroupKeyGenerator groupKeyGenerator;
    if (numGroupByColumns == 1) {
      groupKeyGenerator = new NoDictionarySingleColumnGroupKeyGenerator(_projectOperator,
          ExpressionContext.forIdentifier(COLUMNS.get(groupByColumnIndexes[0])),
          InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, false);
    } else {
      ExpressionContext[] groupByExpressions = new ExpressionContext[numGroupByColumns];
      for (int i = 0; i < numGroupByColumns; i++) {
        groupByExpressions[i] = ExpressionContext.forIdentifier(COLUMNS.get(groupByColumnIndexes[i]));
      }
      groupKeyGenerator = new NoDictionaryMultiColumnGroupKeyGenerator(_projectOperator, groupByExpressions,
          InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, false);
    }
    groupKeyGenerator.generateKeysForBlock(_valueBlock, new int[NUM_RECORDS]);

    // Assert total number of group keys is as expected
    Set<String> expectedGroupKeys = getExpectedGroupKeys(groupByColumnIndexes);
    assertEquals(groupKeyGenerator.getCurrentGroupKeyUpperBound(), expectedGroupKeys.size(),
        "Number of group keys mis-match.");

    // Assert all group key values are as expected
    Iterator<GroupKeyGenerator.GroupKey> groupKeys = groupKeyGenerator.getGroupKeys();
    while (groupKeys.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = groupKeys.next();
      assertTrue(expectedGroupKeys.contains(getActualGroupKey(groupKey._keys)));
    }
  }

  private Set<String> getExpectedGroupKeys(int[] groupByColumnIndexes) {
    int numGroupByColumns = groupByColumnIndexes.length;
    Set<String> groupKeys = new HashSet<>();
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < NUM_UNIQUE_RECORDS; i++) {
      stringBuilder.setLength(0);
      String[] values = _stringValues[i];
      for (int j = 0; j < numGroupByColumns; j++) {
        if (j > 0) {
          stringBuilder.append(GroupKeyGenerator.DELIMITER);
        }
        stringBuilder.append(values[groupByColumnIndexes[j]]);
      }
      groupKeys.add(stringBuilder.toString());
    }
    return groupKeys;
  }

  private String getActualGroupKey(Object[] groupKeys) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int i = 0; i < groupKeys.length; i++) {
      if (i > 0) {
        stringBuilder.append(GroupKeyGenerator.DELIMITER);
      }
      stringBuilder.append(groupKeys[i]);
    }
    return stringBuilder.toString();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
