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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
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
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class DictionaryBasedGroupKeyGeneratorTest {
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INDEX_DIR_PATH = FileUtils.getTempDirectoryPath() + File.separator + SEGMENT_NAME;
  private static final int NUM_ROWS = 1000;
  private static final int UNIQUE_ROWS = 100;
  private static final int MAX_STEP_LENGTH = 1000;
  private static final int MAX_NUM_MULTI_VALUES = 10;
  private static final int NUM_GROUPS = 20;
  private static final int[] SV_GROUP_KEY_BUFFER = new int[NUM_GROUPS];
  private static final int[][] MV_GROUP_KEY_BUFFER = new int[NUM_GROUPS][];
  private static final String FILTER_COLUMN = "docId";
  private static final String[] SV_COLUMNS = {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"};
  private static final String[] MV_COLUMNS = {"m1", "m2"};

  private final long _randomSeed = System.currentTimeMillis();
  private final Random _random = new Random(_randomSeed);
  private final String _errorMessage = "Random seed is: " + _randomSeed;

  private BaseProjectOperator<?> _projectOperator;
  private ValueBlock _valueBlock;

  @BeforeClass
  private void setup()
      throws Exception {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    int value = _random.nextInt(MAX_STEP_LENGTH);

    // Generate random values for the segment
    for (int i = 0; i < UNIQUE_ROWS; i++) {
      Map<String, Object> map = new HashMap<>();
      map.put(FILTER_COLUMN, i);
      for (String svColumn : SV_COLUMNS) {
        map.put(svColumn, value);
        value += 1 + _random.nextInt(MAX_STEP_LENGTH);
      }
      for (String mvColumn : MV_COLUMNS) {
        int numMultiValues = 1 + _random.nextInt(MAX_NUM_MULTI_VALUES);
        Integer[] values = new Integer[numMultiValues];
        for (int k = 0; k < numMultiValues; k++) {
          values[k] = value;
          value += 1 + _random.nextInt(MAX_STEP_LENGTH);
        }
        map.put(mvColumn, values);
      }
      GenericRow row = new GenericRow();
      row.init(map);
      rows.add(row);
    }
    for (int i = UNIQUE_ROWS; i < NUM_ROWS; i++) {
      rows.add(rows.get(i % UNIQUE_ROWS));
    }

    // Create an index segment with the random values
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(FILTER_COLUMN, FieldSpec.DataType.INT, true));
    for (String singleValueColumn : SV_COLUMNS) {
      schema.addField(new DimensionFieldSpec(singleValueColumn, FieldSpec.DataType.INT, true));
    }
    for (String multiValueColumn : MV_COLUMNS) {
      schema.addField(new DimensionFieldSpec(multiValueColumn, FieldSpec.DataType.INT, false));
    }

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR_PATH);
    config.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    IndexSegment indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR_PATH, SEGMENT_NAME), ReadMode.heap);

    // Generate a random query to filter out 2 unique rows
    int docId1 = _random.nextInt(50);
    int docId2 = docId1 + 1 + _random.nextInt(50);
    // NOTE: put all columns into group-by so that transform operator has expressions for all columns
    String query =
        String.format("SELECT COUNT(*) FROM testTable WHERE %s IN (%d, %d) GROUP BY %s, %s", FILTER_COLUMN, docId1,
            docId2, StringUtils.join(SV_COLUMNS, ", "), StringUtils.join(MV_COLUMNS, ", "));
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(query);

    List<ExpressionContext> expressions = new ArrayList<>();
    for (String column : SV_COLUMNS) {
      expressions.add(ExpressionContext.forIdentifier(column));
    }
    for (String column : MV_COLUMNS) {
      expressions.add(ExpressionContext.forIdentifier(column));
    }
    ProjectPlanNode projectPlanNode =
        new ProjectPlanNode(indexSegment, queryContext, expressions, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    _projectOperator = projectPlanNode.run();
    _valueBlock = _projectOperator.nextBlock();
  }

  @Test
  public void testArrayBasedSingleValue() {
    // Cardinality product (100) smaller than arrayBasedThreshold
    String[] groupByColumns = {"s1"};

    // Test initial status
    DictionaryBasedGroupKeyGenerator dictionaryBasedGroupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(_projectOperator, getExpressions(groupByColumns),
            InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT,
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    assertEquals(dictionaryBasedGroupKeyGenerator.getGlobalGroupKeyUpperBound(), UNIQUE_ROWS, _errorMessage);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), UNIQUE_ROWS, _errorMessage);

    // Test group key generation
    dictionaryBasedGroupKeyGenerator.generateKeysForBlock(_valueBlock, SV_GROUP_KEY_BUFFER);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), UNIQUE_ROWS, _errorMessage);
    compareSingleValueBuffer();
    testGetGroupKeys(dictionaryBasedGroupKeyGenerator.getGroupKeys(), 2);
  }

  @Test
  public void testIntMapBasedSingleValue() {
    // Cardinality product (1,000,000) larger than arrayBasedThreshold but smaller than Integer.MAX_VALUE
    String[] groupByColumns = {"s1", "s2", "s3"};

    // Test initial status
    DictionaryBasedGroupKeyGenerator dictionaryBasedGroupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(_projectOperator, getExpressions(groupByColumns),
            InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT,
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    assertEquals(dictionaryBasedGroupKeyGenerator.getGlobalGroupKeyUpperBound(),
        InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, _errorMessage);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 0, _errorMessage);

    // Test group key generation
    dictionaryBasedGroupKeyGenerator.generateKeysForBlock(_valueBlock, SV_GROUP_KEY_BUFFER);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 2, _errorMessage);
    compareSingleValueBuffer();
    testGetGroupKeys(dictionaryBasedGroupKeyGenerator.getGroupKeys(), 2);
  }

  @Test
  public void testLongMapBasedSingleValue() {
    // Cardinality product (10,000,000,000) larger than Integer.MAX_VALUE but smaller than LONG.MAX_VALUE
    String[] groupByColumns = {"s1", "s2", "s3", "s4", "s5"};

    // Test initial status
    DictionaryBasedGroupKeyGenerator dictionaryBasedGroupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(_projectOperator, getExpressions(groupByColumns),
            InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT,
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    assertEquals(dictionaryBasedGroupKeyGenerator.getGlobalGroupKeyUpperBound(),
        InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, _errorMessage);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 0, _errorMessage);

    // Test group key generation
    dictionaryBasedGroupKeyGenerator.generateKeysForBlock(_valueBlock, SV_GROUP_KEY_BUFFER);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 2, _errorMessage);
    compareSingleValueBuffer();
    testGetGroupKeys(dictionaryBasedGroupKeyGenerator.getGroupKeys(), 2);
  }

  @Test
  public void testArrayMapBasedSingleValue() {
    // Cardinality product larger than Long.MAX_VALUE
    String[] groupByColumns = {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"};

    // Test initial status
    DictionaryBasedGroupKeyGenerator dictionaryBasedGroupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(_projectOperator, getExpressions(groupByColumns),
            InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT,
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    assertEquals(dictionaryBasedGroupKeyGenerator.getGlobalGroupKeyUpperBound(),
        InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, _errorMessage);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 0, _errorMessage);

    // Test group key generation
    dictionaryBasedGroupKeyGenerator.generateKeysForBlock(_valueBlock, SV_GROUP_KEY_BUFFER);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 2, _errorMessage);
    compareSingleValueBuffer();
    testGetGroupKeys(dictionaryBasedGroupKeyGenerator.getGroupKeys(), 2);
  }

  /**
   * Helper method to compare the values inside the single value group key buffer.
   *
   * All odd number index values should be the same, all even number index values should be the same.
   * Odd number index values should be different from even number index values.
   */
  private void compareSingleValueBuffer() {
    assertTrue(SV_GROUP_KEY_BUFFER[0] != SV_GROUP_KEY_BUFFER[1], _errorMessage);
    for (int i = 2; i < NUM_GROUPS; i += 2) {
      assertEquals(SV_GROUP_KEY_BUFFER[i], SV_GROUP_KEY_BUFFER[0], _errorMessage);
      assertEquals(SV_GROUP_KEY_BUFFER[i + 1], SV_GROUP_KEY_BUFFER[1], _errorMessage);
    }
  }

  @Test
  public void testArrayBasedMultiValue() {
    // Cardinality product (100 - 1,000) smaller than arrayBasedThreshold
    String[] groupByColumns = {"m1"};

    // Test initial status
    DictionaryBasedGroupKeyGenerator dictionaryBasedGroupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(_projectOperator, getExpressions(groupByColumns),
            InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT,
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    int groupKeyUpperBound = dictionaryBasedGroupKeyGenerator.getGlobalGroupKeyUpperBound();
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), groupKeyUpperBound, _errorMessage);

    // Test group key generation
    dictionaryBasedGroupKeyGenerator.generateKeysForBlock(_valueBlock, MV_GROUP_KEY_BUFFER);
    int numUniqueKeys = MV_GROUP_KEY_BUFFER[0].length + MV_GROUP_KEY_BUFFER[1].length;
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), groupKeyUpperBound, _errorMessage);
    compareMultiValueBuffer();
    testGetGroupKeys(dictionaryBasedGroupKeyGenerator.getGroupKeys(), numUniqueKeys);
  }

  @Test
  public void tesIntMapBasedMultiValue() {
    // Cardinality product (1,000,000 - 100,000,000) larger than arrayBasedThreshold but smaller than Integer.MAX_VALUE
    String[] groupByColumns = {"m1", "m2", "s1"};

    // Test initial status
    DictionaryBasedGroupKeyGenerator dictionaryBasedGroupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(_projectOperator, getExpressions(groupByColumns),
            InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT,
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    assertEquals(dictionaryBasedGroupKeyGenerator.getGlobalGroupKeyUpperBound(),
        InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, _errorMessage);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 0, _errorMessage);

    // Test group key generation
    dictionaryBasedGroupKeyGenerator.generateKeysForBlock(_valueBlock, MV_GROUP_KEY_BUFFER);
    int numUniqueKeys = MV_GROUP_KEY_BUFFER[0].length + MV_GROUP_KEY_BUFFER[1].length;
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), numUniqueKeys, _errorMessage);
    compareMultiValueBuffer();
    testGetGroupKeys(dictionaryBasedGroupKeyGenerator.getGroupKeys(), numUniqueKeys);
  }

  @Test
  public void testLongMapBasedMultiValue() {
    // Cardinality product (10,000,000,000 - 1,000,000,000,000) larger than Integer.MAX_VALUE but smaller than LONG
    // .MAX_VALUE
    String[] groupByColumns = {"m1", "m2", "s1", "s2", "s3"};

    // Test initial status
    DictionaryBasedGroupKeyGenerator dictionaryBasedGroupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(_projectOperator, getExpressions(groupByColumns),
            InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT,
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    assertEquals(dictionaryBasedGroupKeyGenerator.getGlobalGroupKeyUpperBound(),
        InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, _errorMessage);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 0, _errorMessage);

    // Test group key generation
    dictionaryBasedGroupKeyGenerator.generateKeysForBlock(_valueBlock, MV_GROUP_KEY_BUFFER);
    int numUniqueKeys = MV_GROUP_KEY_BUFFER[0].length + MV_GROUP_KEY_BUFFER[1].length;
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), numUniqueKeys, _errorMessage);
    compareMultiValueBuffer();
    testGetGroupKeys(dictionaryBasedGroupKeyGenerator.getGroupKeys(), numUniqueKeys);
  }

  @Test
  public void testArrayMapBasedMultiValue() {
    // Cardinality product larger than Long.MAX_VALUE
    String[] groupByColumns = {"m1", "m2", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"};

    // Test initial status
    DictionaryBasedGroupKeyGenerator dictionaryBasedGroupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(_projectOperator, getExpressions(groupByColumns),
            InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT,
            InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
    assertEquals(dictionaryBasedGroupKeyGenerator.getGlobalGroupKeyUpperBound(),
        InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT, _errorMessage);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 0, _errorMessage);

    // Test group key generation
    dictionaryBasedGroupKeyGenerator.generateKeysForBlock(_valueBlock, MV_GROUP_KEY_BUFFER);
    int numUniqueKeys = MV_GROUP_KEY_BUFFER[0].length + MV_GROUP_KEY_BUFFER[1].length;
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), numUniqueKeys, _errorMessage);
    compareMultiValueBuffer();
    testGetGroupKeys(dictionaryBasedGroupKeyGenerator.getGroupKeys(), numUniqueKeys);
  }

  @Test
  public void testNumGroupsLimit() {
    String[] groupByColumns = {"m1", "m2"};
    int numGroupsLimit = 1;
    // NOTE: arrayBasedThreshold must be smaller or equal to numGroupsLimit
    DictionaryBasedGroupKeyGenerator dictionaryBasedGroupKeyGenerator =
        new DictionaryBasedGroupKeyGenerator(_projectOperator, getExpressions(groupByColumns), numGroupsLimit,
            numGroupsLimit);
    assertEquals(dictionaryBasedGroupKeyGenerator.getGlobalGroupKeyUpperBound(), numGroupsLimit, _errorMessage);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), 0, _errorMessage);

    // Test group key generation
    dictionaryBasedGroupKeyGenerator.generateKeysForBlock(_valueBlock, MV_GROUP_KEY_BUFFER);
    assertEquals(dictionaryBasedGroupKeyGenerator.getCurrentGroupKeyUpperBound(), numGroupsLimit, _errorMessage);
    // Only the first key should be 0, all others should be GroupKeyGenerator.INVALID_ID (-1)
    boolean firstKey = true;
    for (int groupKey : MV_GROUP_KEY_BUFFER[0]) {
      if (firstKey) {
        assertEquals(groupKey, 0);
        firstKey = false;
      } else {
        assertEquals(groupKey, GroupKeyGenerator.INVALID_ID);
      }
    }
    for (int groupKey : MV_GROUP_KEY_BUFFER[1]) {
      assertEquals(groupKey, GroupKeyGenerator.INVALID_ID);
    }
    for (int i = 2; i < NUM_GROUPS; i += 2) {
      assertEquals(MV_GROUP_KEY_BUFFER[i], MV_GROUP_KEY_BUFFER[0], _errorMessage);
      assertEquals(MV_GROUP_KEY_BUFFER[i + 1], MV_GROUP_KEY_BUFFER[1], _errorMessage);
    }
    testGetGroupKeys(dictionaryBasedGroupKeyGenerator.getGroupKeys(), numGroupsLimit);
  }

  private static ExpressionContext[] getExpressions(String[] columns) {
    int numColumns = columns.length;
    ExpressionContext[] expressions = new ExpressionContext[numColumns];
    for (int i = 0; i < numColumns; i++) {
      expressions[i] = ExpressionContext.forIdentifier(columns[i]);
    }
    return expressions;
  }

  /**
   * Helper method to compare the values inside the multi value group key buffer.
   *
   * All odd number index values should be the same, all even number index values should be the same.
   * Odd number index values should be different from even number index values.
   */
  private void compareMultiValueBuffer() {
    assertFalse(Arrays.equals(MV_GROUP_KEY_BUFFER[0], MV_GROUP_KEY_BUFFER[1]), _errorMessage);
    for (int i = 2; i < NUM_GROUPS; i += 2) {
      assertEquals(MV_GROUP_KEY_BUFFER[i], MV_GROUP_KEY_BUFFER[0], _errorMessage);
      assertEquals(MV_GROUP_KEY_BUFFER[i + 1], MV_GROUP_KEY_BUFFER[1], _errorMessage);
    }
  }

  /**
   * Helper method to test the group key iterator returned by getGroupKeys().
   *
   * @param groupKeyIterator group key iterator.
   * @param numUniqueKeys number of unique keys.
   */
  private void testGetGroupKeys(Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator, int numUniqueKeys) {
    int count = 0;
    Set<Integer> idSet = new HashSet<>();
    Set<List<Object>> groupKeySet = new HashSet<>();

    while (groupKeyIterator.hasNext()) {
      count++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      idSet.add(groupKey._groupId);
      groupKeySet.add(Arrays.asList(groupKey._keys));
    }

    assertEquals(count, numUniqueKeys, _errorMessage);
    assertEquals(idSet.size(), numUniqueKeys, _errorMessage);
    assertEquals(groupKeySet.size(), numUniqueKeys, _errorMessage);
  }

  @Test
  public void testMapDefaultValue() {
    assertEquals(DictionaryBasedGroupKeyGenerator.THREAD_LOCAL_LONG_MAP.get().defaultReturnValue(),
        GroupKeyGenerator.INVALID_ID);
    assertEquals(DictionaryBasedGroupKeyGenerator.THREAD_LOCAL_INT_ARRAY_MAP.get().defaultReturnValue(),
        GroupKeyGenerator.INVALID_ID);
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));
  }
}
