/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.query.aggregation.groupby;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.operator.transform.TransformOperator;
import com.linkedin.pinot.core.plan.TransformPlanNode;
import com.linkedin.pinot.core.plan.maker.InstancePlanMakerImplV2;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import com.linkedin.pinot.core.query.aggregation.groupby.NoDictionaryMultiColumnGroupKeyGenerator;
import com.linkedin.pinot.core.query.aggregation.groupby.NoDictionarySingleColumnGroupKeyGenerator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
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
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link NoDictionaryMultiColumnGroupKeyGenerator}
 */
public class NoDictionaryGroupKeyGeneratorTest {
  private static final String SEGMENT_NAME = "testSegment";
  private static final String INDEX_DIR_PATH = FileUtils.getTempDirectoryPath() + File.separator + SEGMENT_NAME;

  private static final String STRING_DICT_COLUMN = "string_dict_column";
  private static final String[] COLUMN_NAMES =
      {"int_column", "long_column", "float_column", "double_column", "string_column", STRING_DICT_COLUMN};
  private static final String[] NO_DICT_COLUMN_NAMES =
      {"int_column", "long_column", "float_column", "double_column", "string_column"};
  private static final FieldSpec.DataType[] DATA_TYPES =
      {FieldSpec.DataType.INT, FieldSpec.DataType.LONG, FieldSpec.DataType.FLOAT, FieldSpec.DataType.DOUBLE,
          FieldSpec.DataType.STRING, FieldSpec.DataType.STRING};
  private static final int NUM_COLUMNS = COLUMN_NAMES.length;
  private static final int NUM_ROWS = 1000;

  private RecordReader _recordReader;
  private TransformOperator _transformOperator;
  private TransformBlock _transformBlock;

  @BeforeClass
  public void setup() throws Exception {
    FileUtils.deleteQuietly(new File(INDEX_DIR_PATH));

    _recordReader = buildSegment();

    // Load the segment.
    IndexSegment indexSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR_PATH, SEGMENT_NAME), ReadMode.heap);

    // Create transform operator and block
    // NOTE: put all columns into group-by so that transform operator has expressions for all columns
    String query = String.format("SELECT COUNT(*) FROM table GROUP BY %s", StringUtils.join(COLUMN_NAMES, ", "));
    TransformPlanNode transformPlanNode =
        new TransformPlanNode(indexSegment, new Pql2Compiler().compileToBrokerRequest(query));
    _transformOperator = transformPlanNode.run();
    _transformBlock = _transformOperator.nextBlock();
  }

  /**
   * Unit test for {@link com.linkedin.pinot.core.query.aggregation.groupby.NoDictionarySingleColumnGroupKeyGenerator}
   * @throws Exception
   */
  @Test
  public void testSingleColumnGroupKeyGenerator() throws Exception {
    for (String column : COLUMN_NAMES) {
      testGroupKeyGenerator(new String[]{column});
    }
  }

  /**
   * Unit test for {@link NoDictionaryMultiColumnGroupKeyGenerator}
   * @throws Exception
   */
  @Test
  public void testMultiColumnGroupKeyGenerator() throws Exception {
    testGroupKeyGenerator(COLUMN_NAMES);
  }

  /**
   * Tests multi-column group key generator when at least one column as dictionary, and others don't.
   */
  @Test
  public void testMultiColumnHybridGroupKeyGenerator() throws Exception {
    for (String noDictColumn : NO_DICT_COLUMN_NAMES) {
      testGroupKeyGenerator(new String[]{noDictColumn, STRING_DICT_COLUMN});
    }
  }

  private void testGroupKeyGenerator(String[] groupByColumns) throws Exception {
    int numGroupByColumns = groupByColumns.length;
    TransformExpressionTree[] groupByExpressions = new TransformExpressionTree[numGroupByColumns];
    for (int i = 0; i < numGroupByColumns; i++) {
      groupByExpressions[i] = TransformExpressionTree.compileToExpressionTree(groupByColumns[i]);
    }

    GroupKeyGenerator groupKeyGenerator;
    if (numGroupByColumns == 1) {
      groupKeyGenerator = new NoDictionarySingleColumnGroupKeyGenerator(_transformOperator, groupByExpressions[0],
          InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT);
    } else {
      groupKeyGenerator = new NoDictionaryMultiColumnGroupKeyGenerator(_transformOperator, groupByExpressions,
          InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT);
    }
    groupKeyGenerator.generateKeysForBlock(_transformBlock, new int[NUM_ROWS]);

    // Assert total number of group keys is as expected
    Set<String> expectedGroupKeys = getExpectedGroupKeys(_recordReader, groupByColumns);
    Assert.assertEquals(groupKeyGenerator.getCurrentGroupKeyUpperBound(), expectedGroupKeys.size(),
        "Number of group keys mis-match.");

    // Assert all group key values are as expected
    Iterator<GroupKeyGenerator.GroupKey> uniqueGroupKeys = groupKeyGenerator.getUniqueGroupKeys();
    while (uniqueGroupKeys.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = uniqueGroupKeys.next();
      String actual = groupKey._stringKey;
      Assert.assertTrue(expectedGroupKeys.contains(actual), "Unexpected group key: " + actual);
    }
  }

  /**
   * Helper method to build group keys for a given array of group-by columns.
   *
   * @param groupByColumns Group-by columns for which to generate the group-keys.
   * @return Set of unique group keys.
   * @throws Exception
   */
  private Set<String> getExpectedGroupKeys(RecordReader recordReader, String[] groupByColumns) throws Exception {
    Set<String> groupKeys = new HashSet<>();
    StringBuilder stringBuilder = new StringBuilder();

    recordReader.rewind();
    while (recordReader.hasNext()) {
      GenericRow row = recordReader.next();

      stringBuilder.setLength(0);
      for (int i = 0; i < groupByColumns.length; i++) {
        stringBuilder.append(row.getValue(groupByColumns[i]));
        if (i < groupByColumns.length - 1) {
          stringBuilder.append(AggregationGroupByTrimmingService.GROUP_KEY_DELIMITER);
        }
      }
      groupKeys.add(stringBuilder.toString());
    }
    return groupKeys;
  }

  /**
   * Helper method to build a segment as follows:
   * <ul>
   *   <li> One string column without dictionary. </li>
   *   <li> One integer column with dictionary. </li>
   * </ul>
   *
   * It also computes the unique group keys while it generates the index.
   *
   * @return Set containing unique group keys from the created segment.
   *
   * @throws Exception
   */
  private static RecordReader buildSegment() throws Exception {
    Schema schema = new Schema();

    for (int i = 0; i < COLUMN_NAMES.length; i++) {
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(COLUMN_NAMES[i], DATA_TYPES[i], true);
      schema.addField(dimensionFieldSpec);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setRawIndexCreationColumns(Arrays.asList(NO_DICT_COLUMN_NAMES));

    config.setOutDir(INDEX_DIR_PATH);
    config.setSegmentName(SEGMENT_NAME);

    Random random = new Random();
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>(NUM_COLUMNS);

      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        String column = fieldSpec.getName();

        FieldSpec.DataType dataType = fieldSpec.getDataType();
        switch (dataType) {
          case INT:
            map.put(column, random.nextInt());
            break;

          case LONG:
            map.put(column, random.nextLong());
            break;

          case FLOAT:
            map.put(column, random.nextFloat());
            break;

          case DOUBLE:
            map.put(column, random.nextDouble());
            break;

          case STRING:
            map.put(column, "value_" + i);
            break;

          default:
            throw new IllegalArgumentException("Illegal data type specified: " + dataType);
        }
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    RecordReader recordReader = new GenericRowRecordReader(rows, schema);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();

    return recordReader;
  }
}
