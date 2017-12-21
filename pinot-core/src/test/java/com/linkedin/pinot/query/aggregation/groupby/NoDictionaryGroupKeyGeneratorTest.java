/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.operator.transform.TransformExpressionOperator;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.query.aggregation.groupby.AggregationGroupByTrimmingService;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import com.linkedin.pinot.core.query.aggregation.groupby.NoDictionaryMultiColumnGroupKeyGenerator;
import com.linkedin.pinot.core.query.aggregation.groupby.NoDictionarySingleColumnGroupKeyGenerator;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
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
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link NoDictionaryMultiColumnGroupKeyGenerator}
 */
public class NoDictionaryGroupKeyGeneratorTest {
  private static final String SEGMENT_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "rawIndexPerf";
  private static final String SEGMENT_NAME = "perfTestSegment";

  private static final String STRING_DICT_COLUMN = "string_dict_column";
  private static final String[] COLUMN_NAMES =
      {"int_column", "long_column", "float_column", "double_column", "string_column", STRING_DICT_COLUMN};
  private static final String[] NO_DICT_COLUMN_NAMES =
      {"int_column", "long_column", "float_column", "double_column", "string_column"};

  private static final FieldSpec.DataType[] DATA_TYPES =
      {FieldSpec.DataType.INT, FieldSpec.DataType.LONG, FieldSpec.DataType.FLOAT, FieldSpec.DataType.DOUBLE, FieldSpec.DataType.STRING, FieldSpec.DataType.STRING};

  private static final int NUM_COLUMNS = DATA_TYPES.length;
  private static final int NUM_ROWS = 1;
  private RecordReader _recordReader;
  private Map<String, BaseOperator> _dataSourceMap;
  private IndexSegment _indexSegment;

  @BeforeClass
  public void setup()
      throws Exception {
    _recordReader = buildSegment();

    // Load the segment.
    File segment = new File(SEGMENT_DIR_NAME, SEGMENT_NAME);
    _indexSegment = Loaders.IndexSegment.load(segment, ReadMode.heap);

    // Build the data source map
    _dataSourceMap = new HashMap<>();
    for (String column : _indexSegment.getColumnNames()) {
      DataSource dataSource = _indexSegment.getDataSource(column);
      _dataSourceMap.put(column, dataSource);
    }
  }

  /**
   * Unit test for {@link com.linkedin.pinot.core.query.aggregation.groupby.NoDictionarySingleColumnGroupKeyGenerator}
   * @throws Exception
   */
  @Test
  public void testSingleColumnGroupKeyGenerator()
      throws Exception {
    for (int i = 0; i < COLUMN_NAMES.length; i++) {
      testGroupKeyGenerator(new String[]{COLUMN_NAMES[i]}, new FieldSpec.DataType[]{DATA_TYPES[i]});
    }
  }

  /**
   * Unit test for {@link NoDictionaryMultiColumnGroupKeyGenerator}
   * @throws Exception
   */
  @Test
  public void testMultiColumnGroupKeyGenerator()
      throws Exception {
    testGroupKeyGenerator(_indexSegment.getColumnNames(), DATA_TYPES);
  }

  /**
   * Tests multi-column group key generator when at least one column as dictionary, and others don't.
   */
  @Test
  public void testMultiColumnHybridGroupKeyGenerator()
      throws Exception {
    for (int i = 0; i < NO_DICT_COLUMN_NAMES.length; i++) {
      testGroupKeyGenerator(new String[]{NO_DICT_COLUMN_NAMES[i], STRING_DICT_COLUMN},
          new FieldSpec.DataType[]{DATA_TYPES[i], FieldSpec.DataType.STRING});
    }
  }

  private void testGroupKeyGenerator(String[] groupByColumns, FieldSpec.DataType[] dataTypes)
      throws Exception {
    // Build the projection operator.
    MatchEntireSegmentOperator matchEntireSegmentOperator = new MatchEntireSegmentOperator(NUM_ROWS);
    BReusableFilteredDocIdSetOperator docIdSetOperator =
        new BReusableFilteredDocIdSetOperator(matchEntireSegmentOperator, NUM_ROWS, 10000);
    MProjectionOperator projectionOperator = new MProjectionOperator(_dataSourceMap, docIdSetOperator);
    TransformExpressionOperator transformOperator =
        new TransformExpressionOperator(projectionOperator, new ArrayList<TransformExpressionTree>());

    // Iterator over all projection blocks and generate group keys.
    TransformBlock transformBlock;
    int[] docIdToGroupKeys = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];

    GroupKeyGenerator groupKeyGenerator = null;
    while ((transformBlock = (TransformBlock) transformOperator.nextBlock()) != null) {
      if (groupKeyGenerator == null) {
        // Build the group key generator.
        groupKeyGenerator =
            (groupByColumns.length == 1) ? new NoDictionarySingleColumnGroupKeyGenerator(groupByColumns[0],
                dataTypes[0]) : new NoDictionaryMultiColumnGroupKeyGenerator(transformBlock, groupByColumns);
      }
      groupKeyGenerator.generateKeysForBlock(transformBlock, docIdToGroupKeys);
    }

    // Assert total number of group keys is as expected
    Assert.assertTrue(groupKeyGenerator != null);
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
  private Set<String> getExpectedGroupKeys(RecordReader recordReader, String[] groupByColumns)
      throws Exception {
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
  private RecordReader buildSegment()
      throws Exception {
    Schema schema = new Schema();

    for (int i = 0; i < COLUMN_NAMES.length; i++) {
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(COLUMN_NAMES[i], DATA_TYPES[i], true);
      schema.addField(dimensionFieldSpec);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setRawIndexCreationColumns(Arrays.asList(NO_DICT_COLUMN_NAMES));

    config.setOutDir(SEGMENT_DIR_NAME);
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
