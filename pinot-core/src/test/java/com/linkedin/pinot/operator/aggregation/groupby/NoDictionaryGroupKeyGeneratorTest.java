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
package com.linkedin.pinot.operator.aggregation.groupby;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.TestRecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.BReusableFilteredDocIdSetOperator;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.aggregation.groupby.AggregationGroupByTrimmingService;
import com.linkedin.pinot.core.operator.aggregation.groupby.GroupKeyGenerator;
import com.linkedin.pinot.core.operator.aggregation.groupby.NoDictionaryGroupKeyGenerator;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.filter.MatchEntireSegmentOperator;
import com.linkedin.pinot.core.plan.DocIdSetPlanNode;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link NoDictionaryGroupKeyGenerator}
 */
public class NoDictionaryGroupKeyGeneratorTest {
  private static final String SEGMENT_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "rawIndexPerf";
  private static final String SEGMENT_NAME = "perfTestSegment";
  private static final int NUM_COLUMNS = 2;
  private static final int NUM_ROWS = 10000;

  /**
   * This test builds a segment with two columns, one without dictionary, and the other with dictionary.
   * It then uses {@link NoDictionaryGroupKeyGenerator} to ensure that all group keys are generated correctly.
   *
   * @throws Exception
   */
  @Test
  public void test()
      throws Exception {
    Set<String> expectedGroupKeys = buildSegment();

    // Load the segment.
    File segment = new File(SEGMENT_DIR_NAME, SEGMENT_NAME);
    IndexSegment indexSegment = Loaders.IndexSegment.load(segment, ReadMode.heap);

    // Build the group key generator.
    GroupKeyGenerator groupKeyGenerator = new NoDictionaryGroupKeyGenerator(indexSegment.getColumnNames());

    // Build the data source map
    Map<String, BaseOperator> dataSourceMap = new HashMap<>();
    for (String column : indexSegment.getColumnNames()) {
      DataSource dataSource = indexSegment.getDataSource(column);
      dataSourceMap.put(column, dataSource);
    }

    // Build the projection operator.
    MatchEntireSegmentOperator matchEntireSegmentOperator = new MatchEntireSegmentOperator(NUM_ROWS);
    BReusableFilteredDocIdSetOperator docIdSetOperator =
        new BReusableFilteredDocIdSetOperator(matchEntireSegmentOperator, NUM_ROWS, 10000);
    MProjectionOperator projectionOperator = new MProjectionOperator(dataSourceMap, docIdSetOperator);

    // Iterator over all projection blocks and generate group keys.
    ProjectionBlock projectionBlock;
    int[] docIdToGroupKeys = new int[DocIdSetPlanNode.MAX_DOC_PER_CALL];

    while ((projectionBlock = (ProjectionBlock) projectionOperator.nextBlock()) != null) {
      groupKeyGenerator.generateKeysForBlock(projectionBlock, docIdToGroupKeys);
    }

    // Assert total number of group keys is as expected
    Assert.assertEquals(groupKeyGenerator.getCurrentGroupKeyUpperBound(), expectedGroupKeys.size(),
        "Number of group keys mis-match.");

    // Assert all group key values are as expected
    Iterator<GroupKeyGenerator.GroupKey> uniqueGroupKeys = groupKeyGenerator.getUniqueGroupKeys();
    while (uniqueGroupKeys.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = uniqueGroupKeys.next();
      String actual = groupKey.getStringKey();
      Assert.assertTrue(expectedGroupKeys.contains(actual), "Unexpected group key: " + actual);
    }
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
  private Set<String> buildSegment()
      throws Exception {
    Schema schema = new Schema();

    for (int i = 0; i < NUM_COLUMNS; i++) {
      String column = "column_" + i;
      FieldSpec.DataType dataType = ((i & 0x1) == 1) ? FieldSpec.DataType.STRING : FieldSpec.DataType.INT;
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(column, dataType, true);
      schema.addField(dimensionFieldSpec);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setRawIndexCreationColumns(Collections.singletonList("column_1"));

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    Set<String> keys = new HashSet<>(NUM_ROWS);
    StringBuilder stringBuilder = new StringBuilder();

    for (int i = 0; i < NUM_ROWS; i++) {
      Map<String, Object> map = new HashMap<>(NUM_COLUMNS);

      int j = 0;
      stringBuilder.setLength(0);
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        String column = fieldSpec.getName();

        if (fieldSpec.getDataType() == FieldSpec.DataType.STRING) {
          map.put(column, "value_" + i);
        } else {
          map.put(column, NUM_ROWS - i - 1);
        }
        stringBuilder.append(map.get(column));
        if (j++ < NUM_COLUMNS - 1) {
          stringBuilder.append(AggregationGroupByTrimmingService.GROUP_KEY_DELIMITER);
        }
      }
      keys.add(stringBuilder.toString());

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    RecordReader recordReader = new TestRecordReader(rows, schema);

    driver.init(config, recordReader);
    driver.build();

    return keys;
  }
}
