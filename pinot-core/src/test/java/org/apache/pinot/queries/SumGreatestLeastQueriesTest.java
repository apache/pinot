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
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.query.AggregationGroupByOrderByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
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
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;


public class SumGreatestLeastQueriesTest extends BaseQueriesTest {

  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SumGreatestLeastQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 1000;
  private static final int BUCKET_SIZE = 8;
  private static final String CLASSIFICATION_COLUMN = "class";
  private static final String ON_COLUMN = "onCol";
  private static final String OFF_COLUMN = "offCol";

  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .addSingleValueDimension(ON_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(OFF_COLUMN, FieldSpec.DataType.INT)
      .addSingleValueDimension(CLASSIFICATION_COLUMN, FieldSpec.DataType.INT)
      .build();

  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

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

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(ON_COLUMN, 1);
      record.putValue(OFF_COLUMN, 0);
      record.putValue(CLASSIFICATION_COLUMN, i % BUCKET_SIZE);
      records.add(record);
    }

    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(records));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @Test
  public void testSum() {
    String query = "select sum(" + ON_COLUMN + "), "
        + "sum(" + OFF_COLUMN + "), "
        + "sum(greatest(" + ON_COLUMN + ", " + OFF_COLUMN + ")), "
        + "sum(least(" + ON_COLUMN + ", " + OFF_COLUMN + ")) "
        + "from " + RAW_TABLE_NAME;
    AggregationOperator aggregationOperator = getOperator(query);
    List<Object> aggregationResult = aggregationOperator.nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 4);
    assertEquals(((Number) aggregationResult.get(0)).intValue(), NUM_RECORDS);
    assertEquals(((Number) aggregationResult.get(1)).intValue(), 0);
    assertEquals(((Number) aggregationResult.get(2)).intValue(), NUM_RECORDS);
    assertEquals(((Number) aggregationResult.get(3)).intValue(), 0);
  }

  @Test
  public void testSumGroupBy() {
    String query = "select sum(" + ON_COLUMN + "), "
        + "sum(" + OFF_COLUMN + "), "
        + "sum(greatest(" + ON_COLUMN + ", " + OFF_COLUMN + ")), "
        + "sum(least(" + ON_COLUMN + ", " + OFF_COLUMN + ")), "
        + "sum(greatest(" + OFF_COLUMN + ", " + ON_COLUMN + ")), "
        + "sum(least(" + OFF_COLUMN + ", " + ON_COLUMN + ")) "
        + "from " + RAW_TABLE_NAME + " "
        + "group by " + CLASSIFICATION_COLUMN;
    AggregationGroupByOrderByOperator groupByOperator = getOperator(query);
    AggregationGroupByResult result = groupByOperator.nextBlock().getAggregationGroupByResult();
    assertNotNull(result);
    Iterator<GroupKeyGenerator.GroupKey> it = result.getGroupKeyIterator();
    while (it.hasNext()) {
      GroupKeyGenerator.GroupKey groupKey = it.next();
      for (int j = 0; j < 6; j++) {
        Object aggregate = result.getResultForGroupId(j, groupKey._groupId);
        assertEquals(((Number) aggregate).intValue(), j % 2 == 0 ? NUM_RECORDS / BUCKET_SIZE : 0);
      }
    }
  }
}
