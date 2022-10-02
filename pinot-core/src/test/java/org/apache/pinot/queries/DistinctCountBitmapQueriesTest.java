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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for DISTINCT_COUNT_BITMAP queries.
 */
@SuppressWarnings("unchecked")
public class DistinctCountBitmapQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DistinctCountBitmapQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_RECORDS = 2000;
  private static final int MAX_VALUE = 1000;

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String STRING_COLUMN = "stringColumn";
  private static final String BYTES_COLUMN = "bytesColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE).addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();

  private Set<Integer> _values;
  private int[] _expectedResults;
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
    FileUtils.deleteDirectory(INDEX_DIR);

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    int hashMapCapacity = HashUtil.getHashMapCapacity(MAX_VALUE);
    _values = new HashSet<>(hashMapCapacity);
    Set<Integer> longResultSet = new HashSet<>(hashMapCapacity);
    Set<Integer> floatResultSet = new HashSet<>(hashMapCapacity);
    Set<Integer> doubleResultSet = new HashSet<>(hashMapCapacity);
    Set<Integer> stringResultSet = new HashSet<>(hashMapCapacity);
    for (int i = 0; i < NUM_RECORDS; i++) {
      int value = RANDOM.nextInt(MAX_VALUE);
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, value);
      _values.add(Integer.hashCode(value));
      record.putValue(LONG_COLUMN, (long) value);
      longResultSet.add(Long.hashCode(value));
      record.putValue(FLOAT_COLUMN, (float) value);
      floatResultSet.add(Float.hashCode(value));
      record.putValue(DOUBLE_COLUMN, (double) value);
      doubleResultSet.add(Double.hashCode(value));
      String stringValue = Integer.toString(value);
      record.putValue(STRING_COLUMN, stringValue);
      stringResultSet.add(stringValue.hashCode());
      // Store serialized bitmaps in the BYTES column
      RoaringBitmap bitmap = new RoaringBitmap();
      bitmap.add(value);
      byte[] bytesValue = RoaringBitmapUtils.serialize(bitmap);
      record.putValue(BYTES_COLUMN, bytesValue);
      records.add(record);
    }
    _expectedResults = new int[]{
        _values.size(), longResultSet.size(), floatResultSet.size(), doubleResultSet.size(), stringResultSet.size(),
        _values.size()
    };

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
  public void testAggregationOnly() {
    String query =
        "SELECT DISTINCTCOUNTBITMAP(intColumn), DISTINCTCOUNTBITMAP(longColumn), DISTINCTCOUNTBITMAP(floatColumn), "
            + "DISTINCTCOUNTBITMAP(doubleColumn), DISTINCTCOUNTBITMAP(stringColumn), DISTINCTCOUNTBITMAP(bytesColumn)"
            + " FROM testTable";

    // Inner segment
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        6 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    for (int i = 0; i < 6; i++) {
      assertEquals(((RoaringBitmap) aggregationResult.get(i)).getCardinality(), _expectedResults[i]);
    }

    // Inter segments
    Object[] expectedResults = Arrays.stream(_expectedResults).boxed().toArray();
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query), 4 * NUM_RECORDS, 0, 4 * 6 * NUM_RECORDS,
        4 * NUM_RECORDS, expectedResults);
  }

  @Test
  public void testAggregationGroupBy() {
    String query = "SELECT DISTINCTCOUNT(intColumn), DISTINCTCOUNT(longColumn), DISTINCTCOUNT(floatColumn), "
        + "DISTINCTCOUNT(doubleColumn), DISTINCTCOUNT(stringColumn), DISTINCTCOUNT(bytesColumn) "
        + "FROM testTable GROUP BY intColumn";

    // Inner segment
    GroupByOperator groupByOperator = getOperator(query);
    GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        6 * NUM_RECORDS, NUM_RECORDS);
    AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
    assertNotNull(aggregationGroupByResult);
    int numGroups = 0;
    Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
    while (groupKeyIterator.hasNext()) {
      numGroups++;
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      Integer key = (Integer) groupKey._keys[0];
      assertTrue(_values.contains(key));
      for (int i = 0; i < 6; i++) {
        assertEquals(((Set<Integer>) aggregationGroupByResult.getResultForGroupId(i, groupKey._groupId)).size(), 1);
      }
    }
    assertEquals(numGroups, _values.size());

    // Inter segments
    Object[] expectedRow = Collections.nCopies(6, 1).toArray();
    List<Object[]> expectedRows = Collections.nCopies(10, expectedRow);
    QueriesTestUtils.testInterSegmentsResult(getBrokerResponse(query), 4 * NUM_RECORDS, 0, 4 * 6 * NUM_RECORDS,
        4 * NUM_RECORDS, expectedRows);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
