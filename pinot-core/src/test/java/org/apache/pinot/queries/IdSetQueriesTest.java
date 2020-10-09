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
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.AggregationResult;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.GroupByResult;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.query.AggregationGroupByOperator;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.utils.idset.BloomFilterIdSet;
import org.apache.pinot.core.query.utils.idset.EmptyIdSet;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.core.query.utils.idset.Roaring64NavigableMapIdSet;
import org.apache.pinot.core.query.utils.idset.RoaringBitmapIdSet;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Queries test for ID_SET queries.
 */
public class IdSetQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "IdSetQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final Random RANDOM = new Random();

  private static final int NUM_RECORDS = 1000;
  private static final int MAX_VALUE = 2000;

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

  private final int[] _values = new int[NUM_RECORDS];
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    // NOTE: Use a filter that does not match any record to test the empty IdSet.
    return " WHERE intColumn > 2000";
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
    for (int i = 0; i < NUM_RECORDS; i++) {
      int intValue = RANDOM.nextInt(MAX_VALUE);
      _values[i] = intValue;
      long longValue = intValue + (long) Integer.MAX_VALUE;
      float floatValue = intValue + 0.5f;
      double doubleValue = intValue + 0.25;
      String stringValue = Integer.toString(intValue);
      byte[] bytesValue = stringValue.getBytes();
      GenericRow record = new GenericRow();
      record.putValue(INT_COLUMN, intValue);
      record.putValue(LONG_COLUMN, longValue);
      record.putValue(FLOAT_COLUMN, floatValue);
      record.putValue(DOUBLE_COLUMN, doubleValue);
      record.putValue(STRING_COLUMN, stringValue);
      record.putValue(BYTES_COLUMN, bytesValue);
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
  public void testAggregationOnly()
      throws IOException {
    String query =
        "SELECT IDSET(intColumn), IDSET(longColumn), IDSET(floatColumn), IDSET(doubleColumn), IDSET(stringColumn), IDSET(bytesColumn) FROM testTable";

    // Inner segment
    {
      // Without filter
      AggregationOperator aggregationOperator = getOperatorForPqlQuery(query);
      IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
          6 * NUM_RECORDS, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getAggregationResult();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 6);
      RoaringBitmapIdSet intIdSet = (RoaringBitmapIdSet) aggregationResult.get(0);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intIdSet.contains(_values[i]));
      }
      Roaring64NavigableMapIdSet longIdSet = (Roaring64NavigableMapIdSet) aggregationResult.get(1);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(longIdSet.contains(_values[i] + (long) Integer.MAX_VALUE));
      }
      BloomFilterIdSet floatIdSet = (BloomFilterIdSet) aggregationResult.get(2);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(floatIdSet.contains(_values[i] + 0.5f));
      }
      BloomFilterIdSet doubleIdSet = (BloomFilterIdSet) aggregationResult.get(3);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(doubleIdSet.contains(_values[i] + 0.25));
      }
      BloomFilterIdSet stringIdSet = (BloomFilterIdSet) aggregationResult.get(4);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(stringIdSet.contains(Integer.toString(_values[i])));
      }
      BloomFilterIdSet bytesIdSet = (BloomFilterIdSet) aggregationResult.get(5);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(bytesIdSet.contains(Integer.toString(_values[i]).getBytes()));
      }
    }
    {
      // With filter
      AggregationOperator aggregationOperator = getOperatorForPqlQueryWithFilter(query);
      IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
      QueriesTestUtils
          .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 0, 0, 0, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getAggregationResult();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 6);
      for (int i = 0; i < 6; i++) {
        assertTrue(aggregationResult.get(i) instanceof EmptyIdSet);
      }
    }

    // Inter segments
    {
      // Without filter (expect 4 * inner segment result)
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
      Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 6 * NUM_RECORDS);
      Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
      List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
      Assert.assertEquals(aggregationResults.size(), 6);
      RoaringBitmapIdSet intIdSet =
          (RoaringBitmapIdSet) IdSets.fromBase64String((String) aggregationResults.get(0).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intIdSet.contains(_values[i]));
      }
      Roaring64NavigableMapIdSet longIdSet =
          (Roaring64NavigableMapIdSet) IdSets.fromBase64String((String) aggregationResults.get(1).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(longIdSet.contains(_values[i] + (long) Integer.MAX_VALUE));
      }
      BloomFilterIdSet floatIdSet =
          (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults.get(2).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(floatIdSet.contains(_values[i] + 0.5f));
      }
      BloomFilterIdSet doubleIdSet =
          (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults.get(3).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(doubleIdSet.contains(_values[i] + 0.25));
      }
      BloomFilterIdSet stringIdSet =
          (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults.get(4).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(stringIdSet.contains(Integer.toString(_values[i])));
      }
      BloomFilterIdSet bytesIdSet =
          (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults.get(5).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(bytesIdSet.contains(Integer.toString(_values[i]).getBytes()));
      }
    }
    {
      // With filter
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQueryWithFilter(query);
      Assert.assertEquals(brokerResponse.getNumDocsScanned(), 0);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 0);
      Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
      List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
      Assert.assertEquals(aggregationResults.size(), 6);
      for (int i = 0; i < 6; i++) {
        assertTrue(IdSets.fromBase64String((String) aggregationResults.get(i).getValue()) instanceof EmptyIdSet);
      }
    }
  }

  @Test
  public void testAggregationGroupBy()
      throws IOException {
    String query =
        "SELECT IDSET(intColumn), IDSET(longColumn), IDSET(floatColumn), IDSET(doubleColumn), IDSET(stringColumn), IDSET(bytesColumn) FROM testTable GROUP BY 1";

    // Inner segment
    {
      AggregationGroupByOperator aggregationGroupByOperator = getOperatorForPqlQuery(query);
      IntermediateResultsBlock resultsBlock = aggregationGroupByOperator.nextBlock();
      QueriesTestUtils
          .testInnerSegmentExecutionStatistics(aggregationGroupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
              6 * NUM_RECORDS, NUM_RECORDS);
      AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
      assertNotNull(aggregationGroupByResult);
      Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
      GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
      RoaringBitmapIdSet intIdSet = (RoaringBitmapIdSet) aggregationGroupByResult.getResultForKey(groupKey, 0);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intIdSet.contains(_values[i]));
      }
      Roaring64NavigableMapIdSet longIdSet =
          (Roaring64NavigableMapIdSet) aggregationGroupByResult.getResultForKey(groupKey, 1);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(longIdSet.contains(_values[i] + (long) Integer.MAX_VALUE));
      }
      BloomFilterIdSet floatIdSet = (BloomFilterIdSet) aggregationGroupByResult.getResultForKey(groupKey, 2);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(floatIdSet.contains(_values[i] + 0.5f));
      }
      BloomFilterIdSet doubleIdSet = (BloomFilterIdSet) aggregationGroupByResult.getResultForKey(groupKey, 3);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(doubleIdSet.contains(_values[i] + 0.25));
      }
      BloomFilterIdSet stringIdSet = (BloomFilterIdSet) aggregationGroupByResult.getResultForKey(groupKey, 4);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(stringIdSet.contains(Integer.toString(_values[i])));
      }
      BloomFilterIdSet bytesIdSet = (BloomFilterIdSet) aggregationGroupByResult.getResultForKey(groupKey, 5);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(bytesIdSet.contains(Integer.toString(_values[i]).getBytes()));
      }
      assertFalse(groupKeyIterator.hasNext());
    }

    // Inter segments (expect 4 * inner segment result)
    {
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
      Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 6 * NUM_RECORDS);
      Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
      List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
      Assert.assertEquals(aggregationResults.size(), 6);
      for (AggregationResult aggregationResult : aggregationResults) {
        Assert.assertNull(aggregationResult.getValue());
        List<GroupByResult> groupByResults = aggregationResult.getGroupByResult();
        assertEquals(groupByResults.size(), 1);
      }
      RoaringBitmapIdSet intIdSet = (RoaringBitmapIdSet) IdSets
          .fromBase64String((String) aggregationResults.get(0).getGroupByResult().get(0).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intIdSet.contains(_values[i]));
      }
      Roaring64NavigableMapIdSet longIdSet = (Roaring64NavigableMapIdSet) IdSets
          .fromBase64String((String) aggregationResults.get(1).getGroupByResult().get(0).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(longIdSet.contains(_values[i] + (long) Integer.MAX_VALUE));
      }
      BloomFilterIdSet floatIdSet = (BloomFilterIdSet) IdSets
          .fromBase64String((String) aggregationResults.get(2).getGroupByResult().get(0).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(floatIdSet.contains(_values[i] + 0.5f));
      }
      BloomFilterIdSet doubleIdSet = (BloomFilterIdSet) IdSets
          .fromBase64String((String) aggregationResults.get(3).getGroupByResult().get(0).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(doubleIdSet.contains(_values[i] + 0.25));
      }
      BloomFilterIdSet stringIdSet = (BloomFilterIdSet) IdSets
          .fromBase64String((String) aggregationResults.get(4).getGroupByResult().get(0).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(stringIdSet.contains(Integer.toString(_values[i])));
      }
      BloomFilterIdSet bytesIdSet = (BloomFilterIdSet) IdSets
          .fromBase64String((String) aggregationResults.get(5).getGroupByResult().get(0).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(bytesIdSet.contains(Integer.toString(_values[i]).getBytes()));
      }
    }
  }

  @Test
  public void testQueryWithParameters()
      throws IOException {
    String query = "SELECT IDSET(intColumn, 'sizeThresholdInBytes=0;expectedInsertions=1000') FROM testTable";

    // Inner segment
    {
      AggregationOperator aggregationOperator = getOperatorForPqlQuery(query);
      IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
          NUM_RECORDS, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getAggregationResult();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 1);
      // Should directly create BloomFilterIdSet
      BloomFilterIdSet intIdSet = (BloomFilterIdSet) aggregationResult.get(0);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intIdSet.contains(_values[i]));
      }
      // Should be much smaller than the default BloomFilterIdSet (4.35MB)
      assertEquals(intIdSet.getSerializedSizeInBytes(), 928);
    }

    // Inter segments (expect 4 * inner segment result)
    {
      BrokerResponseNative brokerResponse = getBrokerResponseForPqlQuery(query);
      Assert.assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      Assert.assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * NUM_RECORDS);
      Assert.assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
      List<AggregationResult> aggregationResults = brokerResponse.getAggregationResults();
      Assert.assertEquals(aggregationResults.size(), 1);
      // Should directly create BloomFilterIdSet
      BloomFilterIdSet intIdSet =
          (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults.get(0).getValue());
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intIdSet.contains(_values[i]));
      }
      // Should be much smaller than the default BloomFilterIdSet (4.35MB)
      assertEquals(intIdSet.getSerializedSizeInBytes(), 928);
    }
  }

  @Test
  public void testInIdSet()
      throws IOException {
    // Create an IdSet with the values from the first half records
    IdSet idSet = IdSets.create(DataType.INT);
    for (int i = 0; i < NUM_RECORDS / 2; i++) {
      idSet.add(_values[i]);
    }
    String serializedIdSet = idSet.toBase64String();

    // Calculate the expected number of matching records
    int expectedNumMatchingRecords = 0;
    for (int value : _values) {
      if (idSet.contains(value)) {
        expectedNumMatchingRecords++;
      }
    }

    {
      String query = "SELECT COUNT(*) FROM testTable where INIDSET(intColumn, '" + serializedIdSet + "') = 1";
      AggregationOperator aggregationOperator = getOperatorForPqlQuery(query);
      IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
      QueriesTestUtils
          .testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), expectedNumMatchingRecords,
              NUM_RECORDS, 0, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getAggregationResult();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 1);
      assertEquals((long) aggregationResult.get(0), expectedNumMatchingRecords);
    }

    {
      String query = "SELECT COUNT(*) FROM testTable where IN_ID_SET(intColumn, '" + serializedIdSet + "') = 0";
      AggregationOperator aggregationOperator = getOperatorForPqlQuery(query);
      IntermediateResultsBlock resultsBlock = aggregationOperator.nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(),
          NUM_RECORDS - expectedNumMatchingRecords, NUM_RECORDS, 0, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getAggregationResult();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 1);
      assertEquals((long) aggregationResult.get(0), NUM_RECORDS - expectedNumMatchingRecords);
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
