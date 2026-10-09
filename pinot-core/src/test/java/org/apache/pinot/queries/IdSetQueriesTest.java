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
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.query.utils.idset.BloomFilterIdSet;
import org.apache.pinot.core.query.utils.idset.EmptyIdSet;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.core.query.utils.idset.Roaring64NavigableMapIdSet;
import org.apache.pinot.core.query.utils.idset.RoaringBitmapIdSet;
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
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Queries test for ID_SET queries.
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
  private static final String INT_MV_COLUMN = "intMVColumn";
  private static final String LONG_MV_COLUMN = "longMVColumn";
  private static final String FLOAT_MV_COLUMN = "floatMVColumn";
  private static final String DOUBLE_MV_COLUMN = "doubleMVColumn";
  private static final String STRING_MV_COLUMN = "stringMVColumn";
  // Holds the INT value, except for every 4th record, which holds NULL
  private static final String NULLABLE_INT_COLUMN = "nullableIntColumn";
  private static final int NUM_NULL_RECORDS = NUM_RECORDS / 4;
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_COLUMN, DataType.DOUBLE).addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES).addMultiValueDimension(INT_MV_COLUMN, DataType.INT)
      .addMultiValueDimension(LONG_MV_COLUMN, DataType.LONG).addMultiValueDimension(FLOAT_MV_COLUMN, DataType.FLOAT)
      .addMultiValueDimension(DOUBLE_MV_COLUMN, DataType.DOUBLE)
      .addMultiValueDimension(STRING_MV_COLUMN, DataType.STRING)
      .addSingleValueDimension(NULLABLE_INT_COLUMN, DataType.INT).build();
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setNullHandlingEnabled(true).build();

  private final int[] _values = new int[NUM_RECORDS];
  private final List<GenericRow> _records = new ArrayList<>(NUM_RECORDS);
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
      record.putValue(INT_MV_COLUMN, new Integer[]{intValue, intValue + MAX_VALUE});
      record.putValue(LONG_MV_COLUMN, new Long[]{longValue, longValue + MAX_VALUE});
      record.putValue(FLOAT_MV_COLUMN, new Float[]{floatValue, floatValue + MAX_VALUE});
      record.putValue(DOUBLE_MV_COLUMN, new Double[]{doubleValue, doubleValue + MAX_VALUE});
      record.putValue(STRING_MV_COLUMN, new String[]{stringValue, stringValue + MAX_VALUE});
      record.putValue(NULLABLE_INT_COLUMN, i % 4 == 0 ? null : intValue);
      records.add(record);
    }
    _records.addAll(records);

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
  public void testCastMV() {
    String query = "SELECT IDSET(CAST(stringMVColumn AS LONG)) FROM testTable";
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    // bloom filter should not be used
    assertTrue(aggregationResult.get(0) instanceof Roaring64NavigableMapIdSet);
    Roaring64NavigableMapIdSet idSet = (Roaring64NavigableMapIdSet) aggregationResult.get(0);
    for (int i = 0; i < NUM_RECORDS; i++) {
      // cast MV column should be used to create the ID_SET
      assertTrue(idSet.contains(Long.parseLong(Integer.toString(_values[i]))));
      assertTrue(idSet.contains(Long.parseLong(Integer.toString(_values[i]) + MAX_VALUE)));
      Exception e = null;
      try {
        // ID_SET applied to different types should fail
        idSet.contains(Integer.toString(_values[i]));
        idSet.contains(Integer.toString(_values[i]) + MAX_VALUE);
      } catch (Exception exception) {
        e = exception;
      }
      assertNotNull(e);
    }
  }

  @Test
  public void testAggregationOnly()
      throws IOException {
    String query =
        "SELECT IDSET(intColumn), IDSET(longColumn), IDSET(floatColumn), IDSET(doubleColumn), IDSET(stringColumn), "
            + "IDSET(bytesColumn), IDSET(intMVColumn), IDSET(longMVColumn), IDSET(floatMVColumn), "
            + "IDSET(doubleMVColumn), IDSET(stringMVColumn) FROM testTable";
    // Inner segment
    {
      // Without filter
      AggregationOperator aggregationOperator = getOperator(query);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
          11 * NUM_RECORDS, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 11);
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
      RoaringBitmapIdSet intMVIdSet = (RoaringBitmapIdSet) aggregationResult.get(6);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intMVIdSet.contains(_values[i]));
        assertTrue(intMVIdSet.contains(_values[i] + MAX_VALUE));
      }
      Roaring64NavigableMapIdSet longMVIdSet = (Roaring64NavigableMapIdSet) aggregationResult.get(7);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(longMVIdSet.contains(_values[i] + (long) Integer.MAX_VALUE));
        assertTrue(longMVIdSet.contains(_values[i] + (long) Integer.MAX_VALUE + MAX_VALUE));
      }
      BloomFilterIdSet floatMVIdSet = (BloomFilterIdSet) aggregationResult.get(8);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(floatMVIdSet.contains(_values[i] + 0.5f));
        assertTrue(floatMVIdSet.contains(_values[i] + 0.5f + MAX_VALUE));
      }
      BloomFilterIdSet doubleMVIdSet = (BloomFilterIdSet) aggregationResult.get(9);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(doubleMVIdSet.contains(_values[i] + 0.25));
        assertTrue(doubleMVIdSet.contains(_values[i] + 0.25 + MAX_VALUE));
      }
      BloomFilterIdSet stringMVIdSet = (BloomFilterIdSet) aggregationResult.get(10);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(stringMVIdSet.contains(Integer.toString(_values[i])));
        assertTrue(stringMVIdSet.contains(Integer.toString(_values[i]) + MAX_VALUE));
      }
    }
    {
      // With filter
      AggregationOperator aggregationOperator = getOperatorWithFilter(query);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), 0, 0, 0,
          NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 11);
      for (int i = 0; i < 11; i++) {
        assertTrue(aggregationResult.get(i) instanceof EmptyIdSet);
      }
    }

    // Inter segments
    {
      // Without filter (expect 4 * inner segment result)
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
      assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 11 * NUM_RECORDS);
      assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
      Object[] aggregationResults = brokerResponse.getResultTable().getRows().get(0);
      assertEquals(aggregationResults.length, 11);
      RoaringBitmapIdSet intIdSet = (RoaringBitmapIdSet) IdSets.fromBase64String((String) aggregationResults[0]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intIdSet.contains(_values[i]));
      }
      Roaring64NavigableMapIdSet longIdSet =
          (Roaring64NavigableMapIdSet) IdSets.fromBase64String((String) aggregationResults[1]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(longIdSet.contains(_values[i] + (long) Integer.MAX_VALUE));
      }
      BloomFilterIdSet floatIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults[2]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(floatIdSet.contains(_values[i] + 0.5f));
      }
      BloomFilterIdSet doubleIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults[3]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(doubleIdSet.contains(_values[i] + 0.25));
      }
      BloomFilterIdSet stringIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults[4]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(stringIdSet.contains(Integer.toString(_values[i])));
      }
      BloomFilterIdSet bytesIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults[5]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(bytesIdSet.contains(Integer.toString(_values[i]).getBytes()));
      }
      RoaringBitmapIdSet intMVIdSet = (RoaringBitmapIdSet) IdSets.fromBase64String((String) aggregationResults[6]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intMVIdSet.contains(_values[i]));
        assertTrue(intMVIdSet.contains(_values[i] + MAX_VALUE));
      }
      Roaring64NavigableMapIdSet longMVIdSet =
          (Roaring64NavigableMapIdSet) IdSets.fromBase64String((String) aggregationResults[7]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(longMVIdSet.contains(_values[i] + (long) Integer.MAX_VALUE));
        assertTrue(longMVIdSet.contains(_values[i] + (long) Integer.MAX_VALUE + MAX_VALUE));
      }
      BloomFilterIdSet floatMVIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults[8]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(floatMVIdSet.contains(_values[i] + 0.5f));
        assertTrue(floatMVIdSet.contains(_values[i] + 0.5f + MAX_VALUE));
      }
      BloomFilterIdSet doubleMVIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults[9]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(doubleMVIdSet.contains(_values[i] + 0.25));
        assertTrue(doubleMVIdSet.contains(_values[i] + 0.25 + MAX_VALUE));
      }
      BloomFilterIdSet stringMVIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults[10]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(stringMVIdSet.contains(Integer.toString(_values[i])));
        assertTrue(stringMVIdSet.contains(Integer.toString(_values[i]) + MAX_VALUE));
      }
    }
    {
      // With filter
      BrokerResponseNative brokerResponse = getBrokerResponseWithFilter(query);
      assertEquals(brokerResponse.getNumDocsScanned(), 0);
      assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 0);
      assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
      Object[] aggregationResults = brokerResponse.getResultTable().getRows().get(0);
      assertEquals(aggregationResults.length, 11);
      for (int i = 0; i < 6; i++) {
        assertTrue(IdSets.fromBase64String((String) aggregationResults[i]) instanceof EmptyIdSet);
      }
    }
  }

  @Test
  public void testAggregationGroupBy()
      throws IOException {
    String query = "SELECT IDSET(intColumn), IDSET(longColumn), IDSET(floatColumn), IDSET(doubleColumn), "
        + "IDSET(stringColumn), IDSET(bytesColumn), IDSET(intMVColumn), IDSET(longMVColumn), IDSET(floatMVColumn), "
        + "IDSET(doubleMVColumn), IDSET(stringMVColumn) FROM testTable GROUP BY '1'";

    // Inner segment
    {
      GroupByOperator groupByOperator = getOperator(query);
      GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
          11 * NUM_RECORDS, NUM_RECORDS);
      AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
      assertNotNull(aggregationGroupByResult);
      Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
      int groupId = groupKeyIterator.next()._groupId;
      RoaringBitmapIdSet intIdSet = (RoaringBitmapIdSet) aggregationGroupByResult.getResultForGroupId(0, groupId);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intIdSet.contains(_values[i]));
      }
      Roaring64NavigableMapIdSet longIdSet =
          (Roaring64NavigableMapIdSet) aggregationGroupByResult.getResultForGroupId(1, groupId);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(longIdSet.contains(_values[i] + (long) Integer.MAX_VALUE));
      }
      BloomFilterIdSet floatIdSet = (BloomFilterIdSet) aggregationGroupByResult.getResultForGroupId(2, groupId);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(floatIdSet.contains(_values[i] + 0.5f));
      }
      BloomFilterIdSet doubleIdSet = (BloomFilterIdSet) aggregationGroupByResult.getResultForGroupId(3, groupId);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(doubleIdSet.contains(_values[i] + 0.25));
      }
      BloomFilterIdSet stringIdSet = (BloomFilterIdSet) aggregationGroupByResult.getResultForGroupId(4, groupId);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(stringIdSet.contains(Integer.toString(_values[i])));
      }
      BloomFilterIdSet bytesIdSet = (BloomFilterIdSet) aggregationGroupByResult.getResultForGroupId(5, groupId);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(bytesIdSet.contains(Integer.toString(_values[i]).getBytes()));
      }
      RoaringBitmapIdSet intMVIdSet = (RoaringBitmapIdSet) aggregationGroupByResult.getResultForGroupId(6, groupId);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intMVIdSet.contains(_values[i]));
        assertTrue(intMVIdSet.contains(_values[i] + MAX_VALUE));
      }
      Roaring64NavigableMapIdSet longMVIdSet =
          (Roaring64NavigableMapIdSet) aggregationGroupByResult.getResultForGroupId(7, groupId);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(longMVIdSet.contains(_values[i] + (long) Integer.MAX_VALUE));
        assertTrue(longMVIdSet.contains(_values[i] + (long) Integer.MAX_VALUE + MAX_VALUE));
      }
      BloomFilterIdSet floatMVIdSet = (BloomFilterIdSet) aggregationGroupByResult.getResultForGroupId(8, groupId);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(floatMVIdSet.contains(_values[i] + 0.5f));
        assertTrue(floatMVIdSet.contains(_values[i] + 0.5f + MAX_VALUE));
      }
      BloomFilterIdSet doubleMVIdSet = (BloomFilterIdSet) aggregationGroupByResult.getResultForGroupId(9, groupId);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(doubleMVIdSet.contains(_values[i] + 0.25));
        assertTrue(doubleMVIdSet.contains(_values[i] + 0.25 + MAX_VALUE));
      }
      BloomFilterIdSet stringMVIdSet = (BloomFilterIdSet) aggregationGroupByResult.getResultForGroupId(10, groupId);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(stringMVIdSet.contains(Integer.toString(_values[i])));
        assertTrue(stringMVIdSet.contains(Integer.toString(_values[i]) + MAX_VALUE));
      }
      assertFalse(groupKeyIterator.hasNext());
    }

    // Inter segments (expect 4 * inner segment result)
    {
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
      assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 11 * NUM_RECORDS);
      assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
      List<Object[]> rows = brokerResponse.getResultTable().getRows();
      assertEquals(rows.size(), 1);
      Object[] results = brokerResponse.getResultTable().getRows().get(0);
      assertEquals(results.length, 11);
      RoaringBitmapIdSet intIdSet = (RoaringBitmapIdSet) IdSets.fromBase64String((String) results[0]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intIdSet.contains(_values[i]));
      }
      Roaring64NavigableMapIdSet longIdSet = (Roaring64NavigableMapIdSet) IdSets.fromBase64String((String) results[1]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(longIdSet.contains(_values[i] + (long) Integer.MAX_VALUE));
      }
      BloomFilterIdSet floatIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) results[2]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(floatIdSet.contains(_values[i] + 0.5f));
      }
      BloomFilterIdSet doubleIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) results[3]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(doubleIdSet.contains(_values[i] + 0.25));
      }
      BloomFilterIdSet stringIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) results[4]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(stringIdSet.contains(Integer.toString(_values[i])));
      }
      BloomFilterIdSet bytesIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) results[5]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(bytesIdSet.contains(Integer.toString(_values[i]).getBytes()));
      }
      RoaringBitmapIdSet intMVIdSet = (RoaringBitmapIdSet) IdSets.fromBase64String((String) results[6]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(intMVIdSet.contains(_values[i]));
        assertTrue(intMVIdSet.contains(_values[i] + MAX_VALUE));
      }
      Roaring64NavigableMapIdSet longMVIdSet =
          (Roaring64NavigableMapIdSet) IdSets.fromBase64String((String) results[7]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(longMVIdSet.contains(_values[i] + (long) Integer.MAX_VALUE));
        assertTrue(longMVIdSet.contains(_values[i] + (long) Integer.MAX_VALUE + MAX_VALUE));
      }
      BloomFilterIdSet floatMVIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) results[8]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(floatMVIdSet.contains(_values[i] + 0.5f));
        assertTrue(floatMVIdSet.contains(_values[i] + 0.5f + MAX_VALUE));
      }
      BloomFilterIdSet doubleMVIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) results[9]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(doubleMVIdSet.contains(_values[i] + 0.25));
        assertTrue(doubleMVIdSet.contains(_values[i] + 0.25 + MAX_VALUE));
      }
      BloomFilterIdSet stringMVIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) results[10]);
      for (int i = 0; i < NUM_RECORDS; i++) {
        assertTrue(stringMVIdSet.contains(Integer.toString(_values[i])));
        assertTrue(stringMVIdSet.contains(Integer.toString(_values[i]) + MAX_VALUE));
      }
    }
  }

  @Test
  public void testQueryWithParameters()
      throws IOException {
    String query = "SELECT IDSET(intColumn, 'sizeThresholdInBytes=0;expectedInsertions=1000') FROM testTable";

    // Inner segment
    {
      AggregationOperator aggregationOperator = getOperator(query);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
          NUM_RECORDS, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
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
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
      assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * NUM_RECORDS);
      assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
      Object[] aggregationResults = brokerResponse.getResultTable().getRows().get(0);
      assertEquals(aggregationResults.length, 1);
      // Should directly create BloomFilterIdSet
      BloomFilterIdSet intIdSet = (BloomFilterIdSet) IdSets.fromBase64String((String) aggregationResults[0]);
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
      AggregationOperator aggregationOperator = getOperator(query);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(),
          expectedNumMatchingRecords, NUM_RECORDS, 0, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 1);
      assertEquals((long) aggregationResult.get(0), expectedNumMatchingRecords);
    }

    {
      String query = "SELECT COUNT(*) FROM testTable where IN_ID_SET(intColumn, '" + serializedIdSet + "') = 0";
      AggregationOperator aggregationOperator = getOperator(query);
      AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(),
          NUM_RECORDS - expectedNumMatchingRecords, NUM_RECORDS, 0, NUM_RECORDS);
      List<Object> aggregationResult = resultsBlock.getResults();
      assertNotNull(aggregationResult);
      assertEquals(aggregationResult.size(), 1);
      assertEquals((long) aggregationResult.get(0), NUM_RECORDS - expectedNumMatchingRecords);
    }
  }

  @Test
  public void testInIdSetOnEveryValueType()
      throws IOException {
    testInIdSet(INT_COLUMN, IdSets.create(DataType.INT));
    testInIdSet(LONG_COLUMN, IdSets.create(DataType.LONG));
    // Bloom filters sized for the records, to keep the literals small
    testInIdSet(FLOAT_COLUMN, createBloomFilterIdSet(DataType.FLOAT));
    testInIdSet(DOUBLE_COLUMN, createBloomFilterIdSet(DataType.DOUBLE));
    testInIdSet(STRING_COLUMN, createBloomFilterIdSet(DataType.STRING));
    testInIdSet(BYTES_COLUMN, createBloomFilterIdSet(DataType.BYTES));
  }

  private static IdSet createBloomFilterIdSet(DataType dataType) {
    return IdSets.create(dataType, IdSets.DEFAULT_SIZE_THRESHOLD_IN_BYTES, NUM_RECORDS, IdSets.DEFAULT_FPP);
  }

  /// Fills the IdSet with the column values of the first half of the records, then checks that IN_ID_SET matches
  /// the records the IdSet contains, including any Bloom filter false positives.
  private void testInIdSet(String column, IdSet idSet)
      throws IOException {
    for (int i = 0; i < NUM_RECORDS / 2; i++) {
      Object value = _records.get(i).getValue(column);
      if (value instanceof Integer) {
        idSet.add((int) value);
      } else if (value instanceof Long) {
        idSet.add((long) value);
      } else if (value instanceof Float) {
        idSet.add((float) value);
      } else if (value instanceof Double) {
        idSet.add((double) value);
      } else if (value instanceof String) {
        idSet.add((String) value);
      } else {
        idSet.add((byte[]) value);
      }
    }
    long expectedNumMatchingRecords = 0;
    for (GenericRow record : _records) {
      Object value = record.getValue(column);
      boolean contains;
      if (value instanceof Integer) {
        contains = idSet.contains((int) value);
      } else if (value instanceof Long) {
        contains = idSet.contains((long) value);
      } else if (value instanceof Float) {
        contains = idSet.contains((float) value);
      } else if (value instanceof Double) {
        contains = idSet.contains((double) value);
      } else if (value instanceof String) {
        contains = idSet.contains((String) value);
      } else {
        contains = idSet.contains((byte[]) value);
      }
      if (contains) {
        expectedNumMatchingRecords++;
      }
    }
    String query =
        "SELECT COUNT(*) FROM testTable WHERE IN_ID_SET(" + column + ", '" + idSet.toBase64String() + "') = 1";
    AggregationOperator aggregationOperator = getOperator(query);
    List<Object> aggregationResult = aggregationOperator.nextBlock().getResults();
    assertNotNull(aggregationResult);
    assertEquals((long) aggregationResult.get(0), expectedNumMatchingRecords, column);
  }

  @Test
  public void testInIdSetInHaving()
      throws IOException {
    // IdSets of the int and long values of the first half of the records
    IdSet intIdSet = IdSets.create(DataType.INT);
    IdSet longIdSet = IdSets.create(DataType.LONG);
    for (int i = 0; i < NUM_RECORDS / 2; i++) {
      intIdSet.add(_values[i]);
      longIdSet.add(_values[i] + (long) Integer.MAX_VALUE);
    }
    long expectedNumGroups = Arrays.stream(_values).distinct().filter(intIdSet::contains).count();

    // HAVING on a group key
    BrokerResponseNative brokerResponse = getBrokerResponse(
        "SELECT intColumn, COUNT(*) FROM testTable GROUP BY intColumn HAVING IN_ID_SET(intColumn, '"
            + intIdSet.toBase64String() + "') = 1 LIMIT " + NUM_RECORDS);
    assertTrue(brokerResponse.getExceptions().isEmpty(), brokerResponse.getExceptions().toString());
    assertEquals(brokerResponse.getResultTable().getRows().size(), expectedNumGroups);

    // HAVING on an aggregation, which returns DOUBLE here, so it is cast to the type of the IdSet. Every group holds a
    // single int value, so its maximum long value is in the IdSet when its int value is.
    brokerResponse = getBrokerResponse(
        "SELECT stringColumn, MAX(longColumn) FROM testTable GROUP BY stringColumn HAVING IN_ID_SET(CAST(MAX("
            + "longColumn) AS LONG), '" + longIdSet.toBase64String() + "') = 1 LIMIT " + NUM_RECORDS);
    assertTrue(brokerResponse.getExceptions().isEmpty(), brokerResponse.getExceptions().toString());
    assertEquals(brokerResponse.getResultTable().getRows().size(), expectedNumGroups);

    // Without the cast, this RoaringBitmap-backed LONG IdSet rejects the DOUBLE values. A LONG IdSet large enough to be
    // a Bloom filter would accept them and hash their raw bits, missing every group, because LONG and DOUBLE values
    // share a funnel.
    brokerResponse = getBrokerResponse(
        "SELECT stringColumn, MAX(longColumn) FROM testTable GROUP BY stringColumn HAVING IN_ID_SET(MAX(longColumn), '"
            + longIdSet.toBase64String() + "') = 1 LIMIT " + NUM_RECORDS);
    assertTrue(brokerResponse.getExceptions().toString()
            .contains("Cannot look up DOUBLE values in an IdSet built from LONG values"),
        brokerResponse.getExceptions().toString());
  }

  @Test
  public void testInIdSetRejectsMismatchedValueType()
      throws IOException {
    IdSet idSet = IdSets.create(DataType.INT);
    idSet.add(_values[0]);
    String query = "SELECT COUNT(*) FROM testTable WHERE IN_ID_SET(longColumn, '" + idSet.toBase64String() + "') = 1";
    BadQueryRequestException exception = expectThrows(BadQueryRequestException.class, () -> getOperator(query));
    assertTrue(exception.getMessage().contains("Cannot look up LONG values in an IdSet built from INT values"),
        exception.getMessage());
  }

  /// As with an `IN` subquery, no value is in an empty IdSet, not even NULL. With null handling enabled, `IN_ID_SET` is
  /// FALSE for a NULL value when the IdSet is empty, and NULL when it is not.
  @Test
  public void testInIdSetNullHandling()
      throws IOException {
    IdSet idSet = IdSets.create(DataType.INT);
    for (int value : _values) {
      idSet.add(value);
    }
    // 2 instances with 2 segments each
    int numRecords = 4 * NUM_RECORDS;
    int numNullRecords = 4 * NUM_NULL_RECORDS;

    String emptyIdSet = IdSets.emptyIdSet().toBase64String();
    assertCount(NULLABLE_INT_COLUMN, emptyIdSet, 1, 0);
    assertCount(NULLABLE_INT_COLUMN, emptyIdSet, 0, numRecords);
    // Whether a NULL value is in a non-empty IdSet is unknown, so neither filter keeps the NULL records
    assertCount(NULLABLE_INT_COLUMN, idSet.toBase64String(), 1, numRecords - numNullRecords);
    assertCount(NULLABLE_INT_COLUMN, idSet.toBase64String(), 0, 0);
  }

  private void assertCount(String column, String serializedIdSet, int inIdSet, long expectedCount) {
    BrokerResponseNative brokerResponse = getBrokerResponse(
        "SET enableNullHandling = true; SELECT COUNT(*) FROM testTable WHERE IN_ID_SET(" + column + ", '"
            + serializedIdSet + "') = " + inIdSet);
    assertTrue(brokerResponse.getExceptions().isEmpty(), brokerResponse.getExceptions().toString());
    assertEquals(brokerResponse.getResultTable().getRows().get(0)[0], expectedCount,
        "IN_ID_SET = " + inIdSet + " with IdSet " + serializedIdSet);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
