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
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.operator.blocks.results.AggregationResultsBlock;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.operator.query.AggregationOperator;
import org.apache.pinot.core.operator.query.GroupByOperator;
import org.apache.pinot.core.query.aggregation.groupby.AggregationGroupByResult;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.segment.local.customobject.ThetaSketchAccumulator;
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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Queries test for DISTINCT_COUNT_THETA_SKETCH queries.
 */
@SuppressWarnings("unchecked")
public class DistinctCountThetaSketchQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "DistinctCountThetaSketchQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final int NUM_RECORDS = 1000;

  private static final String INT_SV_COLUMN = "intSVColumn";
  private static final String LONG_SV_COLUMN = "longSVColumn";
  private static final String FLOAT_SV_COLUMN = "floatSVColumn";
  private static final String DOUBLE_SV_COLUMN = "doubleSVColumn";
  private static final String STRING_SV_COLUMN = "stringSVColumn";
  private static final String INT_MV_COLUMN = "intMVColumn";
  private static final String LONG_MV_COLUMN = "longMVColumn";
  private static final String FLOAT_MV_COLUMN = "floatMVColumn";
  private static final String DOUBLE_MV_COLUMN = "doubleMVColumn";
  private static final String STRING_MV_COLUMN = "stringMVColumn";
  private static final String BYTES_COLUMN = "bytesColumn";
  private static final Schema SCHEMA = new Schema.SchemaBuilder().addSingleValueDimension(INT_SV_COLUMN, DataType.INT)
      .addSingleValueDimension(LONG_SV_COLUMN, DataType.LONG).addSingleValueDimension(FLOAT_SV_COLUMN, DataType.FLOAT)
      .addSingleValueDimension(DOUBLE_SV_COLUMN, DataType.DOUBLE)
      .addSingleValueDimension(STRING_SV_COLUMN, DataType.STRING).addMultiValueDimension(INT_MV_COLUMN, DataType.INT)
      .addMultiValueDimension(LONG_MV_COLUMN, DataType.LONG).addMultiValueDimension(FLOAT_MV_COLUMN, DataType.FLOAT)
      .addMultiValueDimension(DOUBLE_MV_COLUMN, DataType.DOUBLE)
      .addMultiValueDimension(STRING_MV_COLUMN, DataType.STRING).addMetric(BYTES_COLUMN, DataType.BYTES).build();
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
    FileUtils.deleteDirectory(INDEX_DIR);
    UpdateSketchBuilder sketchBuilder = new UpdateSketchBuilder();

    List<GenericRow> records = new ArrayList<>(NUM_RECORDS);
    for (int i = 0; i < NUM_RECORDS; i++) {
      GenericRow record = new GenericRow();
      record.putValue(INT_SV_COLUMN, i);
      record.putValue(LONG_SV_COLUMN, i);
      record.putValue(FLOAT_SV_COLUMN, i);
      record.putValue(DOUBLE_SV_COLUMN, i);
      record.putValue(STRING_SV_COLUMN, i);
      Integer[] mvEntry = new Integer[]{i, i + NUM_RECORDS, i + 2 * NUM_RECORDS};
      record.putValue(INT_MV_COLUMN, mvEntry);
      record.putValue(LONG_MV_COLUMN, mvEntry);
      record.putValue(FLOAT_MV_COLUMN, mvEntry);
      record.putValue(DOUBLE_MV_COLUMN, mvEntry);
      record.putValue(STRING_MV_COLUMN, mvEntry);
      // Store serialized sketches in the BYTES column
      UpdateSketch sketch = sketchBuilder.build();
      sketch.update(i);
      sketch.update(i + NUM_RECORDS);
      sketch.update(i + 2 * NUM_RECORDS);
      record.putValue(BYTES_COLUMN, sketch.compact().toByteArray());
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
  public void testAggregationOnly() {
    String query = "SELECT DISTINCT_COUNT_THETA_SKETCH(intSVColumn), DISTINCT_COUNT_THETA_SKETCH(longSVColumn), "
        + "DISTINCT_COUNT_THETA_SKETCH(floatSVColumn), DISTINCT_COUNT_THETA_SKETCH(doubleSVColumn), "
        + "DISTINCT_COUNT_THETA_SKETCH(stringSVColumn), DISTINCT_COUNT_THETA_SKETCH(intMVColumn), "
        + "DISTINCT_COUNT_THETA_SKETCH(longMVColumn), DISTINCT_COUNT_THETA_SKETCH(floatMVColumn), "
        + "DISTINCT_COUNT_THETA_SKETCH(doubleMVColumn), DISTINCT_COUNT_THETA_SKETCH(stringMVColumn), "
        + "DISTINCT_COUNT_THETA_SKETCH(bytesColumn) FROM testTable";

    // Inner segment
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        11 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 11);
    for (int i = 0; i < 11; i++) {
      List<ThetaSketchAccumulator> accumulators = (List<ThetaSketchAccumulator>) aggregationResult.get(i);
      assertEquals(accumulators.size(), 1);
      ThetaSketchAccumulator accumulator = accumulators.get(0);
      if (i < 5) {
        assertEquals(Math.round(accumulator.getResult().getEstimate()), NUM_RECORDS);
      } else {
        assertEquals(Math.round(accumulator.getResult().getEstimate()), 3 * NUM_RECORDS);
      }
    }

    // Inter segments
    Object[] expectedResults = new Object[11];
    for (int i = 0; i < 11; i++) {
      if (i < 5) {
        expectedResults[i] = (long) NUM_RECORDS;
      } else {
        expectedResults[i] = (long) (3 * NUM_RECORDS);
      }
    }
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 4 * NUM_RECORDS, 0, 4 * 11 * NUM_RECORDS, 4 * NUM_RECORDS,
        expectedResults);
  }

  @Test
  public void testAggregationGroupBy() {
    String baseQuery = "SELECT DISTINCT_COUNT_THETA_SKETCH(intSVColumn), DISTINCT_COUNT_THETA_SKETCH(longSVColumn), "
        + "DISTINCT_COUNT_THETA_SKETCH(floatSVColumn), DISTINCT_COUNT_THETA_SKETCH(doubleSVColumn), "
        + "DISTINCT_COUNT_THETA_SKETCH(stringSVColumn), DISTINCT_COUNT_THETA_SKETCH(intMVColumn), "
        + "DISTINCT_COUNT_THETA_SKETCH(longMVColumn), DISTINCT_COUNT_THETA_SKETCH(floatMVColumn), "
        + "DISTINCT_COUNT_THETA_SKETCH(doubleMVColumn), DISTINCT_COUNT_THETA_SKETCH(stringMVColumn), "
        + "DISTINCT_COUNT_THETA_SKETCH(bytesColumn) FROM testTable GROUP BY ";
    for (boolean groupBySV : new boolean[]{true, false}) {
      String query = baseQuery + (groupBySV ? "intSVColumn" : "intMVColumn");

      // Inner segment
      GroupByOperator groupByOperator = getOperator(query);
      GroupByResultsBlock resultsBlock = groupByOperator.nextBlock();
      QueriesTestUtils.testInnerSegmentExecutionStatistics(groupByOperator.getExecutionStatistics(), NUM_RECORDS, 0,
          11 * NUM_RECORDS, NUM_RECORDS);
      AggregationGroupByResult aggregationGroupByResult = resultsBlock.getAggregationGroupByResult();
      assertNotNull(aggregationGroupByResult);
      int numGroups = 0;
      Iterator<GroupKeyGenerator.GroupKey> groupKeyIterator = aggregationGroupByResult.getGroupKeyIterator();
      while (groupKeyIterator.hasNext()) {
        numGroups++;
        GroupKeyGenerator.GroupKey groupKey = groupKeyIterator.next();
        for (int i = 0; i < 6; i++) {
          List<ThetaSketchAccumulator> accumulators =
              (List<ThetaSketchAccumulator>) aggregationGroupByResult.getResultForGroupId(i, groupKey._groupId);
          assertEquals(accumulators.size(), 1);
          Sketch sketch = accumulators.get(0).getResult();
          if (i < 5) {
            assertEquals(Math.round(sketch.getEstimate()), 1);
          } else {
            assertEquals(Math.round(sketch.getEstimate()), 3);
          }
        }
      }
      if (groupBySV) {
        assertEquals(numGroups, NUM_RECORDS);
      } else {
        assertEquals(numGroups, 3 * NUM_RECORDS);
      }

      // Inter segments
      BrokerResponseNative brokerResponse = getBrokerResponse(query);
      assertEquals(brokerResponse.getNumDocsScanned(), 4 * NUM_RECORDS);
      assertEquals(brokerResponse.getNumEntriesScannedInFilter(), 0);
      assertEquals(brokerResponse.getNumEntriesScannedPostFilter(), 4 * 11 * NUM_RECORDS);
      assertEquals(brokerResponse.getTotalDocs(), 4 * NUM_RECORDS);
      List<Object[]> rows = brokerResponse.getResultTable().getRows();
      assertEquals(rows.size(), 10);
      for (Object[] row : rows) {
        assertEquals(row.length, 11);
        for (int i = 0; i < 11; i++) {
          if (i < 5) {
            assertEquals(row[i], 1L);
          } else {
            assertEquals(row[i], 3L);
          }
        }
      }
    }
  }

  @Test
  public void testPostAggregation() {
    String query = "SELECT DISTINCT_COUNT_THETA_SKETCH(intSVColumn, '', "
        // [300, 500), [800, 900)
        + "'longSVColumn >= 300 AND (floatSVColumn < 500 OR doubleSVColumn BETWEEN 800 AND 899)', "
        // [400, 850)
        + "'intMVColumn >= 2400 AND longMVColumn < 850', "
        // [825, 1000)
        + "'floatMVColumn >= 2825', "
        // [0, 100)
        + "'doubleMVColumn < 100', "
        // Expected: [0, 100), [400, 500), [800, 825)
        + "'SET_UNION($4,SET_DIFF(SET_INTERSECT($1,$2),$3))') FROM testTable";

    // Inner segment
    AggregationOperator aggregationOperator = getOperator(query);
    AggregationResultsBlock resultsBlock = aggregationOperator.nextBlock();
    QueriesTestUtils.testInnerSegmentExecutionStatistics(aggregationOperator.getExecutionStatistics(), NUM_RECORDS, 0,
        8 * NUM_RECORDS, NUM_RECORDS);
    List<Object> aggregationResult = resultsBlock.getResults();
    assertNotNull(aggregationResult);
    assertEquals(aggregationResult.size(), 1);
    List<ThetaSketchAccumulator> accumulators = (List<ThetaSketchAccumulator>) aggregationResult.get(0);
    assertEquals(accumulators.size(), 5);
    assertTrue(accumulators.get(0).getResult().isEmpty());
    assertEquals(Math.round(accumulators.get(1).getResult().getEstimate()), 300);
    assertEquals(Math.round(accumulators.get(2).getResult().getEstimate()), 450);
    assertEquals(Math.round(accumulators.get(3).getResult().getEstimate()), 175);
    assertEquals(Math.round(accumulators.get(4).getResult().getEstimate()), 100);

    // Inter segments
    Object[] expectedResults = new Object[]{225L};
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    QueriesTestUtils.testInterSegmentsResult(brokerResponse, 4 * NUM_RECORDS, 0, 4 * 8 * NUM_RECORDS, 4 * NUM_RECORDS,
        expectedResults);
  }

  @Test
  public void testDistinctCountRawThetaSketch() {
    String query = "SELECT DISTINCT_COUNT_RAW_THETA_SKETCH(intSVColumn) FROM testTable";
    BrokerResponseNative brokerResponse = getBrokerResponse(query);
    String serializedSketch = (String) brokerResponse.getResultTable().getRows().get(0)[0];
    Sketch sketch = ObjectSerDeUtils.DATA_SKETCH_THETA_SER_DE.deserialize(Base64.getDecoder().decode(serializedSketch));
    assertEquals(Math.round(sketch.getEstimate()), NUM_RECORDS);
  }

  @Test
  public void testInvalidQueries() {
    testInvalidQuery("select DISTINCT_COUNT_THETA_SKETCH(intSVColumn, '', 'longSVColumn < 100', '$2') from testTable");
    testInvalidQuery("select DISTINCT_COUNT_THETA_SKETCH(intSVColumn, '', 'longSVColumn < 100', 'foo') from testTable");
    testInvalidQuery(
        "select DISTINCT_COUNT_THETA_SKETCH(intSVColumn, '', 'longSVColumn < 100', 'SET_UNION($1)') from testTable");
    testInvalidQuery(
        "select DISTINCT_COUNT_THETA_SKETCH(intSVColumn, '', 'longSVColumn < 100', 'SET_INTERSECT($1)') from "
            + "testTable");
    testInvalidQuery(
        "select DISTINCT_COUNT_THETA_SKETCH(intSVColumn, '', 'longSVColumn < 100', 'SET_DIFF($1)') from testTable");
    testInvalidQuery(
        "select DISTINCT_COUNT_THETA_SKETCH(intSVColumn, '', 'longSVColumn < 100', 'floatSVColumn > 500', 'SET_DIFF"
            + "($0,$1,$2)') from testTable");
  }

  private void testInvalidQuery(String query) {
    try {
      getBrokerResponse(query);
      fail();
    } catch (BadQueryRequestException e) {
      // Expected
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _indexSegment.destroy();
    FileUtils.deleteDirectory(INDEX_DIR);
  }
}
